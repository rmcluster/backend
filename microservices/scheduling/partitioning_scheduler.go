package scheduling

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"
)

type timestampedTask struct {
	task      Task
	timestamp time.Time
}

func newTimestampedTask(task Task) *timestampedTask {
	return &timestampedTask{
		task:      task,
		timestamp: time.Now(),
	}
}

type instanceInfo struct {
	instance  Instance
	usedNodes []Node
}

type TaskCompletionMessage struct {
	instanceInfo
	task Task
}

type NodeAllocationInfo struct {
	instance Instance
	node     Node
}

const defaultNodeMemoryUtilization = 0.9

func NewPartitioningScheduler(instanceFactory InstanceFactory, parallelismTarget int) *PartitioningScheduler {
	scheduler := &PartitioningScheduler{
		instanceFactory:    instanceFactory,
		modelQueues:        make(map[string][]*timestampedTask),
		unallocatedNodes:   make(map[string]Node),
		allocatedNodes:     make(map[string]NodeAllocationInfo),
		idleInstances:      make(map[string][]instanceInfo),
		newTasksChan:      make(chan Task, 16),
		nodeEventChan:     make(chan NodeEvent, 16),
		taskCancelledChan:  make(chan Task, 16),
		taskCompletedChan:  make(chan TaskCompletionMessage, 16),
		instanceDeadChan:   make(chan instanceInfo, 16),
		parallelismTarget:  parallelismTarget,
		idleBias:           10 * time.Second,
	}
	go scheduler.run()
	return scheduler
}

/*
Algorithm:

1. Calculate score for each queue (function of age and a biasing factor for loaded models)
2. Pop from the highest scoring queue, recalculating as needed
3. Repeat until the highest score is for a model that isn’t loaded
4. Wait for there to be idle nodes (either unallocated or corresponding to a server that is idle)
5. Allocate idle nodes to a new server for the highest scoring model, killing idle servers if necessary. Limit allocation size based on a configurable parameter.
6. Loop back to step 2.

For simplicity, each server only runs one task at a time.
*/
type PartitioningScheduler struct {
	instanceFactory   InstanceFactory
	modelQueues       map[string][]*timestampedTask
	unallocatedNodes  map[string]Node
	allocatedNodes    map[string]NodeAllocationInfo
	idleInstances     map[string][]instanceInfo
	parallelismTarget int           // target for how many nodes to allocate per instance
	idleBias          time.Duration // how many seconds of "advantage" tasks for an idle instance gets

	// channels for the different notification types
	newTasksChan      chan Task
	nodeEventChan     chan NodeEvent
	taskCancelledChan chan Task
	taskCompletedChan chan TaskCompletionMessage
	instanceDeadChan  chan instanceInfo
}

type NodeEventType int

const (
	NodeConnect NodeEventType = iota
	NodeDisconnect
)

type NodeEvent struct {
	node      Node
	eventType NodeEventType
}

// OnNewTask implements [Scheduler].
func (s *PartitioningScheduler) OnNewTask(task Task) {
	log.Printf("PartitioningScheduler: received task for model %s", task.Model())
	s.newTasksChan <- task
}

// OnNodeConnect implements [Scheduler].
func (s *PartitioningScheduler) OnNodeConnect(node Node) {
	s.nodeEventChan <- NodeEvent{node: node, eventType: NodeConnect}
}

// OnNodeDisconnect implements [Scheduler].
func (s *PartitioningScheduler) OnNodeDisconnect(node Node) {
	s.nodeEventChan <- NodeEvent{node: node, eventType: NodeDisconnect}
}

// OnTaskCancelled implements [Scheduler].
func (s *PartitioningScheduler) OnTaskCancelled(task Task) {
	s.taskCancelledChan <- task
}

func (s *PartitioningScheduler) run() {
taskHandlerLoop:
	for {
		s.processEvents()

		// determine which model queue has the highest scoring task
		var highestScoringQueue string
		var maxScore int64 = math.MinInt64

		now := time.Now()
		for model, queue := range s.modelQueues {
			// score only the front of the queue
			for _, t := range queue {
				score := s.scoreTask(t, now)
				if score > maxScore {
					maxScore = score
					highestScoringQueue = model
				}
				break
			}
		}

		var task Task

		if maxScore != math.MinInt64 { // take from highest scoring queue
			task = s.modelQueues[highestScoringQueue][0].task
			s.modelQueues[highestScoringQueue] = s.modelQueues[highestScoringQueue][1:]
		} else { // wait for a task to arrive
		awaitTaskLoop:
			for {
				select {
				case task = <-s.newTasksChan:
					break awaitTaskLoop
				case taskCompletionMessage := <-s.taskCompletedChan:
					s.handleTaskCompletion(taskCompletionMessage)
				case nodeEvent := <-s.nodeEventChan:
					s.handleNodeEvent(nodeEvent)
				case task := <-s.taskCancelledChan:
					s.handleTaskCancellation(task)
				}
			}
		}

		s.processEvents()

		// attempt to assign the task
		for len(s.idleInstances[task.Model()]) > 0 { // assign to the idle instanceInfo
			instanceInfo := s.idleInstances[task.Model()][0]
			s.idleInstances[task.Model()] = s.idleInstances[task.Model()][1:]

			if !s.checkInstanceNodesStillOk(instanceInfo) {
				s.killInstance(instanceInfo)
				continue
			}

			go func() {
				defer func() {
					s.taskCompletedChan <- TaskCompletionMessage{
						task:         task,
						instanceInfo: instanceInfo,
					}
				}()
				task.PerformInference(instanceInfo.instance)
			}()

			continue taskHandlerLoop
		}

		// can we create a new instance?
		requiredNodeCount := s.requiredNodeCount(task.Model())

		if len(s.unallocatedNodes) < requiredNodeCount {
			// can we kill any idle instances?
		killLoop:
			for _, instances := range s.idleInstances {
				for _, instance := range instances {
					instance.instance.Stop()
					instance.instance.AwaitTermination()
					for _, node := range instance.usedNodes {
						if _, ok := s.allocatedNodes[node.Id()]; ok {
							delete(s.allocatedNodes, node.Id())
							s.unallocatedNodes[node.Id()] = node
						}
					}

					if len(s.unallocatedNodes) >= requiredNodeCount {
						break killLoop
					}
				}
			}
		}

		for len(s.unallocatedNodes) < requiredNodeCount {
			select {
			case nodeEvent := <-s.nodeEventChan:
				s.handleNodeEvent(nodeEvent)
			case task := <-s.taskCancelledChan:
				s.handleTaskCancellation(task)
			case completion := <-s.taskCompletedChan:
				if completion.instanceInfo.instance.Model() == task.Model() && s.checkInstanceNodesStillOk(completion.instanceInfo) {
					// reuse the instance
					instanceInfo := completion.instanceInfo
					go func() {
						defer func() {
							s.taskCompletedChan <- TaskCompletionMessage{
								task:         task,
								instanceInfo: instanceInfo,
							}
						}()
						task.PerformInference(instanceInfo.instance)
					}()
					continue taskHandlerLoop
				} else {
					s.killInstance(completion.instanceInfo)
				}
			}
			requiredNodeCount = s.requiredNodeCount(task.Model())
		}

		// create new instance
		nodes := s.selectNodes(task.Model())
		if err := s.checkModelFitsOnNodes(task.Model(), nodes); err != nil {
			log.Printf("PartitioningScheduler: refusing to start model %s: %v", task.Model(), err)
			task.Fail(err)
			continue
		}

		s.logNodeSelection(task.Model(), nodes)
		log.Printf("PartitioningScheduler: starting model %s with %d nodes", task.Model(), len(nodes))

		instance, err := s.instanceFactory.StartInstance(task.Model(), nodes)
		if err != nil {
			log.Printf("Failed to create instance: %v", err)
			task.Fail(err)
			continue
		}

		instanceInfo := instanceInfo{
			instance:  instance,
			usedNodes: nodes,
		}

		go func() {
			instance.AwaitTermination()
			s.instanceDeadChan <- instanceInfo
		}()

		for _, node := range nodes {
			s.allocatedNodes[node.Id()] = NodeAllocationInfo{
				instance: instance,
				node:     node,
			}
			delete(s.unallocatedNodes, node.Id())
		}

		go func() {
			defer func() {
				s.taskCompletedChan <- TaskCompletionMessage{
					task:         task,
					instanceInfo: instanceInfo,
				}
			}()
			if err := instanceInfo.instance.WaitReady(); err != nil {
				log.Printf("Failed to wait for instance to be ready: %v", err)
				task.Fail(err)
				return
			}
			task.PerformInference(instanceInfo.instance)
		}()
	}
}

func (s *PartitioningScheduler) processEvents() {
	for {
		select {
		case taskCompletionMessage := <-s.taskCompletedChan:
			s.handleTaskCompletion(taskCompletionMessage)
		case nodeEvent := <-s.nodeEventChan:
			s.handleNodeEvent(nodeEvent)
		case task := <-s.taskCancelledChan:
			s.handleTaskCancellation(task)
		case instanceInfo := <-s.instanceDeadChan:
			s.killInstance(instanceInfo)
		default:
			return
		}
	}
}

func (s *PartitioningScheduler) handleNodeEvent(nodeEvent NodeEvent) {
	switch nodeEvent.eventType {
	case NodeConnect:
		s.handleNodeConnect(nodeEvent.node)
	case NodeDisconnect:
		s.handleNodeDisconnect(nodeEvent.node)
	}
}

func (s *PartitioningScheduler) handleNodeConnect(node Node) {
	s.unallocatedNodes[node.Id()] = node
}

func (s *PartitioningScheduler) handleNodeDisconnect(node Node) {
	delete(s.unallocatedNodes, node.Id())
	delete(s.allocatedNodes, node.Id())
}

func (s *PartitioningScheduler) handleTaskCompletion(taskCompletionMessage TaskCompletionMessage) {
	if s.checkInstanceNodesStillOk(taskCompletionMessage.instanceInfo) {
		s.idleInstances[taskCompletionMessage.task.Model()] = append(s.idleInstances[taskCompletionMessage.task.Model()], taskCompletionMessage.instanceInfo)
		return
	}
	s.killInstance(taskCompletionMessage.instanceInfo)
}

func (s *PartitioningScheduler) handleTaskCancellation(task Task) {
	for i, t := range s.modelQueues[task.Model()] {
		if t.task == task {
			s.modelQueues[task.Model()] = append(s.modelQueues[task.Model()][:i], s.modelQueues[task.Model()][i+1:]...)
			break
		}
	}
}

func (s *PartitioningScheduler) killInstance(instanceInfo instanceInfo) {
	instanceInfo.instance.Stop()
	instanceInfo.instance.AwaitTermination()
	for _, node := range instanceInfo.usedNodes {
		if s.allocatedNodes[node.Id()].instance == instanceInfo.instance {
			delete(s.allocatedNodes, node.Id())
			s.unallocatedNodes[node.Id()] = node
		}
	}
}

func (s *PartitioningScheduler) checkInstanceNodesStillOk(instanceInfo instanceInfo) bool {
	for _, node := range instanceInfo.usedNodes {
		if s.allocatedNodes[node.Id()].instance != instanceInfo.instance {
			return false
		}
	}
	return true
}

// scoreTask returns a score for a task. Higher scores are prioritized.
func (s *PartitioningScheduler) scoreTask(task *timestampedTask, now time.Time) int64 {
	score := int64(now.Sub(task.timestamp).Nanoseconds())

	// is there an idle instance for this model?
	instances := s.idleInstances[task.task.Model()]
	if len(instances) > 0 {
		score += s.idleBias.Nanoseconds()
	}

	return score
}

func (s *PartitioningScheduler) availableNodesSorted() []Node {
	nodes := make([]Node, 0, len(s.unallocatedNodes))
	for _, node := range s.unallocatedNodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].MaxSize() == nodes[j].MaxSize() {
			return nodes[i].Id() < nodes[j].Id()
		}
		return nodes[i].MaxSize() > nodes[j].MaxSize()
	})
	return nodes
}

func (s *PartitioningScheduler) requiredNodeCount(model string) int {
	nodes := s.availableNodesSorted()
	if len(nodes) == 0 {
		return 0
	}
	if s.parallelismTarget > 0 {
		//defer to parallelism target
		if s.parallelismTarget > len(nodes) {
			return len(nodes)
		}
		return s.parallelismTarget
	}

	modelSizer, ok := s.instanceFactory.(ModelSizer)
	if !ok {
		return len(nodes)
	}
	modelBytes, err := modelSizer.ModelSizeBytes(model)
	if err != nil || modelBytes <= 0 {
		log.Printf("PartitioningScheduler: auto node count unavailable for %s: %v", model, err)
		return len(nodes)
	}
	return requiredNodeCountForModelBytes(modelBytes, nodes, defaultNodeMemoryUtilization)
}

func (s *PartitioningScheduler) selectNodes(model string) []Node {
	nodes := s.availableNodesSorted()
	required := s.requiredNodeCount(model)
	if required <= 0 || required > len(nodes) {
		required = len(nodes)
	}

	if s.parallelismTarget == 0 && required == 1 {
		if modelSizer, ok := s.instanceFactory.(ModelSizer); ok {
			modelBytes, err := modelSizer.ModelSizeBytes(model)
			if err == nil && modelBytes > 0 {
				if node, ok := smallestSufficientNode(modelBytes, nodes, defaultNodeMemoryUtilization); ok {
					return []Node{node}
				}
			}
		}
	}

	return append([]Node(nil), nodes[:required]...)
}

func requiredNodeCountForModelBytes(modelBytes int64, nodes []Node, utilization float64) int {
	if len(nodes) == 0 {
		return 0
	}
	if modelBytes <= 0 || utilization <= 0 {
		return len(nodes)
	}

	var totalUsable int64
	for idx, node := range nodes {
		// Keep node-count selection aligned with tensor-split budgeting: each
		// phone contributes only floor(utilization * max_capacity).
		usable := usableNodeCapacity(node.MaxSize(), utilization)
		if usable < 0 {
			usable = 0
		}
		totalUsable += usable
		if totalUsable >= modelBytes {
			return idx + 1
		}
	}

	return len(nodes)
}

func (s *PartitioningScheduler) checkModelFitsOnNodes(model string, nodes []Node) error {
	modelSizer, ok := s.instanceFactory.(ModelSizer)
	if !ok {
		return nil
	}
	modelBytes, err := modelSizer.ModelSizeBytes(model)
	if err != nil || modelBytes <= 0 {
		return nil
	}
	totalCapacity := totalNodeCapacity(nodes)
	if totalCapacity < modelBytes {
		return fmt.Errorf("model size %d exceeds selected node capacity %d", modelBytes, totalCapacity)
	}
	return nil
}

func smallestSufficientNode(modelBytes int64, nodes []Node, utilization float64) (Node, bool) {
	bestIdx := -1
	bestUsable := int64(0)
	for i, node := range nodes {
		usable := usableNodeCapacity(node.MaxSize(), utilization)
		if usable < modelBytes {
			continue
		}
		if bestIdx < 0 || usable < bestUsable || (usable == bestUsable && node.Id() < nodes[bestIdx].Id()) {
			bestIdx = i
			bestUsable = usable
		}
	}
	if bestIdx < 0 {
		return nil, false
	}
	return nodes[bestIdx], true
}

func usableNodeCapacity(maxSize int64, utilization float64) int64 {
	return int64(float64(maxSize) * utilization)
}

func totalNodeCapacity(nodes []Node) int64 {
	var total int64
	for _, node := range nodes {
		if node.MaxSize() > 0 {
			total += node.MaxSize()
		}
	}
	return total
}

func (s *PartitioningScheduler) logNodeSelection(model string, nodes []Node) {
	modelBytes := int64(0)
	if modelSizer, ok := s.instanceFactory.(ModelSizer); ok {
		if size, err := modelSizer.ModelSizeBytes(model); err == nil {
			modelBytes = size
		}
	}

	nodeIDs := make([]string, 0, len(nodes))
	usableCapacity := int64(0)
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.Id())
		usableCapacity += usableNodeCapacity(node.MaxSize(), defaultNodeMemoryUtilization)
	}

	log.Printf(
		"PartitioningScheduler: model %s selected nodes [%s] raw_capacity=%d usable_capacity=%d model_bytes=%d parallelism_target=%d",
		model,
		strings.Join(nodeIDs, ","),
		totalNodeCapacity(nodes),
		usableCapacity,
		modelBytes,
		s.parallelismTarget,
	)
}

var _ Scheduler = (*PartitioningScheduler)(nil)
