package scheduling

import (
	"fmt"
	"log"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

type reservedInstance struct {
	instanceInfo
	reservedAt time.Time
}

type TaskCompletionMessage struct {
	instanceInfo
	task Task
}

type NodeAllocationInfo struct {
	instance Instance
	node     Node
}

func NewPartitioningScheduler(instanceFactory InstanceFactory, parallelismTarget int) *PartitioningScheduler {
	scheduler := &PartitioningScheduler{
		instanceFactory:    instanceFactory,
		modelQueues:        make(map[string][]*timestampedTask),
		unallocatedNodes:   make(map[string]Node),
		allocatedNodes:     make(map[string]NodeAllocationInfo),
		idleInstances:      make(map[string][]instanceInfo),
		newTasksChan:       make(chan Task, 16),
		nodeEventChan:      make(chan NodeEvent, 16),
		taskCancelledChan:  make(chan Task, 16),
		taskCompletedChan:  make(chan TaskCompletionMessage, 16),
		instanceDeadChan:   make(chan instanceInfo, 16),
		idleBias:           10 * time.Second,
		reservedInstances:  make(map[string]reservedInstance),
		reservationTTL:     15 * time.Second,
		allocationSnapshot: make(map[string][]string),
	}
	scheduler.parallelismTarget.Store(int32(max(parallelismTarget, 1)))
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
	reservedInstances map[string]reservedInstance
	parallelismTarget atomic.Int32  // target for how many nodes to allocate per instance
	idleBias          time.Duration // how many seconds of "advantage" tasks for an idle instance gets
	reservationTTL    time.Duration

	allocationSnapshotMu sync.RWMutex
	allocationSnapshot   map[string][]string

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

func (s *PartitioningScheduler) GetParallelismTarget() int {
	return int(s.parallelismTarget.Load())
}

func (s *PartitioningScheduler) SetParallelismTarget(n int) {
	if n < 1 {
		n = 1
	}
	s.parallelismTarget.Store(int32(n))
	log.Printf("PartitioningScheduler: parallelism target set to %d", n)
}

func (s *PartitioningScheduler) SnapshotAllocations() map[string][]string {
	s.allocationSnapshotMu.RLock()
	defer s.allocationSnapshotMu.RUnlock()

	snapshot := make(map[string][]string, len(s.allocationSnapshot))
	for model, nodes := range s.allocationSnapshot {
		snapshot[model] = slices.Clone(nodes)
	}
	return snapshot
}

func (s *PartitioningScheduler) GetAllocatedNodesForModel(model string) []string {
	s.allocationSnapshotMu.RLock()
	defer s.allocationSnapshotMu.RUnlock()
	return slices.Clone(s.allocationSnapshot[model])
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

		if s.tryAssignExistingInstance(task) {
			continue taskHandlerLoop
		}

		// can we create a new instance?
		target := s.GetParallelismTarget()
		if len(s.unallocatedNodes) < target {
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

					if len(s.unallocatedNodes) >= target {
						break killLoop
					}
				}
			}
		}

		for {
			if s.tryAssignExistingInstance(task) {
				continue taskHandlerLoop
			}
			if len(s.unallocatedNodes) > 0 {
				break
			}
			select {
			case nodeEvent := <-s.nodeEventChan:
				s.handleNodeEvent(nodeEvent)
			case task := <-s.taskCancelledChan:
				s.handleTaskCancellation(task)
			case completion := <-s.taskCompletedChan:
				s.handleTaskCompletion(completion)
			case instanceInfo := <-s.instanceDeadChan:
				s.killInstance(instanceInfo)
			}
		}

		// create new instance
		nodes := []Node{}
		for _, node := range s.unallocatedNodes {
			nodes = append(nodes, node)
		}
		slices.SortFunc(nodes, func(a, b Node) int {
			switch {
			case a.MaxSize() > b.MaxSize():
				return -1
			case a.MaxSize() < b.MaxSize():
				return 1
			case a.Id() < b.Id():
				return -1
			case a.Id() > b.Id():
				return 1
			default:
				return 0
			}
		})
		if len(nodes) > target {
			nodes = nodes[:target]
		}

		log.Printf(
			"PartitioningScheduler: starting model %s with %d nodes (target %d, available %d): %s",
			task.Model(),
			len(nodes),
			target,
			len(s.unallocatedNodes),
			describeNodes(nodes),
		)

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
		s.refreshAllocationSnapshot()
		s.attachAllocatedNodes(task, nodes)

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

func describeNodes(nodes []Node) string {
	if len(nodes) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		model := node.HardwareModel()
		if model == "" {
			model = "unknown-model"
		}
		parts = append(parts, fmt.Sprintf("%s=%s@%s:%d", node.Id(), model, node.Ip(), node.Port()))
	}
	return strings.Join(parts, ", ")
}

func (s *PartitioningScheduler) processEvents() {
	s.releaseExpiredReservations()
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
	s.refreshAllocationSnapshot()
}

func (s *PartitioningScheduler) handleTaskCompletion(taskCompletionMessage TaskCompletionMessage) {
	if !s.checkInstanceNodesStillOk(taskCompletionMessage.instanceInfo) {
		s.killInstance(taskCompletionMessage.instanceInfo)
		return
	}
	if key := s.benchmarkReservationKey(taskCompletionMessage.task); key != "" && s.benchmarkStage(taskCompletionMessage.task) == "warmup" {
		s.reservedInstances[key] = reservedInstance{
			instanceInfo: taskCompletionMessage.instanceInfo,
			reservedAt:   time.Now(),
		}
		return
	}
	s.idleInstances[taskCompletionMessage.task.Model()] = append(s.idleInstances[taskCompletionMessage.task.Model()], taskCompletionMessage.instanceInfo)
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
	s.refreshAllocationSnapshot()
}

func (s *PartitioningScheduler) checkInstanceNodesStillOk(instanceInfo instanceInfo) bool {
	for _, node := range instanceInfo.usedNodes {
		if s.allocatedNodes[node.Id()].instance != instanceInfo.instance {
			return false
		}
	}
	return true
}

func (s *PartitioningScheduler) instanceMatchesTarget(instanceInfo instanceInfo, target int) bool {
	return len(instanceInfo.usedNodes) == target
}

func (s *PartitioningScheduler) attachAllocatedNodes(task Task, nodes []Node) {
	awareTask, ok := task.(AllocatedNodesAwareTask)
	if !ok {
		return
	}
	awareTask.SetAllocatedNodes(slices.Clone(nodes))
}

func (s *PartitioningScheduler) benchmarkReservationKey(task Task) string {
	awareTask, ok := task.(BenchmarkGroupAwareTask)
	if !ok || awareTask.BenchmarkGroupID() == "" {
		return ""
	}
	return task.Model() + "\x00" + awareTask.BenchmarkGroupID()
}

func (s *PartitioningScheduler) benchmarkStage(task Task) string {
	awareTask, ok := task.(BenchmarkGroupAwareTask)
	if !ok {
		return ""
	}
	return awareTask.BenchmarkStage()
}

func (s *PartitioningScheduler) takeReservedInstance(task Task, target int) (instanceInfo, bool) {
	key := s.benchmarkReservationKey(task)
	if key == "" {
		return instanceInfo{}, false
	}
	reserved, ok := s.reservedInstances[key]
	if !ok {
		return instanceInfo{}, false
	}
	delete(s.reservedInstances, key)
	if time.Since(reserved.reservedAt) > s.reservationTTL {
		s.addIdleOrKill(reserved.instanceInfo, task.Model())
		return instanceInfo{}, false
	}
	if !s.checkInstanceNodesStillOk(reserved.instanceInfo) {
		s.killInstance(reserved.instanceInfo)
		return instanceInfo{}, false
	}
	if !s.instanceMatchesTarget(reserved.instanceInfo, target) {
		s.addIdleOrKill(reserved.instanceInfo, task.Model())
		return instanceInfo{}, false
	}
	return reserved.instanceInfo, true
}

func (s *PartitioningScheduler) takeIdleInstance(task Task, target int) (instanceInfo, bool) {
	for len(s.idleInstances[task.Model()]) > 0 {
		instanceInfo := s.idleInstances[task.Model()][0]
		s.idleInstances[task.Model()] = s.idleInstances[task.Model()][1:]
		if !s.checkInstanceNodesStillOk(instanceInfo) {
			s.killInstance(instanceInfo)
			continue
		}
		if !s.instanceMatchesTarget(instanceInfo, target) {
			log.Printf(
				"PartitioningScheduler: discarding idle instance for model %s with %d nodes; target is %d",
				task.Model(),
				len(instanceInfo.usedNodes),
				target,
			)
			s.killInstance(instanceInfo)
			continue
		}
		return instanceInfo, true
	}
	return instanceInfo{}, false
}

func (s *PartitioningScheduler) tryAssignExistingInstance(task Task) bool {
	target := s.GetParallelismTarget()
	if instanceInfo, ok := s.takeReservedInstance(task, target); ok {
		s.startTaskOnInstance(task, instanceInfo)
		return true
	}
	if instanceInfo, ok := s.takeIdleInstance(task, target); ok {
		s.startTaskOnInstance(task, instanceInfo)
		return true
	}
	return false
}

func (s *PartitioningScheduler) startTaskOnInstance(task Task, instanceInfo instanceInfo) {
	s.attachAllocatedNodes(task, instanceInfo.usedNodes)
	go func() {
		defer func() {
			s.taskCompletedChan <- TaskCompletionMessage{
				task:         task,
				instanceInfo: instanceInfo,
			}
		}()
		task.PerformInference(instanceInfo.instance)
	}()
}

func (s *PartitioningScheduler) addIdleOrKill(instanceInfo instanceInfo, model string) {
	if s.checkInstanceNodesStillOk(instanceInfo) {
		s.idleInstances[model] = append(s.idleInstances[model], instanceInfo)
		return
	}
	s.killInstance(instanceInfo)
}

func (s *PartitioningScheduler) releaseExpiredReservations() {
	now := time.Now()
	for key, reserved := range s.reservedInstances {
		if now.Sub(reserved.reservedAt) <= s.reservationTTL {
			continue
		}
		delete(s.reservedInstances, key)
		s.addIdleOrKill(reserved.instanceInfo, reserved.instanceInfo.instance.Model())
	}
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

func (s *PartitioningScheduler) refreshAllocationSnapshot() {
	snapshot := make(map[string][]string)
	for nodeID, info := range s.allocatedNodes {
		model := info.instance.Model()
		snapshot[model] = append(snapshot[model], nodeID)
	}
	for model := range snapshot {
		slices.Sort(snapshot[model])
	}

	s.allocationSnapshotMu.Lock()
	s.allocationSnapshot = snapshot
	s.allocationSnapshotMu.Unlock()
}

var _ Scheduler = (*PartitioningScheduler)(nil)
