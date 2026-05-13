package scheduling

import (
	"log"
	"math"
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

func NewPartitioningScheduler(instanceFactory InstanceFactory, parallelismTarget int) *PartitioningScheduler {
	scheduler := &PartitioningScheduler{
		instanceFactory:    instanceFactory,
		modelQueues:        make(map[string][]*timestampedTask),
		unallocatedNodes:   make(map[string]Node),
		allocatedNodes:     make(map[string]NodeAllocationInfo),
		idleInstances:      make(map[string][]instanceInfo),
		newTasksChan:       make(chan Task, 16),
		nodeConnectChan:    make(chan Node, 16),
		nodeDisconnectChan: make(chan Node, 16),
		taskCancelledChan:  make(chan Task, 16),
		taskCompletedChan:  make(chan TaskCompletionMessage, 16),
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
	nodeConnectChan   chan Node
	nodeDisconnectChan chan Node
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
		if len(s.unallocatedNodes) == 0 {
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

					if len(s.unallocatedNodes) >= s.parallelismTarget {
						break killLoop
					}
				}
			}
		}

		for len(s.unallocatedNodes) == 0 {
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
		}

		// create new instance
		nodes := []Node{}
		for _, node := range s.unallocatedNodes {
			nodes = append(nodes, node)
			if len(nodes) == s.parallelismTarget {
				break
			}
		}

		log.Printf("PartitioningScheduler: starting model %s with %d nodes", task.Model(), len(nodes))

		instance, err := s.instanceFactory.StartInstance(task.Model(), nodes)
		if err != nil {
			log.Printf("Failed to create instance: %v", err)
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

var _ Scheduler = (*PartitioningScheduler)(nil)
