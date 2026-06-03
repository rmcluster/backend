package scheduling

import (
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rmcluster/backend/llama"
)

type pendingTask struct {
	task      Task
	timestamp time.Time
}

func newPendingTask(task Task) *pendingTask {
	return &pendingTask{
		task:      task,
		timestamp: time.Now(),
	}
}

type instanceState struct {
	instance  Instance
	usedNodes []Node
}

type taskCompletionMessage struct {
	instanceState
	task Task
}

type decisionKind int

const (
	decisionNone decisionKind = iota
	decisionReuseIdleInstance
	decisionCreateNewInstance
)

func (k decisionKind) String() string {
	switch k {
	case decisionReuseIdleInstance:
		return "reuse-idle-instance"
	case decisionCreateNewInstance:
		return "create-new-instance"
	default:
		return "none"
	}
}

type scheduleDecision struct {
	kind     decisionKind
	task     *pendingTask
	instance instanceState
	nodes    []Node
}

func (d scheduleDecision) taskSummary() string {
	if d.task == nil {
		return "-"
	}
	return d.task.task.Model()
}

func (d scheduleDecision) taskAge() string {
	if d.task == nil {
		return "-"
	}
	return time.Since(d.task.timestamp).Truncate(time.Millisecond).String()
}

const (
	DefaultMemoryTargetMultiplier       = 1.0
	DefaultMemoryTargetBytes      int64 = 1 << 30
)

type EventDrivenScheduler struct {
	instanceFactory        InstanceFactory
	llmService             llama.Llama
	modelQueues            map[string][]*pendingTask
	unallocatedNodes       map[string]Node
	allocatedNodes         map[string]NodeAllocationInfo
	idleInstances          map[string][]instanceState
	activeInstances        map[Instance]instanceState
	newTasksChan           chan Task
	nodeEventChan          chan NodeEvent
	taskCancelledChan      chan Task
	taskCompletedChan      chan taskCompletionMessage
	instanceDeadChan       chan instanceState
	memoryTargetMultiplier float64
	tunableMu              sync.RWMutex
	modelSizeCache         map[string]int64
	idleBias               time.Duration
}

// NewEventDrivenScheduler constructs a new scheduler that decides one action per event.
func NewEventDrivenScheduler(instanceFactory InstanceFactory, llmService llama.Llama, memoryTargetMultiplier float64) *EventDrivenScheduler {
	if memoryTargetMultiplier <= 0 {
		memoryTargetMultiplier = DefaultMemoryTargetMultiplier
	}
	scheduler := &EventDrivenScheduler{
		instanceFactory:        instanceFactory,
		llmService:             llmService,
		modelQueues:            make(map[string][]*pendingTask),
		unallocatedNodes:       make(map[string]Node),
		allocatedNodes:         make(map[string]NodeAllocationInfo),
		idleInstances:          make(map[string][]instanceState),
		activeInstances:        make(map[Instance]instanceState),
		newTasksChan:           make(chan Task, 16),
		nodeEventChan:          make(chan NodeEvent, 16),
		taskCancelledChan:      make(chan Task, 16),
		taskCompletedChan:      make(chan taskCompletionMessage, 16),
		instanceDeadChan:       make(chan instanceState, 16),
		memoryTargetMultiplier: memoryTargetMultiplier,
		modelSizeCache:         make(map[string]int64),
		idleBias: 10 * time.Second,
	}
	go scheduler.run()
	return scheduler
}

// OnNewTask implements [Scheduler].
func (s *EventDrivenScheduler) OnNewTask(task Task) {
	log.Printf("EventDrivenScheduler: received task for model %s", task.Model())
	s.newTasksChan <- task
}

// OnNodeConnect implements [Scheduler].
func (s *EventDrivenScheduler) OnNodeConnect(node Node) {
	s.nodeEventChan <- NodeEvent{node: node, eventType: NodeConnect}
}

// OnNodeDisconnect implements [Scheduler].
func (s *EventDrivenScheduler) OnNodeDisconnect(node Node) {
	s.nodeEventChan <- NodeEvent{node: node, eventType: NodeDisconnect}
}

// OnTaskCancelled implements [Scheduler].
func (s *EventDrivenScheduler) OnTaskCancelled(task Task) {
	s.taskCancelledChan <- task
}

func (s *EventDrivenScheduler) totalQueuedTasks() int {
	total := 0
	for _, queue := range s.modelQueues {
		total += len(queue)
	}
	return total
}

func (s *EventDrivenScheduler) totalIdleInstances() int {
	total := 0
	for _, instances := range s.idleInstances {
		total += len(instances)
	}
	return total
}

func (s *EventDrivenScheduler) totalActiveInstances() int {
	return len(s.activeInstances)
}

func nodeList(nodes []Node) string {
	if len(nodes) == 0 {
		return "[]"
	}
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.Id())
	}
	return "[" + strings.Join(ids, ",") + "]"
}

func (s *EventDrivenScheduler) run() {
	for {
		s.processEvents()

		decision := s.decideAction()
		log.Printf("EventDrivenScheduler: decision=%s model=%s taskAge=%s queue=%d unallocated=%d idle=%d active=%d nodes=%s",
			decision.kind.String(),
			decision.taskSummary(),
			decision.taskAge(),
			s.totalQueuedTasks(),
			len(s.unallocatedNodes),
			s.totalIdleInstances(),
			s.totalActiveInstances(),
			nodeList(decision.nodes),
		)

		switch decision.kind {
		case decisionReuseIdleInstance:
			s.executeReuseDecision(decision)
		case decisionCreateNewInstance:
			s.executeCreateDecision(decision)
		case decisionNone:
			s.waitForEvent()
		}
	}
}

func (s *EventDrivenScheduler) processEvents() {
	for {
		select {
		case completion := <-s.taskCompletedChan:
			s.handleTaskCompletion(completion)
		case nodeEvent := <-s.nodeEventChan:
			s.handleNodeEvent(nodeEvent)
		case task := <-s.taskCancelledChan:
			s.handleTaskCancellation(task)
		case instanceInfo := <-s.instanceDeadChan:
			s.handleInstanceDeath(instanceInfo)
		default:
			return
		}
	}
}

func (s *EventDrivenScheduler) waitForEvent() {
	select {
	case task := <-s.newTasksChan:
		s.addQueuedTask(task)
	case nodeEvent := <-s.nodeEventChan:
		s.handleNodeEvent(nodeEvent)
	case task := <-s.taskCancelledChan:
		s.handleTaskCancellation(task)
	case completion := <-s.taskCompletedChan:
		s.handleTaskCompletion(completion)
	case instanceInfo := <-s.instanceDeadChan:
		s.handleInstanceDeath(instanceInfo)
	}
}

func (s *EventDrivenScheduler) addQueuedTask(task Task) {
	s.modelQueues[task.Model()] = append(s.modelQueues[task.Model()], newPendingTask(task))
}

func (s *EventDrivenScheduler) decideAction() scheduleDecision {
	s.purgeInvalidIdleInstances()

	if len(s.modelQueues) == 0 {
		return scheduleDecision{kind: decisionNone}
	}

	bestTask, bestModel := s.pickHighestScoringTask()
	if bestTask == nil {
		return scheduleDecision{kind: decisionNone}
	}

	if idle := s.pickIdleInstanceFor(bestModel); idle != nil {
		return scheduleDecision{
			kind:     decisionReuseIdleInstance,
			task:     bestTask,
			instance: *idle,
		}
	}

	if nodes, ok := s.pickNodesForNewInstance(bestModel); ok {
		return scheduleDecision{
			kind:  decisionCreateNewInstance,
			task:  bestTask,
			nodes: nodes,
		}
	}

	return scheduleDecision{kind: decisionNone}
}

func (s *EventDrivenScheduler) pickHighestScoringTask() (*pendingTask, string) {
	var best *pendingTask
	var bestModel string
	var maxScore int64 = math.MinInt64
	for model, queue := range s.modelQueues {
		if len(queue) == 0 {
			continue
		}
		score := s.scoreTask(queue[0], time.Now())
		if score > maxScore {
			maxScore = score
			best = queue[0]
			bestModel = model
		}
	}
	return best, bestModel
}

func (s *EventDrivenScheduler) pickIdleInstanceFor(model string) *instanceState {
	instances := s.idleInstances[model]
	for i := range instances {
		if s.checkInstanceNodesStillOk(instances[i]) {
			return &instances[i]
		}
	}
	return nil
}

func (s *EventDrivenScheduler) pickNodesForNewInstance(model string) ([]Node, bool) {
	target := s.memoryTargetForModel(model)

	available := s.availableNodesSorted()
	selected := make([]Node, 0, len(available))
	total := int64(0)
	for _, node := range available {
		selected = append(selected, node)
		if size := node.MaxSize(); size > 0 {
			total += size
		}
		if total >= target {
			return selected, true
		}
	}

	idleInstances := s.collectIdleInstancesSortedByKillCost()
	for _, inst := range idleInstances {
		if !s.checkInstanceNodesStillOk(inst) {
			continue
		}
		for _, node := range inst.usedNodes {
			selected = append(selected, node)
			if size := node.MaxSize(); size > 0 {
				total += size
			} else {
				total += 1
			}
		}
		if total >= target {
			return selected, true
		}
	}

	if total > 0 {
		return selected, true
	}

	return nil, false
}

func (s *EventDrivenScheduler) availableNodesSorted() []Node {
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

func (s *EventDrivenScheduler) memoryTargetForModel(model string) int64 {
	modelSize := s.cachedModelSize(model)
	if modelSize <= 0 {
		return DefaultMemoryTargetBytes
	}
	s.tunableMu.RLock()
	multiplier := s.memoryTargetMultiplier
	s.tunableMu.RUnlock()
	target := int64(float64(modelSize) * multiplier)
	if target <= 0 {
		return DefaultMemoryTargetBytes
	}
	return target
}

func (s *EventDrivenScheduler) cachedModelSize(model string) int64 {
	if size, ok := s.modelSizeCache[model]; ok {
		return size
	}
	size := s.loadModelSize(model)
	s.modelSizeCache[model] = size
	return size
}

func (s *EventDrivenScheduler) loadModelSize(model string) int64 {
	info, err := s.llmService.Inspect(model)
	if err != nil || info.Path == "" {
		return 0
	}
	fi, err := os.Stat(info.Path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (s *EventDrivenScheduler) collectIdleInstancesSortedByKillCost() []instanceState {
	instances := make([]instanceState, 0)
	for _, queue := range s.idleInstances {
		for _, inst := range queue {
			instances = append(instances, inst)
		}
	}
	sort.Slice(instances, func(i, j int) bool {
		if len(instances[i].usedNodes) != len(instances[j].usedNodes) {
			return len(instances[i].usedNodes) < len(instances[j].usedNodes)
		}
		return instanceTotalMemory(instances[i]) < instanceTotalMemory(instances[j])
	})
	return instances
}

func instanceTotalMemory(info instanceState) int64 {
	total := int64(0)
	for _, node := range info.usedNodes {
		total += node.MaxSize()
	}
	return total
}

func (s *EventDrivenScheduler) executeReuseDecision(decision scheduleDecision) {
	if !s.checkInstanceNodesStillOk(decision.instance) {
		s.killInstance(decision.instance)
		return
	}

	s.removeIdleInstance(decision.instance.instance)
	s.dequeueTask(decision.task)
	go s.performTask(decision.task, decision.instance)
}

func (s *EventDrivenScheduler) executeCreateDecision(decision scheduleDecision) {
	s.dequeueTask(decision.task)
	for _, node := range decision.nodes {
		if allocation, ok := s.allocatedNodes[node.Id()]; ok {
			if idleInstance := s.findIdleInstance(allocation.instance); idleInstance != nil {
				s.killInstance(*idleInstance)
			}
		}
	}

	instance, err := s.instanceFactory.StartInstance(decision.task.task.Model(), decision.nodes)
	if err != nil {
		log.Printf("EventDrivenScheduler: failed to start instance: %v", err)
		decision.task.task.Fail(err)
		return
	}

	info := instanceState{instance: instance, usedNodes: decision.nodes}
	s.activeInstances[instance] = info
	for _, node := range decision.nodes {
		s.allocatedNodes[node.Id()] = NodeAllocationInfo{instance: instance, node: node}
		delete(s.unallocatedNodes, node.Id())
	}

	go func() {
		instance.AwaitTermination()
		s.instanceDeadChan <- info
	}()

	go func() {
		if err := instance.WaitReady(); err != nil {
			log.Printf("EventDrivenScheduler: failed to wait for instance readiness: %v", err)
			decision.task.task.Fail(err)
			s.instanceDeadChan <- info
			return
		}
		go s.performTask(decision.task, info)
	}()
}

func (s *EventDrivenScheduler) performTask(task *pendingTask, instance instanceState) {
	defer func() {
		s.taskCompletedChan <- taskCompletionMessage{
			instanceState: instance,
			task:          task.task,
		}
	}()
	_ = task.task.PerformInference(instance.instance)
}

func (s *EventDrivenScheduler) dequeueTask(task *pendingTask) {
	queue := s.modelQueues[task.task.Model()]
	for i, pending := range queue {
		if pending == task {
			s.modelQueues[task.task.Model()] = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	if len(s.modelQueues[task.task.Model()]) == 0 {
		delete(s.modelQueues, task.task.Model())
	}
}

func (s *EventDrivenScheduler) handleNodeEvent(nodeEvent NodeEvent) {
	switch nodeEvent.eventType {
	case NodeConnect:
		s.unallocatedNodes[nodeEvent.node.Id()] = nodeEvent.node
	case NodeDisconnect:
		delete(s.unallocatedNodes, nodeEvent.node.Id())
		delete(s.allocatedNodes, nodeEvent.node.Id())
	}
}

func (s *EventDrivenScheduler) handleTaskCompletion(message taskCompletionMessage) {
	if !s.checkInstanceNodesStillOk(message.instanceState) {
		s.killInstance(message.instanceState)
		return
	}

	delete(s.activeInstances, message.instanceState.instance)
	s.idleInstances[message.task.Model()] = append(s.idleInstances[message.task.Model()], message.instanceState)
}

func (s *EventDrivenScheduler) handleTaskCancellation(task Task) {
	queue := s.modelQueues[task.Model()]
	for i, pending := range queue {
		if pending.task == task {
			s.modelQueues[task.Model()] = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	if len(s.modelQueues[task.Model()]) == 0 {
		delete(s.modelQueues, task.Model())
	}
}

func (s *EventDrivenScheduler) handleInstanceDeath(instanceInfo instanceState) {
	s.removeFromIdle(instanceInfo.instance)
	delete(s.activeInstances, instanceInfo.instance)
	s.killInstance(instanceInfo)
}

func (s *EventDrivenScheduler) removeFromIdle(instance Instance) {
	for model, list := range s.idleInstances {
		for i, info := range list {
			if info.instance == instance {
				s.idleInstances[model] = append(list[:i], list[i+1:]...)
				if len(s.idleInstances[model]) == 0 {
					delete(s.idleInstances, model)
				}
				return
			}
		}
	}
}

func (s *EventDrivenScheduler) removeIdleInstance(instance Instance) {
	for model, list := range s.idleInstances {
		for i, info := range list {
			if info.instance == instance {
				s.idleInstances[model] = append(list[:i], list[i+1:]...)
				if len(s.idleInstances[model]) == 0 {
					delete(s.idleInstances, model)
				}
				return
			}
		}
	}
}

func (s *EventDrivenScheduler) findIdleInstance(instance Instance) *instanceState {
	for model, list := range s.idleInstances {
		for i, info := range list {
			if info.instance == instance {
				return &s.idleInstances[model][i]
			}
		}
	}
	return nil
}

func (s *EventDrivenScheduler) killInstance(instanceInfo instanceState) {
	instanceInfo.instance.Stop()
	instanceInfo.instance.AwaitTermination()

	delete(s.activeInstances, instanceInfo.instance)
	s.removeIdleInstance(instanceInfo.instance)

	for _, node := range instanceInfo.usedNodes {
		if s.allocatedNodes[node.Id()].instance == instanceInfo.instance {
			delete(s.allocatedNodes, node.Id())
			s.unallocatedNodes[node.Id()] = node
		}
	}
}

func (s *EventDrivenScheduler) checkInstanceNodesStillOk(instanceInfo instanceState) bool {
	for _, node := range instanceInfo.usedNodes {
		if s.allocatedNodes[node.Id()].instance != instanceInfo.instance {
			return false
		}
	}
	return true
}

func (s *EventDrivenScheduler) purgeInvalidIdleInstances() {
	for model, list := range s.idleInstances {
		valid := list[:0]
		for _, info := range list {
			if s.checkInstanceNodesStillOk(info) {
				valid = append(valid, info)
			} else {
				s.killInstance(info)
			}
		}
		if len(valid) == 0 {
			delete(s.idleInstances, model)
		} else {
			s.idleInstances[model] = valid
		}
	}
}

func (s *EventDrivenScheduler) scoreTask(task *pendingTask, now time.Time) int64 {
	score := int64(now.Sub(task.timestamp).Nanoseconds())
	if len(s.idleInstances[task.task.Model()]) > 0 {
		score += s.idleBias.Nanoseconds()
	}
	return score
}

var _ Scheduler = (*EventDrivenScheduler)(nil)
