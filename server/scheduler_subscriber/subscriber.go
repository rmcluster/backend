package schedulersubscriber

import (
	"log"
	"strconv"
	"sync"

	"github.com/wk-y/rama-swap/microservices/scheduling"
	"github.com/wk-y/rama-swap/tracker"
)

type SchedulerSubscriber struct {
	scheduler scheduling.Scheduler
	// maps from node ids to the nodes' tracker info
	nodesLock sync.Mutex
	nodes     map[string]*node
}

func NewSchedulerSubscriber(scheduler scheduling.Scheduler) *SchedulerSubscriber {
	return &SchedulerSubscriber{
		scheduler: scheduler,
		nodes:     make(map[string]*node),
	}
}

// OnNodeUpdated implements [tracker.TrackerSubscriber].
func (s *SchedulerSubscriber) OnNodeUpdated(node tracker.RpcServerInfo) {
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	// The tracker tracks nodes by their ids, but the scheduler uses ip + port as node id.
	// When a node changes address, we reflect it as a node being disconnected and a new node being connected.
	oldNode := s.nodes[node.Id]
	if oldNode == nil {
		log.Printf("WARN: node %s not found in nodes", node.Id)
		s.OnNodeAdded(node)
		return
	}

	convertedNode := convertTrackerNode(node)

	if oldNode.ip != convertedNode.ip || oldNode.port != convertedNode.port {
		s.scheduler.OnNodeDisconnect(oldNode)
		s.scheduler.OnNodeConnect(convertedNode)
		s.nodes[node.Id] = convertedNode
		return
	}

	oldNode.setMaxSize(node.MaxSize)
}

func (s *SchedulerSubscriber) OnNodeAdded(trackerNode tracker.RpcServerInfo) {
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()
	convertedNode := convertTrackerNode(trackerNode)
	s.nodes[trackerNode.Id] = convertedNode

	log.Printf("Node added: %v", trackerNode)
	s.scheduler.OnNodeConnect(convertedNode)
}

func (s *SchedulerSubscriber) OnNodeRemoved(trackerNode tracker.RpcServerInfo) {
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()
	delete(s.nodes, trackerNode.Id)

	log.Printf("Node removed: %v", trackerNode)
	s.scheduler.OnNodeDisconnect(convertTrackerNode(trackerNode))
}

func convertTrackerNode(trackerNode tracker.RpcServerInfo) *node {
	converted := &node{
		id:            trackerNode.Ip + ":" + strconv.Itoa(trackerNode.Port),
		hardwareModel: trackerNode.HardwareModel,
		ip:            trackerNode.Ip,
		port:          trackerNode.Port,
	}
	converted.setMaxSize(trackerNode.MaxSize)
	return converted
}

var _ tracker.TrackerSubscriber = (*SchedulerSubscriber)(nil)
