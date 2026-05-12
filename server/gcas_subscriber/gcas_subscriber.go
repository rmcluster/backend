package gcassubscriber

import (
	"github.com/wk-y/rama-swap/server/gcas"
	"github.com/wk-y/rama-swap/tracker"
)

func NewGCASSubscriber(g gcas.GCAS) *GCASSubscriber {
	return &GCASSubscriber{
		gcas: g,
	}
}

type GCASSubscriber struct {
	gcas gcas.GCAS
}

// OnNodeAdded implements [tracker.TrackerSubscriber].
func (g *GCASSubscriber) OnNodeAdded(node tracker.RpcServerInfo) {
	if node.StoragePort == 0 {
		return
	}
	g.gcas.AddNode(gcas.NewRemoteCAS(node.Id, node.Ip, node.StoragePort))
}

// OnNodeRemoved implements [tracker.TrackerSubscriber].
func (g *GCASSubscriber) OnNodeRemoved(node tracker.RpcServerInfo) {
	g.gcas.RemoveNode(node.Id)
}

// OnNodeUpdated implements [tracker.TrackerSubscriber].
func (g *GCASSubscriber) OnNodeUpdated(node tracker.RpcServerInfo) {
	if node.StoragePort == 0 {
		g.gcas.RemoveNode(node.Id)
		return
	}
	g.gcas.ReplaceNode(gcas.NewRemoteCAS(node.Id, node.Ip, node.StoragePort))
}

var _ tracker.TrackerSubscriber = (*GCASSubscriber)(nil)
