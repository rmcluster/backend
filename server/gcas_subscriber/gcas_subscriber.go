package gcassubscriber

import (
	"context"
	"log"
	"strings"

	"github.com/rmcluster/backend/server/gcas"
	"github.com/rmcluster/backend/tracker"
)

func NewGCASSubscriber(g gcas.GCAS) *GCASSubscriber {
	return &GCASSubscriber{
		gcas: g,
	}
}

type GCASSubscriber struct {
	gcas gcas.GCAS
}

type deviceMetadataUpserter interface {
	UpsertDeviceMetadata(ctx context.Context, nodeID, displayName string) error
}

// OnNodeAdded implements [tracker.TrackerSubscriber].
func (g *GCASSubscriber) OnNodeAdded(node tracker.RpcServerInfo) {
	g.upsertDeviceMetadata(node)
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
	g.upsertDeviceMetadata(node)
	if node.StoragePort == 0 {
		g.gcas.RemoveNode(node.Id)
		return
	}
	g.gcas.ReplaceNode(gcas.NewRemoteCAS(node.Id, node.Ip, node.StoragePort))
}

func (g *GCASSubscriber) upsertDeviceMetadata(node tracker.RpcServerInfo) {
	upserter, ok := g.gcas.(deviceMetadataUpserter)
	if !ok {
		return
	}
	displayName := strings.TrimSpace(node.Nickname)
	if displayName == "" {
		displayName = strings.TrimSpace(node.HardwareModel)
	}
	if displayName == "" {
		displayName = "Unknown device"
	}
	if err := upserter.UpsertDeviceMetadata(context.Background(), node.Id, displayName); err != nil {
		log.Printf("gcas: failed to persist device metadata for %s: %v", node.Id, err)
	}
}

var _ tracker.TrackerSubscriber = (*GCASSubscriber)(nil)
