package gcassubscriber

import (
	"context"
	"testing"

	"github.com/rmcluster/backend/server/gcas"
	"github.com/rmcluster/backend/tracker"
)

// mockGCAS is a mock implementation of gcas.GCAS for testing
type mockGCAS struct {
	addNodeCalls     []gcas.NamedCAS
	removeNodeCalls  []string
	replaceNodeCalls []gcas.NamedCAS
}

func (m *mockGCAS) AddNode(node gcas.NamedCAS) {
	m.addNodeCalls = append(m.addNodeCalls, node)
}

func (m *mockGCAS) RemoveNode(nodeName string) {
	m.removeNodeCalls = append(m.removeNodeCalls, nodeName)
}

func (m *mockGCAS) ReplaceNode(node gcas.NamedCAS) {
	m.replaceNodeCalls = append(m.replaceNodeCalls, node)
}

// Implement CAS interface methods (not used in subscriber, but required for interface)
func (m *mockGCAS) Put(ctx context.Context, hash gcas.Hash, data []byte) error {
	return nil
}

func (m *mockGCAS) Get(ctx context.Context, hash gcas.Hash) ([]byte, error) {
	return nil, nil
}

func (m *mockGCAS) Delete(ctx context.Context, hash gcas.Hash) error {
	return nil
}

func (m *mockGCAS) List(ctx context.Context) (<-chan gcas.Hash, error) {
	return nil, nil
}

func (m *mockGCAS) FreeSpace(ctx context.Context) (int64, error) {
	return 0, nil
}

func (m *mockGCAS) RunMaintenance(ctx context.Context) error {
	return nil
}

func (m *mockGCAS) Repair(ctx context.Context) error {
	return nil
}

var _ gcas.GCAS = (*mockGCAS)(nil)

func TestNewGCASSubscriber(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	if subscriber == nil {
		t.Fatal("NewGCASSubscriber returned nil")
	}

	if subscriber.gcas != mockGcas {
		t.Error("GCASSubscriber.gcas not set correctly")
	}
}

func TestOnNodeAdded_WithStoragePort(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 9090,
	}

	subscriber.OnNodeAdded(node)

	if len(mockGcas.addNodeCalls) != 1 {
		t.Fatalf("Expected 1 AddNode call, got %d", len(mockGcas.addNodeCalls))
	}

	addedNode := mockGcas.addNodeCalls[0]
	if addedNode.Name() != "node1" {
		t.Errorf("Expected node name 'node1', got '%s'", addedNode.Name())
	}
}

func TestOnNodeAdded_WithoutStoragePort(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 0,
	}

	subscriber.OnNodeAdded(node)

	if len(mockGcas.addNodeCalls) != 0 {
		t.Fatalf("Expected 0 AddNode calls, got %d", len(mockGcas.addNodeCalls))
	}
}

func TestOnNodeRemoved(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 9090,
	}

	subscriber.OnNodeRemoved(node)

	if len(mockGcas.removeNodeCalls) != 1 {
		t.Fatalf("Expected 1 RemoveNode call, got %d", len(mockGcas.removeNodeCalls))
	}

	if mockGcas.removeNodeCalls[0] != "node1" {
		t.Errorf("Expected node ID 'node1', got '%s'", mockGcas.removeNodeCalls[0])
	}
}

func TestOnNodeRemoved_IgnoresStoragePort(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 0,
	}

	subscriber.OnNodeRemoved(node)

	if len(mockGcas.removeNodeCalls) != 1 {
		t.Fatalf("Expected 1 RemoveNode call, got %d", len(mockGcas.removeNodeCalls))
	}

	if mockGcas.removeNodeCalls[0] != "node1" {
		t.Errorf("Expected node ID 'node1', got '%s'", mockGcas.removeNodeCalls[0])
	}
}

func TestOnNodeUpdated_WithStoragePort(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 9090,
	}

	subscriber.OnNodeUpdated(node)

	if len(mockGcas.replaceNodeCalls) != 1 {
		t.Fatalf("Expected 1 ReplaceNode call, got %d", len(mockGcas.replaceNodeCalls))
	}

	replacedNode := mockGcas.replaceNodeCalls[0]
	if replacedNode.Name() != "node1" {
		t.Errorf("Expected node name 'node1', got '%s'", replacedNode.Name())
	}
}

func TestOnNodeUpdated_WithoutStoragePort(t *testing.T) {
	mockGcas := &mockGCAS{}
	subscriber := NewGCASSubscriber(mockGcas)

	node := tracker.RpcServerInfo{
		Id:          "node1",
		Ip:          "192.168.1.1",
		Port:        8080,
		StoragePort: 0,
	}

	subscriber.OnNodeUpdated(node)

	if len(mockGcas.replaceNodeCalls) != 0 {
		t.Fatalf("Expected 0 ReplaceNode calls, got %d", len(mockGcas.replaceNodeCalls))
	}

	if len(mockGcas.removeNodeCalls) != 1 {
		t.Fatalf("Expected 1 RemoveNode call, got %d", len(mockGcas.removeNodeCalls))
	}

	if mockGcas.removeNodeCalls[0] != "node1" {
		t.Errorf("Expected node ID 'node1', got '%s'", mockGcas.removeNodeCalls[0])
	}
}

func TestTrackerSubscriberInterface(t *testing.T) {
	// Verify that GCASSubscriber implements tracker.TrackerSubscriber
	var _ tracker.TrackerSubscriber = (*GCASSubscriber)(nil)
}
