package schedulersubscriber

import (
	"sync"
	"testing"

	"github.com/rmcluster/backend/server/scheduling"
	"github.com/rmcluster/backend/tracker"
)

// MockScheduler implements scheduling.Scheduler for testing
type MockScheduler struct {
	mu                 sync.Mutex
	ConnectCalls       []scheduling.Node
	DisconnectCalls    []scheduling.Node
	ConnectCount       int
	DisconnectCount    int
}

func (m *MockScheduler) OnNewTask(task scheduling.Task) {
	// Not used in this package's tests
}

func (m *MockScheduler) OnTaskCancelled(task scheduling.Task) {
	// Not used in this package's tests
}

func (m *MockScheduler) OnNodeConnect(node scheduling.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ConnectCalls = append(m.ConnectCalls, node)
	m.ConnectCount++
}

func (m *MockScheduler) OnNodeDisconnect(node scheduling.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DisconnectCalls = append(m.DisconnectCalls, node)
	m.DisconnectCount++
}

func TestNewSchedulerSubscriber(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	if subscriber == nil {
		t.Fatal("NewSchedulerSubscriber returned nil")
	}
	if subscriber.scheduler == nil {
		t.Error("scheduler not set correctly")
	}
	if subscriber.nodes == nil {
		t.Error("nodes map not initialized")
	}
	if len(subscriber.nodes) != 0 {
		t.Error("nodes map should be empty initially")
	}
}

func TestNodeGetters(t *testing.T) {
	n := &node{
		id:            "192.168.1.1:8080",
		ip:            "192.168.1.1",
		port:          8080,
		storagePort:   9000,
		maxSize:       1000000,
		nickname:      "test-node",
		hardwareModel: "GPU-V100",
		battery:       85.5,
		temperature:   45.2,
	}

	tests := []struct {
		name     string
		getter   func() interface{}
		expected interface{}
	}{
		{"Id", func() interface{} { return n.Id() }, "192.168.1.1:8080"},
		{"Ip", func() interface{} { return n.Ip() }, "192.168.1.1"},
		{"Port", func() interface{} { return n.Port() }, 8080},
		{"StoragePort", func() interface{} { return n.StoragePort() }, 9000},
		{"MaxSize", func() interface{} { return n.MaxSize() }, int64(1000000)},
		{"Nickname", func() interface{} { return n.Nickname() }, "test-node"},
		{"HardwareModel", func() interface{} { return n.HardwareModel() }, "GPU-V100"},
		{"Battery", func() interface{} { return n.Battery() }, 85.5},
		{"Temperature", func() interface{} { return n.Temperature() }, 45.2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.getter()
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestOnNodeAdded(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	subscriber.OnNodeAdded(trackerNode)

	if mockScheduler.ConnectCount != 1 {
		t.Errorf("OnNodeConnect not called, got %d calls", mockScheduler.ConnectCount)
	}

	if len(subscriber.nodes) != 1 {
		t.Errorf("expected 1 node in map, got %d", len(subscriber.nodes))
	}

	storedNode := subscriber.nodes["node1"]
	if storedNode == nil {
		t.Fatal("node not stored in map")
	}

	if storedNode.ip != "192.168.1.1" {
		t.Errorf("ip mismatch: got %s", storedNode.ip)
	}
	if storedNode.port != 8080 {
		t.Errorf("port mismatch: got %d", storedNode.port)
	}
	if storedNode.maxSize != 5000000 {
		t.Errorf("maxSize mismatch: got %d", storedNode.maxSize)
	}
}

func TestOnNodeRemoved(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add node first
	subscriber.OnNodeAdded(trackerNode)
	if len(subscriber.nodes) != 1 {
		t.Fatal("node not added")
	}

	// Remove node
	subscriber.OnNodeRemoved(trackerNode)

	if len(subscriber.nodes) != 0 {
		t.Errorf("expected 0 nodes in map after removal, got %d", len(subscriber.nodes))
	}

	if mockScheduler.DisconnectCount != 1 {
		t.Errorf("OnNodeDisconnect not called, got %d calls", mockScheduler.DisconnectCount)
	}
}

func TestOnNodeUpdatedNodeNotFound(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Update node that doesn't exist (should trigger OnNodeAdded)
	subscriber.OnNodeUpdated(trackerNode)

	if mockScheduler.ConnectCount != 1 {
		t.Errorf("expected OnNodeConnect to be called once, got %d", mockScheduler.ConnectCount)
	}

	if len(subscriber.nodes) != 1 {
		t.Errorf("expected node to be added, got %d nodes", len(subscriber.nodes))
	}
}

func TestOnNodeUpdatedNoChanges(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add node
	subscriber.OnNodeAdded(trackerNode)
	mockScheduler.ConnectCount = 0 // Reset count

	// Update with same values
	subscriber.OnNodeUpdated(trackerNode)

	if mockScheduler.ConnectCount != 0 {
		t.Errorf("OnNodeConnect should not be called on no changes, got %d", mockScheduler.ConnectCount)
	}
	if mockScheduler.DisconnectCount != 0 {
		t.Errorf("OnNodeDisconnect should not be called on no changes, got %d", mockScheduler.DisconnectCount)
	}
}

func TestOnNodeUpdatedIpChanged(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add node
	subscriber.OnNodeAdded(trackerNode)
	mockScheduler.ConnectCount = 0
	mockScheduler.DisconnectCount = 0

	// Update with different IP
	updatedNode := trackerNode
	updatedNode.Ip = "192.168.1.2"
	subscriber.OnNodeUpdated(updatedNode)

	if mockScheduler.DisconnectCount != 1 {
		t.Errorf("OnNodeDisconnect should be called once, got %d", mockScheduler.DisconnectCount)
	}
	if mockScheduler.ConnectCount != 1 {
		t.Errorf("OnNodeConnect should be called once, got %d", mockScheduler.ConnectCount)
	}
}

func TestOnNodeUpdatedPortChanged(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add node
	subscriber.OnNodeAdded(trackerNode)
	mockScheduler.ConnectCount = 0
	mockScheduler.DisconnectCount = 0

	// Update with different port
	updatedNode := trackerNode
	updatedNode.Port = 8081
	subscriber.OnNodeUpdated(updatedNode)

	if mockScheduler.DisconnectCount != 1 {
		t.Errorf("OnNodeDisconnect should be called once, got %d", mockScheduler.DisconnectCount)
	}
	if mockScheduler.ConnectCount != 1 {
		t.Errorf("OnNodeConnect should be called once, got %d", mockScheduler.ConnectCount)
	}
}

func TestOnNodeUpdatedMaxSizeChanged(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add node
	subscriber.OnNodeAdded(trackerNode)
	mockScheduler.ConnectCount = 0
	mockScheduler.DisconnectCount = 0

	// Update with different maxSize
	updatedNode := trackerNode
	updatedNode.MaxSize = 6000000
	subscriber.OnNodeUpdated(updatedNode)

	if mockScheduler.DisconnectCount != 1 {
		t.Errorf("OnNodeDisconnect should be called once, got %d", mockScheduler.DisconnectCount)
	}
	if mockScheduler.ConnectCount != 1 {
		t.Errorf("OnNodeConnect should be called once, got %d", mockScheduler.ConnectCount)
	}
}

func TestConvertTrackerNode(t *testing.T) {
	trackerNode := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "test-node",
		HardwareModel: "GPU-A100",
		Battery:       85.5,
		Temperature:   42.3,
	}

	converted := convertTrackerNode(trackerNode)

	if converted.id != "192.168.1.1:8080" {
		t.Errorf("id conversion failed, got %s", converted.id)
	}
	if converted.ip != "192.168.1.1" {
		t.Errorf("ip not converted correctly")
	}
	if converted.port != 8080 {
		t.Errorf("port not converted correctly")
	}
	if converted.storagePort != 9000 {
		t.Errorf("storagePort not converted correctly")
	}
	if converted.maxSize != 5000000 {
		t.Errorf("maxSize not converted correctly")
	}
	if converted.nickname != "test-node" {
		t.Errorf("nickname not converted correctly")
	}
	if converted.hardwareModel != "GPU-A100" {
		t.Errorf("hardwareModel not converted correctly")
	}
	if converted.battery != 85.5 {
		t.Errorf("battery not converted correctly")
	}
	if converted.temperature != 42.3 {
		t.Errorf("temperature not converted correctly")
	}
}

func TestOnNodeUpdatedMultipleNodes(t *testing.T) {
	mockScheduler := &MockScheduler{}
	subscriber := NewSchedulerSubscriber(mockScheduler)

	node1 := tracker.RpcServerInfo{
		Id:            "node1",
		Ip:            "192.168.1.1",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "node1",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	node2 := tracker.RpcServerInfo{
		Id:            "node2",
		Ip:            "192.168.1.2",
		Port:          8080,
		StoragePort:   9000,
		MaxSize:       5000000,
		Nickname:      "node2",
		HardwareModel: "GPU-A100",
		Battery:       90.0,
		Temperature:   40.0,
	}

	// Add both nodes
	subscriber.OnNodeAdded(node1)
	subscriber.OnNodeAdded(node2)

	if len(subscriber.nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(subscriber.nodes))
	}

	mockScheduler.ConnectCount = 0
	mockScheduler.DisconnectCount = 0

	// Update node1 with new port
	updatedNode1 := node1
	updatedNode1.Port = 8081
	subscriber.OnNodeUpdated(updatedNode1)

	// node2 should not be affected
	if len(subscriber.nodes) != 2 {
		t.Errorf("expected 2 nodes after update, got %d", len(subscriber.nodes))
	}

	// Check that only node1 was updated
	if mockScheduler.DisconnectCount != 1 || mockScheduler.ConnectCount != 1 {
		t.Errorf("expected 1 disconnect and 1 connect, got %d disconnects and %d connects",
			mockScheduler.DisconnectCount, mockScheduler.ConnectCount)
	}
}

func TestNodeInterfaceImplementation(t *testing.T) {
	var _ scheduling.Node = (*node)(nil)
}

func TestSubscriberInterfaceImplementation(t *testing.T) {
	var _ tracker.TrackerSubscriber = (*SchedulerSubscriber)(nil)
}
