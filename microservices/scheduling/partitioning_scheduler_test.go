package scheduling

import (
	"net/http/httputil"
	"sync"
	"testing"
	"time"

	"github.com/openai/openai-go/v2"
)

type testInstance struct {
	model     string
	waitReady chan struct{}
	stopped   chan struct{}
	stopOnce  sync.Once
}

func (t *testInstance) Model() string                        { return t.model }
func (t *testInstance) GetOpenAIClient() openai.Client       { return openai.Client{} }
func (t *testInstance) ReverseProxy() *httputil.ReverseProxy { return nil }
func (t *testInstance) WaitReady() error {
	if t.waitReady != nil {
		<-t.waitReady
	}
	return nil
}
func (t *testInstance) Stop() {
	t.stopOnce.Do(func() {
		if t.stopped != nil {
			close(t.stopped)
		}
	})
}
func (t *testInstance) Kill()             {}
func (t *testInstance) AwaitTermination() {}

type testNode struct {
	id            string
	hardwareModel string
	maxSize       int64
}

func (t *testNode) Id() string            { return t.id }
func (t *testNode) HardwareModel() string { return t.hardwareModel }
func (t *testNode) Ip() string            { return "" }
func (t *testNode) Port() int             { return 0 }
func (t *testNode) MaxSize() int64        { return t.maxSize }

type testFactory struct {
	mu         sync.Mutex
	startCalls []int
}

func (t *testFactory) StartInstance(model string, nodes []Node) (Instance, error) {
	t.mu.Lock()
	t.startCalls = append(t.startCalls, len(nodes))
	t.mu.Unlock()
	return &testInstance{model: model}, nil
}

func (t *testFactory) StartCounts() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int(nil), t.startCalls...)
}

type testTask struct {
	model     string
	started   chan struct{}
	allocated []string
	failErr   error
	releaseCh chan struct{}
	startOnce sync.Once
}

func (t *testTask) Model() string { return t.model }
func (t *testTask) Fail(err error) {
	t.failErr = err
	t.startOnce.Do(func() { close(t.started) })
}
func (t *testTask) PerformInference(instance Instance) error {
	t.startOnce.Do(func() { close(t.started) })
	if t.releaseCh != nil {
		<-t.releaseCh
	}
	return nil
}
func (t *testTask) SetAllocatedNodes(nodes []Node) {
	t.allocated = t.allocated[:0]
	for _, node := range nodes {
		t.allocated = append(t.allocated, node.Id())
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %v", timeout)
}

func TestPartitioningSchedulerParallelismTarget(t *testing.T) {
	scheduler := NewPartitioningScheduler(&testFactory{}, 3)

	if got := scheduler.GetParallelismTarget(); got != 3 {
		t.Fatalf("GetParallelismTarget() = %d, want 3", got)
	}

	scheduler.SetParallelismTarget(6)
	if got := scheduler.GetParallelismTarget(); got != 6 {
		t.Fatalf("GetParallelismTarget() after set = %d, want 6", got)
	}

	scheduler.SetParallelismTarget(0)
	if got := scheduler.GetParallelismTarget(); got != 1 {
		t.Fatalf("GetParallelismTarget() after clamped set = %d, want 1", got)
	}
}

func TestPartitioningSchedulerSnapshotAllocations(t *testing.T) {
	scheduler := NewPartitioningScheduler(&testFactory{}, 2)
	instance := &testInstance{model: "test-model"}
	scheduler.allocatedNodes["node-b"] = NodeAllocationInfo{instance: instance, node: &testNode{id: "node-b"}}
	scheduler.allocatedNodes["node-a"] = NodeAllocationInfo{instance: instance, node: &testNode{id: "node-a"}}
	scheduler.refreshAllocationSnapshot()

	got := scheduler.GetAllocatedNodesForModel("test-model")
	want := []string{"node-a", "node-b"}
	if len(got) != len(want) {
		t.Fatalf("GetAllocatedNodesForModel() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("GetAllocatedNodesForModel()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	snapshot := scheduler.SnapshotAllocations()
	snapshot["test-model"][0] = "mutated"
	again := scheduler.GetAllocatedNodesForModel("test-model")
	if again[0] != "node-a" {
		t.Fatalf("snapshot mutation leaked into scheduler state: %v", again)
	}
}

func TestPartitioningSchedulerUsesAvailableNodesUpToTarget(t *testing.T) {
	factory := &testFactory{}
	scheduler := NewPartitioningScheduler(factory, 2)
	task := &testTask{model: "demo", started: make(chan struct{})}

	scheduler.OnNodeConnect(&testNode{id: "node-1"})
	scheduler.OnNewTask(task)

	waitUntil(t, time.Second, func() bool {
		return len(factory.StartCounts()) == 1
	})

	<-task.started
	got := factory.StartCounts()
	if got[0] != 1 {
		t.Fatalf("StartInstance used %d nodes, want 1 available node", got[0])
	}
	if len(task.allocated) != 1 || task.allocated[0] != "node-1" {
		t.Fatalf("allocated nodes = %v, want [node-1]", task.allocated)
	}
}

func TestPartitioningSchedulerDoesNotReuseIdleInstanceWithWrongTarget(t *testing.T) {
	factory := &testFactory{}
	scheduler := NewPartitioningScheduler(factory, 1)
	scheduler.OnNodeConnect(&testNode{id: "node-1"})
	scheduler.OnNodeConnect(&testNode{id: "node-2"})

	first := &testTask{model: "demo", started: make(chan struct{})}
	scheduler.OnNewTask(first)
	<-first.started
	waitUntil(t, time.Second, func() bool {
		return len(scheduler.idleInstances["demo"]) == 1
	})

	scheduler.SetParallelismTarget(2)
	second := &testTask{model: "demo", started: make(chan struct{})}
	scheduler.OnNewTask(second)
	<-second.started

	got := factory.StartCounts()
	if len(got) != 2 {
		t.Fatalf("StartInstance call count = %d, want 2 (%v)", len(got), got)
	}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("StartInstance node counts = %v, want [1 2]", got)
	}
	if len(second.allocated) != 2 {
		t.Fatalf("second task allocated nodes = %v, want 2 nodes", second.allocated)
	}
}

func TestPartitioningSchedulerPrefersLargestNodes(t *testing.T) {
	factory := &testFactory{}
	scheduler := NewPartitioningScheduler(factory, 2)
	scheduler.OnNodeConnect(&testNode{id: "small", maxSize: 100})
	scheduler.OnNodeConnect(&testNode{id: "large", maxSize: 300})
	scheduler.OnNodeConnect(&testNode{id: "medium", maxSize: 200})

	task := &testTask{model: "demo", started: make(chan struct{})}
	scheduler.OnNewTask(task)
	<-task.started

	if len(task.allocated) != 2 {
		t.Fatalf("allocated nodes = %v, want 2 nodes", task.allocated)
	}

	got := map[string]bool{}
	for _, nodeID := range task.allocated {
		got[nodeID] = true
	}
	if got["small"] {
		t.Fatalf("allocated nodes = %v, should not include lowest-capacity node", task.allocated)
	}
	if !got["large"] || !got["medium"] {
		t.Fatalf("allocated nodes = %v, want large and medium nodes", task.allocated)
	}
}
