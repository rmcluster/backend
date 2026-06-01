package scheduling

import (
	"testing"

	"github.com/rmcluster/backend/llama"
)

func TestEventDrivenSchedulerParallelismTarget(t *testing.T) {
	scheduler := NewEventDrivenScheduler(nil, llama.Llama{}, 1.0, 3)

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

func TestEventDrivenPickNodesRespectsParallelismTarget(t *testing.T) {
	t.Parallel()

	scheduler := &EventDrivenScheduler{}
	scheduler.parallelismTarget.Store(2)

	scheduler.unallocatedNodes = map[string]Node{
		"a": testNode{id: "a", maxSize: 1_000},
		"b": testNode{id: "b", maxSize: 800},
		"c": testNode{id: "c", maxSize: 500},
	}

	nodes, ok := scheduler.pickNodesForNewInstance("model")
	if !ok {
		t.Fatal("expected node selection to succeed")
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].Id() != "a" || nodes[1].Id() != "b" {
		t.Fatalf("expected largest-first [a b], got [%s %s]", nodes[0].Id(), nodes[1].Id())
	}
}

type testNode struct {
	id      string
	maxSize int64
}

func (n testNode) Id() string            { return n.id }
func (n testNode) Ip() string            { return "" }
func (n testNode) Port() int             { return 0 }
func (n testNode) MaxSize() int64        { return n.maxSize }
func (n testNode) Nickname() string      { return "" }
func (n testNode) StoragePort() int      { return 0 }
func (n testNode) HardwareModel() string { return "" }
func (n testNode) Battery() float64      { return 0 }
func (n testNode) Temperature() float64  { return 0 }
