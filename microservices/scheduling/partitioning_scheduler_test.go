package scheduling

import "testing"

type testNode struct {
	id            string
	maxSize       int64
	nickname      string
	hardwareModel string
}

func (n testNode) Id() string            { return n.id }
func (n testNode) Ip() string            { return "" }
func (n testNode) Port() int             { return 0 }
func (n testNode) MaxSize() int64        { return n.maxSize }
func (n testNode) Nickname() string      { return n.nickname }
func (n testNode) HardwareModel() string { return n.hardwareModel }

type testModelSizer struct {
	modelBytes int64
}

func (s testModelSizer) StartInstance(model string, nodes []Node) (Instance, error) {
	panic("not used in node count tests")
}

func (s testModelSizer) ModelSizeBytes(model string) (int64, error) {
	return s.modelBytes, nil
}

func TestRequiredNodeCountForModelBytesUsesNinetyPercentThreshold(t *testing.T) {
	t.Parallel()

	nodes := []Node{
		testNode{id: "a", maxSize: 1_000},
		testNode{id: "b", maxSize: 800},
		testNode{id: "c", maxSize: 500},
	}

	got := requiredNodeCountForModelBytes(1_500, nodes, 0.9)
	if got != 2 {
		t.Fatalf("expected 2 nodes, got %d", got)
	}
}

func TestRequiredNodeCountForModelBytesFallsBackToAllNodesWhenModelIsTooLarge(t *testing.T) {
	t.Parallel()

	nodes := []Node{
		testNode{id: "a", maxSize: 1_000},
		testNode{id: "b", maxSize: 800},
	}

	got := requiredNodeCountForModelBytes(5_000, nodes, 0.9)
	if got != 2 {
		t.Fatalf("expected all nodes, got %d", got)
	}
}

func TestRequiredNodeCountForModelBytesNeedsOnlyOneLargeNode(t *testing.T) {
	t.Parallel()

	nodes := []Node{
		testNode{id: "a", maxSize: 2_000},
		testNode{id: "b", maxSize: 1_000},
		testNode{id: "c", maxSize: 1_000},
	}

	got := requiredNodeCountForModelBytes(1_700, nodes, 0.9)
	if got != 1 {
		t.Fatalf("expected 1 node, got %d", got)
	}
}

func TestRequiredNodeCountUsesParallelismTargetOverride(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 100},
		unallocatedNodes: map[string]Node{
			"a": testNode{id: "a", maxSize: 1_000},
			"b": testNode{id: "b", maxSize: 800},
			"c": testNode{id: "c", maxSize: 500},
		},
		parallelismTarget: 2,
	}

	got := scheduler.requiredNodeCount("model")
	if got != 2 {
		t.Fatalf("expected fixed override of 2 nodes, got %d", got)
	}
}

func TestRequiredNodeCountAutoSizesUsingNinetyPercentCapacity(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 1_500},
		unallocatedNodes: map[string]Node{
			"a": testNode{id: "a", maxSize: 1_000},
			"b": testNode{id: "b", maxSize: 800},
			"c": testNode{id: "c", maxSize: 500},
		},
	}

	got := scheduler.requiredNodeCount("model")
	if got != 2 {
		t.Fatalf("expected auto-sized count of 2 nodes, got %d", got)
	}
}

func TestSelectNodesUsesSmallestSufficientNodeForSinglePhoneModel(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 600},
		unallocatedNodes: map[string]Node{
			"a": testNode{id: "a", maxSize: 1_500},
			"b": testNode{id: "b", maxSize: 700},
			"c": testNode{id: "c", maxSize: 1_000},
		},
	}

	got := scheduler.selectNodes("model")
	if len(got) != 1 {
		t.Fatalf("expected 1 node, got %d", len(got))
	}
	if got[0].Id() != "b" {
		t.Fatalf("expected smallest sufficient node b, got %q", got[0].Id())
	}
}

func TestSelectNodesKeepsLargestFirstForMultiPhoneModel(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 1_500},
		unallocatedNodes: map[string]Node{
			"a": testNode{id: "a", maxSize: 1_500},
			"b": testNode{id: "b", maxSize: 700},
			"c": testNode{id: "c", maxSize: 1_000},
		},
	}

	got := scheduler.selectNodes("model")
	if len(got) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(got))
	}
	if got[0].Id() != "a" || got[1].Id() != "c" {
		t.Fatalf("expected largest-first multi-node selection [a c], got [%s %s]", got[0].Id(), got[1].Id())
	}
}

func TestCheckModelFitsOnNodesAllowsFitWithinRawCapacity(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 1_600},
	}

	err := scheduler.checkModelFitsOnNodes("model", []Node{
		testNode{id: "a", maxSize: 1_000},
		testNode{id: "b", maxSize: 800},
	})
	if err != nil {
		t.Fatalf("expected fit to succeed, got %v", err)
	}
}

func TestCheckModelFitsOnNodesFailsWhenRawCapacityIsTooSmall(t *testing.T) {
	t.Parallel()

	scheduler := &PartitioningScheduler{
		instanceFactory: testModelSizer{modelBytes: 1_900},
	}

	err := scheduler.checkModelFitsOnNodes("model", []Node{
		testNode{id: "a", maxSize: 1_000},
		testNode{id: "b", maxSize: 800},
	})
	if err == nil {
		t.Fatal("expected fit check to fail")
	}
}
