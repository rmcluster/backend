package scheduling

import "testing"

type stubNode struct {
	id      string
	model   string
	ip      string
	port    int
	maxSize int64
}

func (n stubNode) Id() string            { return n.id }
func (n stubNode) HardwareModel() string { return n.model }
func (n stubNode) Ip() string            { return n.ip }
func (n stubNode) Port() int             { return n.port }
func (n stubNode) MaxSize() int64        { return n.maxSize }

func TestChooseOffloadLayersDefaultsToAllLayers(t *testing.T) {
	t.Setenv("LLAMA_OFFLOAD_LAYERS", "")
	layers := chooseOffloadLayers([]Node{stubNode{maxSize: 512 * 1024 * 1024}})
	if layers != 99 {
		t.Fatalf("chooseOffloadLayers() = %d, want 99", layers)
	}
}

func TestChooseOffloadLayersEnvOverride(t *testing.T) {
	t.Setenv("LLAMA_OFFLOAD_LAYERS", "64")
	layers := chooseOffloadLayers([]Node{stubNode{maxSize: 64 * 1024 * 1024}})
	if layers != 64 {
		t.Fatalf("chooseOffloadLayers() = %d, want 64", layers)
	}
}

func TestChooseTensorSplitUsesNodeMaxSizeWeights(t *testing.T) {
	split := chooseTensorSplit([]Node{
		stubNode{maxSize: 300},
		stubNode{maxSize: 100},
	})
	if len(split) != 2 {
		t.Fatalf("len(chooseTensorSplit()) = %d, want 2", len(split))
	}
	if split[0] != 0.75 || split[1] != 0.25 {
		t.Fatalf("chooseTensorSplit() = %v, want [0.75 0.25]", split)
	}
}

func TestChooseTensorSplitSkipsSingleNodeAndUnknownCapacity(t *testing.T) {
	if split := chooseTensorSplit([]Node{stubNode{maxSize: 300}}); split != nil {
		t.Fatalf("chooseTensorSplit(single) = %v, want nil", split)
	}
	if split := chooseTensorSplit([]Node{stubNode{maxSize: 300}, stubNode{maxSize: 0}}); split != nil {
		t.Fatalf("chooseTensorSplit(unknown capacity) = %v, want nil", split)
	}
}
