package llama

import "testing"

func TestParseCachedModelRef(t *testing.T) {
	repo, quant, ok := parseCachedModelRef("unsloth/gemma-3-1b-it-GGUF:Q4_K_M")
	if !ok {
		t.Fatalf("expected cached model ref to parse")
	}
	if repo != "unsloth/gemma-3-1b-it-GGUF" {
		t.Fatalf("unexpected repo: %q", repo)
	}
	if quant != "Q4_K_M" {
		t.Fatalf("unexpected quant: %q", quant)
	}
}
