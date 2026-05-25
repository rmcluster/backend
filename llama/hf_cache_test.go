package llama

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseHFModelRef(t *testing.T) {
	repo, filename, ok := parseHFModelRef("hf:unsloth/Qwen3-1.7B-GGUF:Qwen3-1.7B-Q4_K_M.gguf")
	if !ok {
		t.Fatalf("parseHFModelRef returned ok=false")
	}
	if repo != "unsloth/Qwen3-1.7B-GGUF" {
		t.Fatalf("repo = %q", repo)
	}
	if filename != "Qwen3-1.7B-Q4_K_M.gguf" {
		t.Fatalf("filename = %q", filename)
	}
}

func TestResolveCachedHFModelPath(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HUGGINGFACE_HUB_CACHE", tmp)
	t.Setenv("HF_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")

	model := "hf:unsloth/Qwen3-1.7B-GGUF:Qwen3-1.7B-Q4_K_M.gguf"
	repoDir := filepath.Join(tmp, "models--unsloth--Qwen3-1.7B-GGUF", "snapshots", "abc123")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	expected := filepath.Join(repoDir, "Qwen3-1.7B-Q4_K_M.gguf")
	if err := os.WriteFile(expected, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, ok, err := resolveCachedHFModelPath(model)
	if err != nil {
		t.Fatalf("resolveCachedHFModelPath error: %v", err)
	}
	if !ok {
		t.Fatalf("resolveCachedHFModelPath ok=false")
	}
	if got != expected {
		t.Fatalf("path = %q, want %q", got, expected)
	}
}
