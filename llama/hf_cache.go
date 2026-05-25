package llama

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func resolveCachedHFModelPath(model string) (string, bool, error) {
	repo, filename, ok := parseHFModelRef(model)
	if !ok {
		return "", false, nil
	}

	cacheRoot, err := huggingFaceHubCacheDir()
	if err != nil {
		return "", false, err
	}

	repoDir := filepath.Join(cacheRoot, "models--"+strings.ReplaceAll(repo, "/", "--"))
	snapshotsDir := filepath.Join(repoDir, "snapshots")
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		candidate := filepath.Join(snapshotsDir, entry.Name(), filename)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, true, nil
		}
	}

	return "", false, nil
}

func parseHFModelRef(model string) (repo string, filename string, ok bool) {
	if !strings.HasPrefix(model, "hf:") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(model, "hf:")
	repo, filename, ok = strings.Cut(trimmed, ":")
	if !ok || repo == "" || filename == "" {
		return "", "", false
	}
	return repo, filename, true
}

func huggingFaceHubCacheDir() (string, error) {
	if cacheDir := strings.TrimSpace(os.Getenv("HUGGINGFACE_HUB_CACHE")); cacheDir != "" {
		return cacheDir, nil
	}
	if hfHome := strings.TrimSpace(os.Getenv("HF_HOME")); hfHome != "" {
		return filepath.Join(hfHome, "hub"), nil
	}
	if xdgCache := strings.TrimSpace(os.Getenv("XDG_CACHE_HOME")); xdgCache != "" {
		return filepath.Join(xdgCache, "huggingface", "hub"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for hugging face cache: %w", err)
	}
	return filepath.Join(home, ".cache", "huggingface", "hub"), nil
}
