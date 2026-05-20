package uiapi

import (
	"os"
	"path/filepath"
	"strings"
)

func localModelStorageDir() string {
	if dir := strings.TrimSpace(os.Getenv("RMD_MODEL_STORAGE_DIR")); dir != "" {
		return dir
	}
	if xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdg != "" {
		return filepath.Join(xdg, "rmd", "models")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".local", "share", "rmd", "models")
	}
	return filepath.Join(".", ".rmd", "models")
}
