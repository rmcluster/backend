package uiapi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func localModelStorageDir() string {
	if dir := strings.TrimSpace(os.Getenv("RMD_MODEL_STORAGE_DIR")); dir != "" {
		return dir
	}
	if cacheDir, err := os.UserCacheDir(); err == nil && cacheDir != "" {
		return filepath.Join(cacheDir, "rmd", "models")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".cache", "rmd", "models")
	}
	return filepath.Join(".", ".rmd", "models")
}

func uniqueStoragePath(dir string, baseName string) string {
	baseName = filepath.Base(baseName)
	stem := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	ext := filepath.Ext(baseName)
	path := filepath.Join(dir, baseName)
	for index := 1; ; index++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return path
		}
		path = filepath.Join(dir, fmt.Sprintf("%s-%d%s", stem, index, ext))
	}
}
