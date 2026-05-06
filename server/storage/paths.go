package storage

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

var (
	ErrInvalidPath   = errors.New("invalid path")
	ErrNotDir        = errors.New("not a directory")
	ErrIsDir         = errors.New("is a directory")
	ErrAlreadyExists = errors.New("already exists")
)

func normalisePath(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidPath)
	}
	if strings.ContainsRune(name, '\x00') {
		return "", fmt.Errorf("%w: NUL byte not allowed", ErrInvalidPath)
	}
	if strings.Contains(name, `\`) {
		return "", fmt.Errorf("%w: backslash not allowed", ErrInvalidPath)
	}

	clean := path.Clean("/" + name)

	if !strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("%w: not absolute", ErrInvalidPath)
	}

	return clean, nil
}

func parentPath(p string) string {
	if p == "/" {
		return "/"
	}
	parent := path.Dir(p)
	if parent == "." {
		return "/"
	}
	return parent
}

func baseName(p string) string {
	if p == "/" {
		return "/"
	}
	return path.Base(p)
}
