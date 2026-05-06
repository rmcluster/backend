package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestService(t *testing.T) *StorageServiceImpl {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	t.Cleanup(func() { db.Close() })

	svc, err := NewStorageService(db, nil)
	if err != nil {
		t.Fatalf("NewStorageService: %v", err)
	}
	return svc
}

func TestNormalizePath(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"/", "/", false},
		{"/a", "/a", false},
		{"a", "/a", false},
		{"/a/b/", "/a/b", false},
		{"/a//b", "/a/b", false},
		{"/a/./b", "/a/b", false},
		{"/a/../b", "/b", false},
		{"/a/../../b", "/b", false},
		{"", "", true},
		{"   ", "", true},
		{"a\x00b", "", true},
		{`a\b`, "", true},
	}
	for _, c := range cases {
		got, err := normalisePath(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("normalisePath(%q): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalisePath(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("normalisePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStatRoot(t *testing.T) {
	svc := newTestService(t)
	fi, err := svc.Stat(context.Background(), "/")
	if err != nil {
		t.Fatalf("Stat /: %v", err)
	}
	if !fi.IsDir() {
		t.Errorf("root directory should exist")
	}
}

func TestStatNotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.Stat(context.Background(), "/missing")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("want os.ErrNotExist, got %v", err)
	}
}

func TestMkdirAndStat(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	if err := svc.Mkdir(ctx, "/foo", 0o755); err != nil {
		t.Fatalf("mkdir /foo: %v", err)
	}
	fi, err := svc.Stat(ctx, "/foo")
	if err != nil {
		t.Fatalf("stat /foo: %v", err)
	}
	if !fi.IsDir() {
		t.Error("foo should be a directory")
	}
}

func TestMkdirParentMissing(t *testing.T) {
	svc := newTestService(t)
	err := svc.Mkdir(context.Background(), "/a/b", 0o755)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("want os.ErrNotExist, got %v", err)
	}
}

func TestMkdirAlreadyExists(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	if err := svc.Mkdir(ctx, "/a", 0o755); err != nil {
		t.Fatal(err)
	}
	err := svc.Mkdir(ctx, "/a", 0o755)
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("want os.ErrExist, got %v", err)
	}
}

func TestMkdirRoot(t *testing.T) {
	svc := newTestService(t)
	err := svc.Mkdir(context.Background(), "/", 0o755)
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("want os.ErrExist for /, got %v", err)
	}
}
