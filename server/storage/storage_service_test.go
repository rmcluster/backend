package storage

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rmcluster/backend/server/gcas"
	xwebdav "golang.org/x/net/webdav"
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

type fakeCAS struct {
	data map[gcas.Hash][]byte
}

func newFakeCAS() *fakeCAS {
	return &fakeCAS{data: map[gcas.Hash][]byte{}}
}

func (f *fakeCAS) Put(_ context.Context, h gcas.Hash, b []byte) error {
	if _, ok := f.data[h]; ok {
		return &gcas.HashExistsError{}
	}
	f.data[h] = append([]byte{}, b...)
	return nil
}
func (f *fakeCAS) Get(_ context.Context, h gcas.Hash) ([]byte, error) {
	b, ok := f.data[h]
	if !ok {
		return nil, &gcas.HashNotFoundError{}
	}
	return append([]byte{}, b...), nil
}
func (f *fakeCAS) Delete(_ context.Context, h gcas.Hash) error {
	if _, ok := f.data[h]; !ok {
		return &gcas.HashNotFoundError{}
	}
	delete(f.data, h)
	return nil
}
func (f *fakeCAS) List(ctx context.Context) (<-chan gcas.Hash, error) {
	ch := make(chan gcas.Hash, len(f.data))
	for h := range f.data {
		ch <- h
	}
	close(ch)
	return ch, nil
}
func (f *fakeCAS) FreeSpace(_ context.Context) (int64, error) { return 1 << 30, nil }

// Wrap fakeCAS as a GCAS for the constructor (AddNode/RemoveNode are no-ops).
type fakeGCAS struct{ *fakeCAS }

func (f *fakeGCAS) AddNode(_ gcas.NamedCAS)                {}
func (f *fakeGCAS) RemoveNode(_ string)                    {}
func (f *fakeGCAS) ReplaceNode(_ gcas.NamedCAS)            {}
func (f *fakeGCAS) RunMaintenance(_ context.Context) error { return nil }
func (f *fakeGCAS) Repair(_ context.Context) error         { return nil }

func TestOpenFileRead(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cas := newFakeCAS()
	svc, err := NewStorageService(db, &fakeGCAS{cas})
	if err != nil {
		t.Fatal(err)
	}

	chunk1 := []byte("hello, ")
	chunk2 := []byte("world!")
	h1 := sha256.Sum256(chunk1)
	h2 := sha256.Sum256(chunk2)

	if err := cas.Put(ctx, h1, chunk1); err != nil {
		t.Fatal(err)
	}
	if err := cas.Put(ctx, h2, chunk2); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()
	if _, err := db.Exec(`INSERT INTO files VALUES('/hello.txt','/',420,?,?,1)`,
		int64(len(chunk1)+len(chunk2)), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO file_chunks VALUES('/hello.txt',0,?,?)`,
		h1[:], int64(len(chunk1))); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO file_chunks VALUES('/hello.txt',1,?,?)`,
		h2[:], int64(len(chunk2))); err != nil {
		t.Fatal(err)
	}

	f, err := svc.OpenFile(ctx, "/hello.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello, world!" {
		t.Errorf("got %q", got)
	}
}

func TestOpenDirReaddir(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc, err := NewStorageService(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.Mkdir(ctx, "/a", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := svc.Mkdir(ctx, "/b", 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := svc.OpenFile(ctx, "/", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	entries, err := f.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("want 2 entries, got %d", len(entries))
	}
}

func newServiceWithGCAS(t *testing.T) (*StorageServiceImpl, *fakeCAS) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cas := newFakeCAS()
	svc, err := NewStorageService(db, &fakeGCAS{cas})
	if err != nil {
		t.Fatalf("NewStorageService: %v", err)
	}
	return svc, cas
}

func TestStorageServiceDefaultChunkSize(t *testing.T) {
	svc, _ := newServiceWithGCAS(t)
	if got := svc.GetChunkSize(); got != DefaultChunkSize {
		t.Fatalf("GetChunkSize() = %d, want %d", got, DefaultChunkSize)
	}
}

func TestStorageServiceCustomChunkSizeSplitsWrites(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)
	if err := svc.SetChunkSize(4 * 1024 * 1024); err != nil {
		t.Fatalf("SetChunkSize: %v", err)
	}

	f, err := svc.OpenFile(ctx, "/split.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	body := uniqueData(10 * 1024 * 1024)
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if len(cas.data) != 3 {
		t.Fatalf("want 3 chunks in GCAS, got %d", len(cas.data))
	}
}

func TestStorageServiceChunkSizeOnlyAffectsNewWrites(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	mustWrite(t, svc, "/before.txt", uniqueData(10*1024*1024))
	if len(cas.data) != 2 {
		t.Fatalf("before chunk-size change want 2 chunks, got %d", len(cas.data))
	}

	if err := svc.SetChunkSize(4 * 1024 * 1024); err != nil {
		t.Fatalf("SetChunkSize: %v", err)
	}
	mustWrite(t, svc, "/after.txt", uniqueData(10*1024*1024+123))
	if len(cas.data) != 5 {
		t.Fatalf("after chunk-size change want 5 total chunks, got %d", len(cas.data))
	}

	r, err := svc.OpenFile(ctx, "/before.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10*1024*1024 {
		t.Fatalf("before.txt size = %d, want %d", len(got), 10*1024*1024)
	}
}

func TestOpenFileWriteCreate(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	f, err := svc.OpenFile(ctx, "/hello.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	body := []byte("hello, world!")
	if n, err := f.Write(body); err != nil || n != len(body) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Read it back.
	r, err := svc.OpenFile(ctx, "/hello.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Errorf("got %q want %q", got, body)
	}

	// Should have produced exactly one chunk in GCAS.
	if len(cas.data) != 1 {
		t.Errorf("want 1 chunk in GCAS, got %d", len(cas.data))
	}
}

func TestOpenFileWriteOverwriteDecRefs(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	write := func(body []byte) {
		f, err := svc.OpenFile(ctx, "/x", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	write([]byte("first"))
	if len(cas.data) != 1 {
		t.Fatalf("after first write want 1 chunk, got %d", len(cas.data))
	}

	write([]byte("second"))
	// Old chunk should be gone (refcount hit 0), only the new one remains.
	if len(cas.data) != 1 {
		t.Errorf("after overwrite want 1 chunk, got %d", len(cas.data))
	}
}

func TestOpenFileWriteRejectsExistingDir(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	if err := svc.Mkdir(ctx, "/d", 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := svc.OpenFile(ctx, "/d", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err == nil {
		t.Error("expected error writing to a directory")
	}
}

func TestOpenFileWriteParentMissing(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	_, err := svc.OpenFile(ctx, "/missing/file", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("want os.ErrNotExist, got %v", err)
	}
}

func TestOpenFileWriteRejectsAppend(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	_, err := svc.OpenFile(ctx, "/x", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err == nil {
		t.Error("expected error for O_APPEND open")
	}
}

func mustWrite(t *testing.T, svc *StorageServiceImpl, p string, body []byte) {
	t.Helper()
	f, err := svc.OpenFile(context.Background(), p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	if _, err := f.Write(body); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", p, err)
	}
}

func uniqueData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func TestRemoveAllFile(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	mustWrite(t, svc, "/x", []byte("hello"))
	if len(cas.data) != 1 {
		t.Fatalf("setup: want 1 chunk, got %d", len(cas.data))
	}

	if err := svc.RemoveAll(ctx, "/x"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Stat(ctx, "/x"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("post-remove stat: want ErrNotExist, got %v", err)
	}
	if len(cas.data) != 0 {
		t.Errorf("want chunk deleted, got %d remaining", len(cas.data))
	}
}

func TestRemoveAllDirRecursive(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	if err := svc.Mkdir(ctx, "/d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := svc.Mkdir(ctx, "/d/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, svc, "/d/a", []byte("aaa"))
	mustWrite(t, svc, "/d/sub/b", []byte("bbb"))

	if err := svc.RemoveAll(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/d", "/d/sub", "/d/a", "/d/sub/b"} {
		if _, err := svc.Stat(ctx, p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("after remove, %s: want ErrNotExist, got %v", p, err)
		}
	}
	if len(cas.data) != 0 {
		t.Errorf("want all chunks deleted, got %d", len(cas.data))
	}
}

func TestRemoveAllRootForbidden(t *testing.T) {
	svc, _ := newServiceWithGCAS(t)
	if err := svc.RemoveAll(context.Background(), "/"); err == nil {
		t.Error("expected error removing root")
	}
}

func TestRenameFile(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	mustWrite(t, svc, "/a", []byte("hello"))

	if err := svc.Rename(ctx, "/a", "/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Stat(ctx, "/a"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("/a still exists: %v", err)
	}
	r, err := svc.OpenFile(ctx, "/b", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "hello" {
		t.Errorf("read /b: %q err=%v", got, err)
	}
}

func TestRenameDirSubtree(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	if err := svc.Mkdir(ctx, "/d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := svc.Mkdir(ctx, "/d/sub", 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, svc, "/d/sub/x", []byte("xx"))

	if err := svc.Rename(ctx, "/d", "/e"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Stat(ctx, "/d"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("/d still exists")
	}
	for _, p := range []string{"/e", "/e/sub", "/e/sub/x"} {
		if _, err := svc.Stat(ctx, p); err != nil {
			t.Errorf("stat %s: %v", p, err)
		}
	}
}

func TestRenameDestExists(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	mustWrite(t, svc, "/a", []byte("a"))
	mustWrite(t, svc, "/b", []byte("b"))
	err := svc.Rename(ctx, "/a", "/b")
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("want ErrExist, got %v", err)
	}
}

func TestRenameIntoOwnDescendant(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)
	if err := svc.Mkdir(ctx, "/d", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := svc.Rename(ctx, "/d", "/d/sub"); err == nil {
		t.Error("expected error renaming into own descendant")
	}
}

func TestGarbageCollect(t *testing.T) {
	ctx := context.Background()
	svc, cas := newServiceWithGCAS(t)

	orphan := []byte("nobody refs me")
	h := sha256.Sum256(orphan)
	if err := cas.Put(ctx, h, orphan); err != nil {
		t.Fatal(err)
	}

	mustWrite(t, svc, "/x", []byte("kept"))

	if len(cas.data) != 2 {
		t.Fatalf("setup: want 2 chunks, got %d", len(cas.data))
	}

	if err := svc.GarbageCollect(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ok := cas.data[h]; ok {
		t.Errorf("orphan chunk should have been GCed")
	}
	if len(cas.data) != 1 {
		t.Errorf("want 1 chunk remaining, got %d", len(cas.data))
	}
}

// TestFileDirMethods tests the dirFile methods
func TestFileDirMethods(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a directory
	if err := svc.Mkdir(ctx, "/testdir", 0o755); err != nil {
		t.Fatal(err)
	}

	// Open the directory
	f, err := svc.OpenFile(ctx, "/testdir", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Test Stat()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if !fi.IsDir() {
		t.Error("Should be a directory")
	}

	// Test Read() on directory
	n, err := f.Read(make([]byte, 10))
	if n != 0 || err == nil {
		t.Errorf("Read on directory should fail: n=%d, err=%v", n, err)
	}

	// Test Write() on directory
	n, err = f.Write([]byte("test"))
	if n != 0 || err == nil {
		t.Errorf("Write on directory should fail: n=%d, err=%v", n, err)
	}

	// Test Seek() on directory
	pos, err := f.Seek(0, io.SeekStart)
	if pos != 0 || err == nil {
		t.Errorf("Seek on directory should fail: pos=%d, err=%v", pos, err)
	}
}

// TestReadFileMethods tests the readFile methods
func TestReadFileMethods(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a file
	mustWrite(t, svc, "/testfile.txt", []byte("test content"))

	// Open the file for reading
	f, err := svc.OpenFile(ctx, "/testfile.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Test Stat()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if fi.IsDir() {
		t.Error("Should not be a directory")
	}
	if fi.Name() != "testfile.txt" {
		t.Errorf("Name mismatch: got %s", fi.Name())
	}

	// Test Write() on read-only file
	n, err := f.Write([]byte("test"))
	if n != 0 || err == nil {
		t.Errorf("Write on read-only file should fail: n=%d, err=%v", n, err)
	}

	// Test Readdir() on file
	entries, err := f.Readdir(-1)
	if entries != nil || err == nil {
		t.Errorf("Readdir on file should fail: entries=%v, err=%v", entries, err)
	}

	// Test Seek() valid operations
	// Seek to start
	pos, err := f.Seek(0, io.SeekStart)
	if err != nil || pos != 0 {
		t.Errorf("Seek to start failed: pos=%d, err=%v", pos, err)
	}

	// Seek to current
	pos, err = f.Seek(5, io.SeekCurrent)
	if err != nil || pos != 5 {
		t.Errorf("Seek from current failed: pos=%d, err=%v", pos, err)
	}

	// Seek to end
	pos, err = f.Seek(0, io.SeekEnd)
	if err != nil || pos != fi.Size() {
		t.Errorf("Seek to end failed: pos=%d, size=%d, err=%v", pos, fi.Size(), err)
	}

	// Test Seek() invalid whence
	pos, err = f.Seek(0, 999)
	if err == nil {
		t.Errorf("Seek with invalid whence should fail: pos=%d", pos)
	}

	// Test Seek() negative position
	pos, err = f.Seek(-1, io.SeekStart)
	if err == nil {
		t.Errorf("Seek to negative position should fail: pos=%d", pos)
	}

	f.Close()
}

// TestMetaFileInfoMethods tests all metaFileInfo methods
func TestMetaFileInfoMethods(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a directory
	if err := svc.Mkdir(ctx, "/testinfo", 0o755); err != nil {
		t.Fatal(err)
	}

	// Get FileInfo for directory
	fi, err := svc.Stat(ctx, "/testinfo")
	if err != nil {
		t.Fatal(err)
	}

	// Test all methods
	if fi.Name() == "" {
		t.Error("Name should not be empty")
	}

	// For directories, Size() might return 0, but it should work
	_ = fi.Size()

	mode := fi.Mode()
	if !mode.IsDir() {
		t.Error("Mode should indicate directory")
	}

	modTime := fi.ModTime()
	// Just ensure it returns a time
	_ = modTime

	if !fi.IsDir() {
		t.Error("IsDir should be true for directory")
	}

	sys := fi.Sys()
	if sys != nil {
		t.Error("Sys() should return nil")
	}

	// Now test for a file
	mustWrite(t, svc, "/testfile.txt", []byte("test"))
	fi, err = svc.Stat(ctx, "/testfile.txt")
	if err != nil {
		t.Fatal(err)
	}

	if fi.Name() != "testfile.txt" {
		t.Errorf("File name mismatch: got %s", fi.Name())
	}

	size := fi.Size()
	if size != 4 {
		t.Errorf("File size mismatch: got %d", size)
	}

	mode = fi.Mode()
	if mode.IsDir() {
		t.Error("File mode should not indicate directory")
	}

	if fi.IsDir() {
		t.Error("File IsDir should be false")
	}
}

// TestStorageTunables tests the tunables methods
func TestStorageTunables(t *testing.T) {
	svc, _ := newServiceWithGCAS(t)

	// Test TunableSpecs
	specs := svc.TunableSpecs()
	if len(specs) == 0 {
		t.Error("TunableSpecs should return at least one spec")
	}

	// Test TunableValues
	values := svc.TunableValues()
	if len(values) == 0 {
		t.Error("TunableValues should return values")
	}

	chunkSizeMiB, ok := values[TunableChunkSizeMiB]
	if !ok {
		t.Fatal("TunableValues should contain chunk_size_mib")
	}

	// Convert to int64 for comparison - just check it exists, don't assign to unused variable
	_, ok = chunkSizeMiB.(int64)
	if !ok {
		t.Fatalf("chunk_size_mib should be int64, got %T", chunkSizeMiB)
	}

	// Test ApplyTunables with valid value
	err := svc.ApplyTunables(map[string]any{
		TunableChunkSizeMiB: 2.0,
	})
	if err != nil {
		t.Fatalf("ApplyTunables failed: %v", err)
	}

	// Verify chunk size changed
	if svc.GetChunkSize() != 2*1024*1024 {
		t.Errorf("Chunk size not updated: got %d", svc.GetChunkSize())
	}

	// Test ApplyTunables with empty map
	err = svc.ApplyTunables(map[string]any{})
	if err == nil {
		t.Error("ApplyTunables with empty map should fail")
	}

	// Test ApplyTunables with unknown key
	err = svc.ApplyTunables(map[string]any{
		"unknown_key": 1.0,
	})
	if err == nil {
		t.Error("ApplyTunables with unknown key should fail")
	}

	// Test ApplyTunables with invalid value (too small)
	err = svc.ApplyTunables(map[string]any{
		TunableChunkSizeMiB: 0.5,
	})
	if err == nil {
		t.Error("ApplyTunables with value below min should fail")
	}

	// Test ApplyTunables with invalid value (too large)
	err = svc.ApplyTunables(map[string]any{
		TunableChunkSizeMiB: 2000.0,
	})
	if err == nil {
		t.Error("ApplyTunables with value above max should fail")
	}
}

// TestReadFileSeekAndRead tests seek and read operations together
func TestReadFileSeekAndRead(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	content := []byte("Hello, World! This is a longer test file.")
	mustWrite(t, svc, "/seektest.txt", content)

	f, err := svc.OpenFile(ctx, "/seektest.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Test reading from middle
	pos, err := f.Seek(7, io.SeekStart)
	if err != nil || pos != 7 {
		t.Fatalf("Seek failed: pos=%d, err=%v", pos, err)
	}

	buf := make([]byte, 6)
	n, err := f.Read(buf)
	if err != nil || n != 6 {
		t.Fatalf("Read failed: n=%d, err=%v", n, err)
	}
	if string(buf) != "World!" {
		t.Errorf("Read wrong content: got %s", string(buf))
	}

	// Test seeking back and reading
	pos, err = f.Seek(0, io.SeekStart)
	if err != nil || pos != 0 {
		t.Fatalf("Seek to start failed: pos=%d, err=%v", pos, err)
	}

	buf = make([]byte, 5)
	n, err = f.Read(buf)
	if err != nil || n != 5 {
		t.Fatalf("Read from start failed: n=%d, err=%v", n, err)
	}
	if string(buf) != "Hello" {
		t.Errorf("Read wrong content from start: got %s", string(buf))
	}
}

// TestDirFileClosedOperations tests operations on closed files
func TestDirFileClosedOperations(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a directory
	if err := svc.Mkdir(ctx, "/closedtest", 0o755); err != nil {
		t.Fatal(err)
	}

	// Open and close the directory
	f, err := svc.OpenFile(ctx, "/closedtest", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Test Stat() on closed file
	_, err = f.Stat()
	if err == nil {
		t.Error("Stat on closed file should fail")
	}

	// Test Readdir() on closed file
	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("Readdir on closed file should fail")
	}
}

// TestReadFileClosedOperations tests operations on closed read files
func TestReadFileClosedOperations(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a file
	mustWrite(t, svc, "/closedfile.txt", []byte("test"))

	// Open and close the file
	f, err := svc.OpenFile(ctx, "/closedfile.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Test Stat() on closed file
	_, err = f.Stat()
	if err == nil {
		t.Error("Stat on closed read file should fail")
	}

	// Test Read() on closed file
	n, err := f.Read(make([]byte, 10))
	if n != 0 || err == nil {
		t.Errorf("Read on closed file should fail: n=%d, err=%v", n, err)
	}

	// Test Seek() on closed file
	_, err = f.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("Seek on closed file should fail")
	}
}

// TestListChildrenErrorHandling tests error cases in listChildren
func TestListChildrenErrorHandling(t *testing.T) {
	// This test requires a database connection that will fail
	// We'll use a closed database connection
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// listChildren should fail with closed DB
	_, err = listChildren(context.Background(), db, "/")
	if err == nil {
		t.Error("listChildren with closed DB should fail")
	}
}

// TestReadFileLocateError tests error cases in locate method
func TestReadFileLocateError(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create a test file
	content := []byte("test content")
	mustWrite(t, svc, "/locatetest.txt", content)

	// Open the file
	f, err := svc.OpenFile(ctx, "/locatetest.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Test Seek with out of bounds position (negative)
	_, err = f.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seek to negative position should fail")
	}

	// Test Seek with out of bounds position (beyond end)
	// This is actually allowed by Seek, so we'll read beyond EOF
	_, err = f.Seek(100, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek beyond end should not fail: %v", err)
	}

	// Reading should return EOF
	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Read beyond EOF: n=%d, err=%v", n, err)
	}
}

// TestWebdavProps tests WebDAV properties functionality
func TestWebdavProps(t *testing.T) {
	ctx := context.Background()

	// Create a mock GCAS that implements gcasDeviceProvider
	mockCAS := &mockDeviceProviderCAS{
		data:    make(map[gcas.Hash][]byte),
		devices: map[gcas.Hash][]gcas.DeviceDisplay{
			// We'll set up test data below
		},
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc, err := NewStorageService(db, mockCAS)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file
	content := []byte("test content for props")
	h := sha256.Sum256(content)

	// Add device info for this hash
	mockCAS.devices[h] = []gcas.DeviceDisplay{
		{DisplayName: "device1"},
		{DisplayName: "device2"},
	}

	if err := mockCAS.Put(ctx, h, content); err != nil {
		t.Fatal(err)
	}

	// Insert file metadata manually
	now := time.Now().UnixNano()
	if _, err := db.Exec(`INSERT INTO files VALUES('/props.txt','/',420,?,?,1)`,
		int64(len(content)), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO file_chunks VALUES('/props.txt',0,?,?)`,
		h[:], int64(len(content))); err != nil {
		t.Fatal(err)
	}

	// Open the file
	f, err := svc.OpenFile(ctx, "/props.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Cast to readFile interface to access DeadProps and Patch
	// Since readFile implements xwebdav.DeadPropsHolder internally
	// We need to check if the returned file has those methods
	if rf, ok := f.(interface {
		DeadProps() (map[xml.Name]xwebdav.Property, error)
		Patch([]xwebdav.Proppatch) ([]xwebdav.Propstat, error)
	}); ok {
		// Test DeadProps() - this should work with our mock
		props, err := rf.DeadProps()
		if err != nil {
			t.Fatalf("DeadProps failed: %v", err)
		}

		// The mock returns devices, so props should not be empty
		if len(props) == 0 {
			t.Error("DeadProps should return properties when devices are available")
		}

		// Test Patch() - this should return 403
		propstats, err := rf.Patch(nil)
		if err != nil {
			t.Fatalf("Patch failed: %v", err)
		}
		if len(propstats) == 0 {
			t.Error("Patch should return at least one Propstat")
		}
		if propstats[0].Status != 403 {
			t.Errorf("Patch should return status 403, got %d", propstats[0].Status)
		}
	} else {
		t.Skip("File doesn't implement DeadPropsHolder interface")
	}
}

// mockDeviceProviderCAS implements both gcas.GCAS and gcasDeviceProvider
type mockDeviceProviderCAS struct {
	data    map[gcas.Hash][]byte
	devices map[gcas.Hash][]gcas.DeviceDisplay
}

func (m *mockDeviceProviderCAS) Put(_ context.Context, h gcas.Hash, b []byte) error {
	if _, ok := m.data[h]; ok {
		return &gcas.HashExistsError{}
	}
	m.data[h] = append([]byte{}, b...)
	return nil
}

func (m *mockDeviceProviderCAS) Get(_ context.Context, h gcas.Hash) ([]byte, error) {
	b, ok := m.data[h]
	if !ok {
		return nil, &gcas.HashNotFoundError{}
	}
	return append([]byte{}, b...), nil
}

func (m *mockDeviceProviderCAS) Delete(_ context.Context, h gcas.Hash) error {
	if _, ok := m.data[h]; !ok {
		return &gcas.HashNotFoundError{}
	}
	delete(m.data, h)
	return nil
}

func (m *mockDeviceProviderCAS) List(ctx context.Context) (<-chan gcas.Hash, error) {
	ch := make(chan gcas.Hash, len(m.data))
	for h := range m.data {
		ch <- h
	}
	close(ch)
	return ch, nil
}

func (m *mockDeviceProviderCAS) FreeSpace(_ context.Context) (int64, error) { return 1 << 30, nil }

func (m *mockDeviceProviderCAS) AddNode(_ gcas.NamedCAS)                {}
func (m *mockDeviceProviderCAS) RemoveNode(_ string)                    {}
func (m *mockDeviceProviderCAS) ReplaceNode(_ gcas.NamedCAS)            {}
func (m *mockDeviceProviderCAS) RunMaintenance(_ context.Context) error { return nil }
func (m *mockDeviceProviderCAS) Repair(_ context.Context) error         { return nil }

func (m *mockDeviceProviderCAS) DevicesForHashes(ctx context.Context, hashes []gcas.Hash) ([]gcas.DeviceDisplay, error) {
	var devices []gcas.DeviceDisplay
	for _, h := range hashes {
		if devs, ok := m.devices[h]; ok {
			devices = append(devices, devs...)
		}
	}
	return devices, nil
}

// TestWebdavPropsNoDevices tests when no devices are available
func TestWebdavPropsNoDevices(t *testing.T) {
	ctx := context.Background()

	// Use a regular fakeCAS that doesn't implement gcasDeviceProvider
	cas := newFakeCAS()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc, err := NewStorageService(db, &fakeGCAS{cas})
	if err != nil {
		t.Fatal(err)
	}

	// Create a file
	content := []byte("test content")
	h := sha256.Sum256(content)

	if err := cas.Put(ctx, h, content); err != nil {
		t.Fatal(err)
	}

	// Insert file metadata
	now := time.Now().UnixNano()
	if _, err := db.Exec(`INSERT INTO files VALUES('/nodevices.txt','/',420,?,?,1)`,
		int64(len(content)), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO file_chunks VALUES('/nodevices.txt',0,?,?)`,
		h[:], int64(len(content))); err != nil {
		t.Fatal(err)
	}

	// Open the file
	f, err := svc.OpenFile(ctx, "/nodevices.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Cast to readFile interface to access DeadProps
	if rf, ok := f.(interface {
		DeadProps() (map[xml.Name]xwebdav.Property, error)
	}); ok {
		// Test DeadProps() - should return empty map without error
		props, err := rf.DeadProps()
		if err != nil {
			t.Fatalf("DeadProps failed: %v", err)
		}

		// Since fakeCAS doesn't implement gcasDeviceProvider, props should be empty
		if len(props) != 0 {
			t.Errorf("DeadProps should return empty map when no device provider, got %d props", len(props))
		}
	} else {
		t.Skip("File doesn't implement DeadPropsHolder interface")
	}
}

// TestFileWriteFlushError tests error handling in flushChunk
func TestFileWriteFlushError(t *testing.T) {
	ctx := context.Background()

	// Create a mock CAS that fails on Put
	failingCAS := &failingPutCAS{}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc, err := NewStorageService(db, failingCAS)
	if err != nil {
		t.Fatal(err)
	}

	// Try to write a file - should fail during flush
	f, err := svc.OpenFile(ctx, "/fail.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	_, err = f.Write([]byte("test data that should fail"))
	if err != nil {
		t.Fatalf("Write should succeed initially: %v", err)
	}

	// Close should fail due to flush error
	err = f.Close()
	if err == nil {
		t.Error("Close should fail when flush fails")
	}
}

type failingPutCAS struct{}

func (f *failingPutCAS) Put(_ context.Context, h gcas.Hash, b []byte) error {
	return fmt.Errorf("simulated Put failure")
}

func (f *failingPutCAS) Get(_ context.Context, h gcas.Hash) ([]byte, error) {
	return nil, &gcas.HashNotFoundError{}
}

func (f *failingPutCAS) Delete(_ context.Context, h gcas.Hash) error {
	return nil
}

func (f *failingPutCAS) List(ctx context.Context) (<-chan gcas.Hash, error) {
	ch := make(chan gcas.Hash)
	close(ch)
	return ch, nil
}

func (f *failingPutCAS) FreeSpace(_ context.Context) (int64, error) { return 1 << 30, nil }

func (f *failingPutCAS) AddNode(_ gcas.NamedCAS)                {}
func (f *failingPutCAS) RemoveNode(_ string)                    {}
func (f *failingPutCAS) ReplaceNode(_ gcas.NamedCAS)            {}
func (f *failingPutCAS) RunMaintenance(_ context.Context) error { return nil }
func (f *failingPutCAS) Repair(_ context.Context) error         { return nil }

// TestDBErrorHandling tests database error scenarios
func TestDBErrorHandling(t *testing.T) {
	// Test with nil database
	_, err := NewStorageService(nil, nil)
	if err == nil {
		t.Error("NewStorageService with nil DB should fail")
	}

	// Test with closed database - this might not fail immediately
	// because sql.DB doesn't check connection on creation
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// This might not fail immediately, but subsequent operations will
	svc, err := NewStorageService(db, nil)
	if err != nil {
		// If it does fail, that's okay
		return
	}

	// Try to use the service - should fail
	ctx := context.Background()
	_, err = svc.Stat(ctx, "/")
	if err == nil {
		t.Error("Stat with closed DB should fail")
	}
}

// TestChunkSizeValidation tests chunk size validation
func TestChunkSizeValidation(t *testing.T) {
	svc, _ := newServiceWithGCAS(t)

	// Test setting chunk size too small
	err := svc.SetChunkSize(minChunkSizeBytes - 1)
	if err == nil {
		t.Error("SetChunkSize below min should fail")
	}

	// Test setting chunk size too large
	err = svc.SetChunkSize(maxChunkSizeBytes + 1)
	if err == nil {
		t.Error("SetChunkSize above max should fail")
	}

	// Test valid chunk sizes
	err = svc.SetChunkSize(minChunkSizeBytes)
	if err != nil {
		t.Errorf("SetChunkSize at min should succeed: %v", err)
	}

	err = svc.SetChunkSize(maxChunkSizeBytes)
	if err != nil {
		t.Errorf("SetChunkSize at max should succeed: %v", err)
	}

	err = svc.SetChunkSize((minChunkSizeBytes + maxChunkSizeBytes) / 2)
	if err != nil {
		t.Errorf("SetChunkSize in valid range should succeed: %v", err)
	}
}

// TestReaddirWithCount tests Readdir with different count values
func TestReaddirWithCount(t *testing.T) {
	ctx := context.Background()
	svc, _ := newServiceWithGCAS(t)

	// Create multiple files in a directory
	if err := svc.Mkdir(ctx, "/readdirtest", 0o755); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		mustWrite(t, svc, fmt.Sprintf("/readdirtest/file%d.txt", i), []byte(fmt.Sprintf("content %d", i)))
	}

	// Open directory
	f, err := svc.OpenFile(ctx, "/readdirtest", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Test Readdir with count = 0 (should read all)
	entries, err := f.Readdir(0)
	if err != nil {
		t.Fatalf("Readdir(0) failed: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("Readdir(0) should return all entries, got %d", len(entries))
	}

	// Reset position by reopening
	f.Close()
	f, err = svc.OpenFile(ctx, "/readdirtest", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Test Readdir with count = 2
	entries, err = f.Readdir(2)
	if err != nil {
		t.Fatalf("Readdir(2) failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Readdir(2) should return 2 entries, got %d", len(entries))
	}

	// Read more entries
	entries, err = f.Readdir(2)
	if err != nil {
		t.Fatalf("Second Readdir(2) failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Second Readdir(2) should return 2 entries, got %d", len(entries))
	}

	// Read remaining entries
	entries, err = f.Readdir(2)
	if err != nil {
		t.Fatalf("Third Readdir(2) failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Third Readdir(2) should return 1 entry, got %d", len(entries))
	}

	// Read beyond EOF
	entries, err = f.Readdir(1)
	if err != io.EOF || len(entries) != 0 {
		t.Errorf("Readdir beyond EOF: err=%v, entries=%d", err, len(entries))
	}
}

// TestOpenDBError tests OpenDB error scenarios
func TestOpenDBError(t *testing.T) {
	// Test with invalid path
	_, err := OpenDB("/invalid/path/that/does/not/exist/test.db", 1)
	if err == nil {
		t.Error("OpenDB with invalid path should fail")
	}

	// Test with read-only directory
	readOnlyDir := filepath.Join(t.TempDir(), "readonly")
	if err := os.Mkdir(readOnlyDir, 0o444); err != nil {
		t.Fatal(err)
	}

	// Try to create DB in read-only directory
	dbPath := filepath.Join(readOnlyDir, "test.db")
	_, err = OpenDB(dbPath, 1)
	if err == nil {
		t.Error("OpenDB in read-only directory should fail")
	}
}
