package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rmcluster/backend/server/gcas"
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
	if err := svc.SetChunkSize(4); err != nil {
		t.Fatalf("SetChunkSize: %v", err)
	}

	f, err := svc.OpenFile(ctx, "/split.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("abcdefghij")); err != nil {
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

	mustWrite(t, svc, "/before.txt", []byte("abcdefghij"))
	if len(cas.data) != 1 {
		t.Fatalf("before chunk-size change want 1 chunk, got %d", len(cas.data))
	}

	if err := svc.SetChunkSize(4); err != nil {
		t.Fatalf("SetChunkSize: %v", err)
	}
	mustWrite(t, svc, "/after.txt", []byte("abcdefghij"))
	if len(cas.data) != 4 {
		t.Fatalf("after chunk-size change want 4 total chunks, got %d", len(cas.data))
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
	if string(got) != "abcdefghij" {
		t.Fatalf("got %q, want %q", got, "abcdefghij")
	}
}

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
