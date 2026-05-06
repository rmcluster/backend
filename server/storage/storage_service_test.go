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

	"github.com/wk-y/rama-swap/server/gcas"
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

func (f *fakeGCAS) AddNode(_ gcas.NamedCAS)    {}
func (f *fakeGCAS) RemoveNode(_ gcas.NamedCAS) {}

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
