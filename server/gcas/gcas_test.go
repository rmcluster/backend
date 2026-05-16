package gcas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

// test putting one chunk into gcas
func TestGCASPutGet(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// test data
	data := []byte("hello")
	dataHash := sha256.Sum256(data)

	// put data in CAS
	err = gcas.Put(context.Background(), dataHash, data)
	if err != nil {
		t.Fatal(err)
	}

	// get data from CAS
	retrievedData, err := gcas.Get(context.Background(), dataHash)
	if err != nil {
		t.Fatal(err)
	}

	// compare retrieved data with original data
	if !bytes.Equal(data, retrievedData) {
		t.Errorf("expected %s, got %s", data, retrievedData)
	}
}

// test double-put behavior
// the first put should succeed, whereas the second put should throw HashExistsError
func TestGCASDoublePut(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// test data
	data := []byte("hello")
	dataHash := sha256.Sum256(data)

	// put data in CAS
	err = gcas.Put(context.Background(), dataHash, data)
	if err != nil {
		t.Fatal(err)
	}

	// test that the CAS actually has the data
	retrievedData, err := gcas.Get(context.Background(), dataHash)
	if err != nil {
		t.Fatal(err)
	}
	// compare retrieved data with original data
	if !bytes.Equal(data, retrievedData) {
		t.Errorf("expected %s, got %s", data, retrievedData)
	}

	// test that the CAS already has the data
	err = gcas.Put(context.Background(), dataHash, data)
	if !errors.Is(err, HashExistsError{}) {
		t.Errorf("expected HashExistsError, got %v", err)
	}
}

func TestGCASNoNodes(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// try to put when there are no nodes
	// it should error
	data := []byte("hello")
	dataHash := sha256.Sum256(data)
	err = gcas.Put(context.Background(), dataHash, data)
	if !errors.Is(err, ErrNoNodes{}) {
		t.Errorf("expected ErrNoNodes, got %v", err)
	}
}

func TestGCASFreeSpaceWithoutNodes(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Without nodes there is no free space
	freeSpace, err := gcas.FreeSpace(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if freeSpace != 0 {
		t.Errorf("expected 0 free space, got %d", freeSpace)
	}
}

// TestGCASGetNotFound verifies that getting a non-existent hash returns HashNotFoundError.
func TestGCASGetNotFound(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	hash := sha256.Sum256([]byte("nonexistent"))
	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

// TestGCASDeleteNotFound verifies that deleting a non-existent hash returns HashNotFoundError.
func TestGCASDeleteNotFound(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	hash := sha256.Sum256([]byte("nonexistent"))
	err = gcas.Delete(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

// TestGCASDelete verifies the full delete lifecycle: put, delete, then get returns
// HashNotFoundError, and a second delete also returns HashNotFoundError.
func TestGCASDelete(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)

	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	if err = gcas.Delete(context.Background(), hash); err != nil {
		t.Fatalf("expected delete to succeed, got %v", err)
	}

	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError after delete, got %v", err)
	}

	err = gcas.Delete(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError on second delete, got %v", err)
	}
}

// TestGCASGetDisconnectedNode verifies that getting a chunk whose node has been removed
// returns an error (the chunk is in the DB but the node is not connected).
func TestGCASGetDisconnectedNode(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)

	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	gcas.RemoveNode("node0")

	_, err = gcas.Get(context.Background(), hash)
	if err == nil {
		t.Error("expected error for disconnected node, got nil")
	}
}

// TestGCASDeleteDisconnectedNode verifies that deleting a chunk whose node has been
// removed still succeeds: the DB record is removed even though the node is not connected.
// The actual data on the node becomes orphaned until the node reconnects.
func TestGCASDeleteDisconnectedNode(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)

	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	gcas.RemoveNode("node0")

	// Delete should succeed: it removes the DB record even without the node connected.
	if err = gcas.Delete(context.Background(), hash); err != nil {
		t.Errorf("expected delete to succeed for disconnected node, got %v", err)
	}

	// A subsequent Get must return HashNotFoundError (record removed from DB).
	gcas.AddNode(NewMockCAS("node0"))
	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError after disconnected-node delete, got %v", err)
	}
}

// TestGCASFreeSpaceWithNodes verifies that FreeSpace sums free space across all connected nodes.
func TestGCASFreeSpaceWithNodes(t *testing.T) {
	const numNodes = 3
	gcas, db, err := createTestGCAS(numNodes)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	freeSpace, err := gcas.FreeSpace(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	expected := int64(numNodes) * (1 << 30)
	if freeSpace != expected {
		t.Errorf("expected %d, got %d", expected, freeSpace)
	}
}

// TestGCASFreeSpaceNodeError verifies that when one node's FreeSpace fails, GCAS returns
// the partial sum from the working nodes along with the error.
func TestGCASFreeSpaceNodeError(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	good := NewMockCAS("good")
	bad := NewMockCAS("bad")
	bad.SetFreeSpaceError(errors.New("node unavailable"))

	gcas.AddNode(good)
	gcas.AddNode(bad)

	freeSpace, err := gcas.FreeSpace(context.Background())
	if err == nil {
		t.Error("expected error from failing node, got nil")
	}
	if freeSpace != 1<<30 {
		t.Errorf("expected partial free space %d from working node, got %d", int64(1<<30), freeSpace)
	}
}

// TestGCASListEmpty verifies that listing with no nodes returns an empty channel.
func TestGCASListEmpty(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ch, err := gcas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 hashes from empty GCAS, got %d", count)
	}
}

// TestGCASList verifies that all stored chunks appear in List results.
func TestGCASList(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	entries := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	want := make(map[Hash]struct{}, len(entries))
	for _, d := range entries {
		h := sha256.Sum256(d)
		want[h] = struct{}{}
		if err = gcas.Put(context.Background(), h, d); err != nil {
			t.Fatal(err)
		}
	}

	ch, err := gcas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[Hash]struct{})
	for h := range ch {
		got[h] = struct{}{}
	}

	for h := range want {
		if _, ok := got[h]; !ok {
			t.Errorf("hash missing from List output")
		}
	}
	if len(got) != len(want) {
		t.Errorf("expected %d hashes, got %d", len(want), len(got))
	}
}

// TestGCASInternalList verifies that GCAS uses its own database to look up hashes,
// and does not rely on the accuracy of the nodes' lists
func TestGCASInternalList(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node0 := NewMockCAS("node0")
	node1 := NewMockCAS("node1")

	data := []byte("shared")
	hash := sha256.Sum256(data)

	// directly insert hash into nodes, should not be listed
	if err := node0.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}
	if err := node1.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	gcas.AddNode(node0)
	gcas.AddNode(node1)

	ch, err := gcas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected empty list, got %d elements", count)
	}

	// now put hash through gcas and check that it's listed
	if err := gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	ch, err = gcas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 hash, got %d", count)
	}
}

// TestGCASListNodeError documents that when a node's List call returns an error, GCAS
// silently drops the error: the returned channel closes with no hashes from that node
// or any nodes that would have been iterated after it.  The caller has no way to detect
// that an error occurred (GCAS.List always returns a nil error).
func TestGCASListNodeError(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node := NewMockCAS("node0")
	data := []byte("hello")
	hash := sha256.Sum256(data)
	// Seed data directly so it exists in the node without going through GCAS.
	node.DirectPut(hash, data)
	node.SetListError(errors.New("list unavailable"))
	gcas.AddNode(node)

	_, listErr := gcas.List(context.Background())
	// GCAS.List always returns nil even when a node fails.
	if listErr != nil {
		t.Fatalf("unexpected non-nil error from GCAS.List: %v", listErr)
	}
}

// TestGCASListContextCancel verifies that a context cancellation while reading from
// List stops the output channel promptly.
func TestGCASListContextCancel(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put enough chunks so there is something to iterate over.
	for i := 0; i < 5; i++ {
		d := []byte(fmt.Sprintf("chunk-%d", i))
		h := sha256.Sum256(d)
		if err = gcas.Put(context.Background(), h, d); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := gcas.List(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Read one hash then cancel.
	<-ch
	cancel()

	// Drain the channel; it must close within a reasonable time after cancellation.
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("channel did not close after context cancellation")
	}
}

// TestGCASAddRemoveNode verifies that after removing the only node, Put returns ErrNoNodes.
func TestGCASAddRemoveNode(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node := NewMockCAS("node0")
	gcas.AddNode(node)

	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	gcas.RemoveNode("node0")

	data2 := []byte("world")
	hash2 := sha256.Sum256(data2)
	err = gcas.Put(context.Background(), hash2, data2)
	if !errors.Is(err, ErrNoNodes{}) {
		t.Errorf("expected ErrNoNodes after removing all nodes, got %v", err)
	}
}

// TestGCASReplaceNode verifies that after replacing a node with a fresh empty node of the
// same name, data that was stored on the old node is no longer accessible: Get returns
// HashNotFoundError because the new node has no data.
func TestGCASReplaceNode(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node0 := NewMockCAS("node0")
	gcas.AddNode(node0)

	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Replace with a fresh empty node bearing the same name.
	gcas.ReplaceNode(NewMockCAS("node0"))

	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError after replacing node, got %v", err)
	}
}

// TestGCASPutInvalidHash verifies that putting data with a mismatched hash returns
// DataCorruptError.
func TestGCASPutInvalidHash(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	wrongHash := sha256.Sum256([]byte("not hello"))

	err = gcas.Put(context.Background(), wrongHash, data)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError for hash mismatch, got %v", err)
	}
}

// TestGCASGetCorruptNode verifies that when the underlying node reports data corruption,
// GCAS.Get propagates a DataCorruptError to the caller.
func TestGCASGetCorruptNode(t *testing.T) {
	gcas, db, err := createTestGCAS(0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node := NewMockCAS("node0")
	gcas.AddNode(node)

	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Corrupt the stored bytes directly on the node.
	node.CorruptData(hash)

	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError from corrupt node, got %v", err)
	}
}

// deleteErrCAS wraps mockCAS but returns a fixed error from Delete,
// allowing tests to exercise the error-propagation branch in GcasImpl.Delete.
type deleteErrCAS struct {
	*mockCAS
	deleteErr error
}

func (d *deleteErrCAS) Delete(_ context.Context, _ Hash) error {
	return d.deleteErr
}

// TestGCASDeleteExecError verifies that a DB failure on the UPDATE statement is propagated.
// It uses a SQLite BEFORE UPDATE trigger to make the ExecContext call fail after the
// initial SELECT succeeds.
func TestGCASDeleteExecError(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TRIGGER prevent_delete BEFORE UPDATE ON chunks BEGIN SELECT RAISE(ABORT, 'delete prevented'); END`)
	if err != nil {
		t.Fatal(err)
	}

	if err = gcas.Delete(context.Background(), hash); err == nil {
		t.Error("expected DB error from ExecContext, got nil")
	}
}

// TestGCASGetDBError verifies that a DB failure during Get is propagated to the caller.
func TestGCASGetDBError(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	hash := sha256.Sum256([]byte("hello"))
	_, err = gcas.Get(context.Background(), hash)
	if err == nil || errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected a DB error, got %v", err)
	}
}

// TestGCASDeleteDBError verifies that a DB failure during Delete is propagated to the caller.
func TestGCASDeleteDBError(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	hash := sha256.Sum256([]byte("hello"))
	err = gcas.Delete(context.Background(), hash)
	if err == nil || errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected a DB error, got %v", err)
	}
}

// TestGCASPutDBError verifies that a DB failure when checking for duplicates in Put
// is propagated to the caller.
func TestGCASPutDBError(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)
	err = gcas.Put(context.Background(), hash, data)
	if err == nil {
		t.Error("expected a DB error, got nil")
	}
}

func TestGCASRunMaintenance(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// put first chunk
	// this chunk will be deleted later
	data1 := []byte("hello")
	hash1 := sha256.Sum256(data1)
	if err = gcas.Put(context.Background(), hash1, data1); err != nil {
		t.Fatal(err)
	}

	// put second chunk
	data2 := []byte("world")
	hash2 := sha256.Sum256(data2)
	if err = gcas.Put(context.Background(), hash2, data2); err != nil {
		t.Fatal(err)
	}

	// delete first chunk
	if err = gcas.Delete(context.Background(), hash1); err != nil {
		t.Fatal(err)
	}

	// run maintenance. it will garbage collect the first chunk
	if err = gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// try to get the first chunk. it should fail
	_, err = gcas.Get(context.Background(), hash1)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError after GC, got %v", err)
	}

	// get the second chunk. it should not fail
	dataRetreived, err := gcas.Get(context.Background(), hash2)
	if err != nil {
		t.Errorf("expected success after GC, got %v", err)
	}

	if !bytes.Equal(dataRetreived, data2) {
		t.Errorf("expected data %v after GC, got %v", data2, dataRetreived)
	}
}

func createTestGCAS(numNodes int) (GCAS, *sql.DB, error) {
	db, err := OpenDB(":memory:")
	gcas := NewGCAS(db)

	if err != nil {
		return nil, nil, err
	}

	nodes := make([]NamedCAS, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = NewMockCAS(fmt.Sprintf("node%d", i))
	}

	for _, node := range nodes {
		gcas.AddNode(node)
	}

	return gcas, db, nil
}
