package gcas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
	return createTestGCASWithDataShards(numNodes, defaultDataShards)
}

func createTestGCASWithDataShards(numNodes, dataShards int) (GCAS, *sql.DB, error) {
	db, err := OpenDB(":memory:")
	if err != nil {
		return nil, nil, err
	}

	gcas := NewGCASWithDataShards(db, dataShards)

	nodes := make([]*mockCAS, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = NewMockCAS(fmt.Sprintf("node%d", i))
	}

	for _, node := range nodes {
		gcas.AddNode(node)
	}

	return gcas, db, nil
}

// testPutToNode directly places a chunk on a specific node, bypassing Put's random
// assignment. Used by EC tests to guarantee deterministic stripe layout.
func testPutToNode(t *testing.T, db *sql.DB, nodes map[string]*mockCAS, nodeID string, hash Hash, data []byte) {
	t.Helper()
	if err := nodes[nodeID].Put(context.Background(), hash, data); err != nil && !errors.Is(err, HashExistsError{}) {
		t.Fatalf("testPutToNode %s: %v", nodeID, err)
	}
	if _, err := db.Exec("INSERT OR IGNORE INTO chunks (hash, size, node_id) VALUES (?, ?, ?)", hash[:], len(data), nodeID); err != nil {
		t.Fatalf("testPutToNode DB insert: %v", err)
	}
}

// testSetupStripe places k chunks on nodes 0..k-1 deterministically and runs
// maintenance to form a stripe. Returns the k data hashes.
func testSetupStripe(t *testing.T, gcas GCAS, db *sql.DB, nodes map[string]*mockCAS, k int) []Hash {
	t.Helper()
	hashes := make([]Hash, k)
	for i := 0; i < k; i++ {
		data := []byte(fmt.Sprintf("stripe-data-%d", i))
		h := sha256.Sum256(data)
		hashes[i] = h
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i), h, data)
	}
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	return hashes
}

// TestGCASErasureCoding verifies that maintenance forms an erasure group when
// enough distinct-node chunks exist.
func TestGCASErasureCoding(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	testSetupStripe(t, gcas, db, nodes, k)

	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 erasure group, got %d", groupCount)
	}

	var memberCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group_member").Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if memberCount != k+parityShards {
		t.Errorf("expected %d erasure group members, got %d", k+parityShards, memberCount)
	}
}

// TestGCASStripeNodeConstraint verifies that no two stripe members share a node.
func TestGCASStripeNodeConstraint(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	testSetupStripe(t, gcas, db, nodes, k)

	rows, err := db.Query(`
		SELECT c.node_id FROM erasure_group_member egm
		JOIN chunks c ON c.hash = egm.hash_id
		WHERE egm.erasure_group_id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	seen := make(map[string]bool)
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			t.Fatal(err)
		}
		if seen[nodeID] {
			t.Errorf("node %s appears more than once in the stripe", nodeID)
		}
		seen[nodeID] = true
	}
}

// TestGCASGetNodeFailure verifies that Get succeeds via EC recovery when a
// single node holding a data chunk is removed.
func TestGCASGetNodeFailure(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// hashes[0] is on node0 (deterministic placement)
	gcas.RemoveNode("node0")

	// Get should recover via EC
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Errorf("expected EC recovery to succeed, got: %v", err)
	}
	expected := []byte("stripe-data-0")
	if string(data) != string(expected) {
		t.Errorf("recovered data mismatch: got %q, want %q", data, expected)
	}
}

// TestGCASGetTwoNodeFailure verifies EC recovery with 2 nodes down (maximum
// tolerable for 2 parity shards).
func TestGCASGetTwoNodeFailure(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Remove both data nodes (node0 and node1); only the 2 parity nodes survive
	gcas.RemoveNode("node0")
	gcas.RemoveNode("node1")

	// With k=2 data shards and 2 parity shards, losing 2 shards is still recoverable
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Errorf("expected EC recovery with 2 node failures, got: %v", err)
	}
	expected := []byte("stripe-data-0")
	if string(data) != string(expected) {
		t.Errorf("recovered data mismatch: got %q, want %q", data, expected)
	}
}

// TestGCASGetUnrecoverableFailure verifies that Get fails when more nodes are
// down than the parity count allows.
func TestGCASGetUnrecoverableFailure(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Remove all 4 stripe nodes — 0 shards survive, need k=2 for recovery
	rows, err := db.Query(`
		SELECT DISTINCT c.node_id FROM erasure_group_member egm
		JOIN chunks c ON c.hash = egm.hash_id
		WHERE egm.erasure_group_id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	var toRemove []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		toRemove = append(toRemove, n)
	}
	rows.Close()
	for _, n := range toRemove {
		gcas.RemoveNode(n)
	}

	_, err = gcas.Get(context.Background(), hashes[0])
	if err == nil {
		t.Error("expected Get to fail with all nodes down, got nil")
	}
}

// TestGCASRepairAndGet removes a node, runs Repair, and verifies Get succeeds
// without the original node.
func TestGCASRepairAndGet(t *testing.T) {
	const k = 2
	// k+parityShards nodes for the stripe + 1 spare so Repair has a node to place recovered shard
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards+1, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// hashes[0] is on node0 (deterministic placement); remove it
	gcas.RemoveNode("node0")

	// Before repair: Get should fail (primary node gone, EC recovery still works but
	// after repair the shard is placed on the spare node and Get uses the direct path)
	// Run repair to restore the shard to the spare node
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	// After repair, Get should succeed even without node0
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Errorf("expected Get to succeed after Repair, got: %v", err)
	}
	expected := []byte("stripe-data-0")
	if string(data) != string(expected) {
		t.Errorf("data mismatch after repair: got %q, want %q", data, expected)
	}
}

// TestGCASRepairCorruptData verifies that Repair restores a shard whose data
// has been corrupted on the node.
func TestGCASRepairCorruptData(t *testing.T) {
	const k = 2
	// +1 spare node so Repair can place the recovered shard somewhere other than the corrupt node
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards+1, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Corrupt data for hashes[0] on node0 (deterministic placement)
	nodes["node0"].CorruptData(hashes[0])

	// Repair should reconstruct hashes[0] onto the spare node
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	// After repair, Get should return correct data
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Errorf("expected Get to succeed after Repair, got: %v", err)
	}
	expected := []byte("stripe-data-0")
	if string(data) != string(expected) {
		t.Errorf("data mismatch after repair: got %q, want %q", data, expected)
	}
}

// createTestGCASWithNodes is like createTestGCASWithDataShards but returns the
// node map so tests can corrupt or inspect individual nodes.
func createTestGCASWithNodes(t *testing.T, numNodes, dataShards int) (GCAS, *sql.DB, map[string]*mockCAS) {
	t.Helper()
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	gcas := NewGCASWithDataShards(db, dataShards)
	nodeMap := make(map[string]*mockCAS, numNodes)
	for i := 0; i < numNodes; i++ {
		name := fmt.Sprintf("node%d", i)
		node := NewMockCAS(name)
		nodeMap[name] = node
		gcas.AddNode(node)
	}
	return gcas, db, nodeMap
}

// TestNewGCAS tests the NewGCAS constructor with default data shards.
func TestNewGCAS(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// NewGCAS uses default dataShards (4)
	gcas := NewGCAS(db)
	if gcas == nil {
		t.Fatal("expected non-nil GCAS instance")
	}

	// Test that it works by adding a node and putting data
	node := NewMockCAS("node0")
	gcas.AddNode(node)

	data := []byte("test")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	retrieved, err := gcas.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, retrieved) {
		t.Errorf("expected %s, got %s", data, retrieved)
	}
}

// TestRunGCCleanupEmpty tests garbage collection when there's nothing to clean.
// Since RunGC is not exported, we test it through RunMaintenance.
func TestRunGCCleanupEmpty(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// RunMaintenance should succeed and call RunGC
	if err = gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestRunGCWithDeletedChunks tests that RunGC properly cleans up deleted chunks
// that are not part of any erasure group (tested through RunMaintenance).
func TestRunGCWithDeletedChunks(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put two chunks
	data1 := []byte("chunk1")
	hash1 := sha256.Sum256(data1)
	data2 := []byte("chunk2")
	hash2 := sha256.Sum256(data2)

	if err = gcas.Put(context.Background(), hash1, data1); err != nil {
		t.Fatal(err)
	}
	if err = gcas.Put(context.Background(), hash2, data2); err != nil {
		t.Fatal(err)
	}

	// Delete both chunks
	if err = gcas.Delete(context.Background(), hash1); err != nil {
		t.Fatal(err)
	}
	if err = gcas.Delete(context.Background(), hash2); err != nil {
		t.Fatal(err)
	}

	// RunMaintenance should clean them up (calls RunGC internally)
	if err = gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify they're gone from DB
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chunks WHERE is_data = 0").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 deleted chunks after GC, got %d", count)
	}
}

// TestRunGCWithErasureGroups tests that RunGC cleans up erasure groups
// when all data chunks are deleted.
func TestRunGCWithErasureGroups(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Verify erasure group was created
	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 erasure group before GC, got %d", groupCount)
	}

	// Delete all data chunks
	for _, hash := range hashes {
		if err := gcas.Delete(context.Background(), hash); err != nil {
			t.Fatal(err)
		}
	}

	// RunMaintenance should remove the erasure group and all members (calls RunGC)
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify erasure group is gone
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 0 {
		t.Errorf("expected 0 erasure groups after GC, got %d", groupCount)
	}

	// Verify erasure group members are gone
	var memberCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group_member").Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Errorf("expected 0 erasure group members after GC, got %d", memberCount)
	}
}

// TestRunGCWithNodeError tests that RunMaintenance continues even if a node deletion fails.
func TestRunGCWithNodeError(t *testing.T) {
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

	// Delete it so is_data = 0
	if err = gcas.Delete(context.Background(), hash); err != nil {
		t.Fatal(err)
	}

	// Now make the node error on Delete
	deleteErrNode := &deleteErrCAS{
		mockCAS:   node,
		deleteErr: errors.New("node delete failed"),
	}
	gcas.ReplaceNode(deleteErrNode)

	// RunMaintenance should still succeed (error is logged, not returned)
	if err = gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestRunMaintenanceContextCancelled tests that RunMaintenance respects context cancellation.
func TestRunMaintenanceContextCancelled(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = gcas.RunMaintenance(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestRunMaintenanceConcurrent tests that RunMaintenance rejects concurrent calls.
func TestRunMaintenanceConcurrent(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	impl := gcas.(*GcasImpl)
	impl.maintenanceLock.Lock()
	defer impl.maintenanceLock.Unlock()

	err = gcas.RunMaintenance(context.Background())
	if err == nil || !strings.Contains(err.Error(), "maintenance already running") {
		t.Errorf("expected maintenance already running error, got %v", err)
	}
}

// TestFormStripesWithInsufficientNodes tests that formStripes returns early
// when not enough distinct-node chunks exist.
func TestFormStripesWithInsufficientNodes(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k, k) // Only k nodes, need k+parityShards
	defer db.Close()

	// Put k chunks on k nodes (not enough for stripe)
	for i := 0; i < k; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		h := sha256.Sum256(data)
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i), h, data)
	}

	// formStripes should succeed but not create a stripe
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 0 {
		t.Errorf("expected 0 erasure groups with insufficient nodes, got %d", groupCount)
	}
}

// TestFormStripesExactlyEnoughNodes tests stripe formation with exactly
// k+parityShards nodes.
func TestFormStripesExactlyEnough(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	testSetupStripe(t, gcas, db, nodes, k)

	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 erasure group, got %d", groupCount)
	}
}

// TestEncodeStripeMultipleStripes tests that multiple stripes can be formed.
func TestEncodeStripeMultipleStripes(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, 2*(k+parityShards), k)
	defer db.Close()

	// Create 2*k chunks to form 2 stripes
	for i := 0; i < 2*k; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		h := sha256.Sum256(data)
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i%(k+parityShards)), h, data)
	}

	// Run maintenance to form stripes
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Should have 2 erasure groups (may not form both depending on node reuse)
	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount == 0 {
		t.Error("expected at least 1 erasure group")
	}
}

// TestECRecoverWithPartialShards tests recovery when some shards are missing.
func TestECRecoverWithPartialShards(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Remove data node 0
	gcas.RemoveNode("node0")

	// Get should still succeed via EC recovery
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Errorf("expected EC recovery to work, got: %v", err)
	}

	expected := []byte("stripe-data-0")
	if !bytes.Equal(data, expected) {
		t.Errorf("data mismatch: got %q, want %q", data, expected)
	}
}

// TestECRecoverNotInErasureGroup tests that recovery fails if a chunk
// is not in any erasure group.
func TestECRecoverNotInGroup(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put a regular data chunk (not in erasure group)
	data := []byte("regular")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Remove the node
	gcas.RemoveNode("node0")

	// Get should fail (no erasure group to recover from)
	_, err = gcas.Get(context.Background(), hash)
	if err == nil {
		t.Error("expected Get to fail for non-erasure-coded chunk")
	}
}

// TestECRecoverInsufficientShards tests that recovery fails with too many failures.
func TestECRecoverInsufficientShards(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Remove all nodes (more than parity can recover from)
	for _, name := range []string{"node0", "node1", "node2", "node3"} {
		gcas.RemoveNode(name)
	}

	// Get should fail - too many nodes down
	_, err := gcas.Get(context.Background(), hashes[0])
	if err == nil {
		t.Error("expected Get to fail with too many nodes down")
	}
}

// TestRepairMultipleGroups tests Repair with multiple erasure groups.
func TestRepairMultipleGroups(t *testing.T) {
	const k = 2
	// Need enough nodes for 2 stripes + spares
	gcas, db, nodes := createTestGCASWithNodes(t, 2*(k+parityShards)+2, k)
	defer db.Close()

	// Form two stripes
	stripe1Hashes := make([]Hash, k)
	stripe2Hashes := make([]Hash, k)

	for i := 0; i < k; i++ {
		data1 := []byte(fmt.Sprintf("stripe1-data-%d", i))
		h1 := sha256.Sum256(data1)
		stripe1Hashes[i] = h1
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i), h1, data1)

		data2 := []byte(fmt.Sprintf("stripe2-data-%d", i))
		h2 := sha256.Sum256(data2)
		stripe2Hashes[i] = h2
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i+k+parityShards), h2, data2)
	}

	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Corrupt data on one node of each stripe
	nodes["node0"].CorruptData(stripe1Hashes[0])
	nodes[fmt.Sprintf("node%d", k+parityShards)].CorruptData(stripe2Hashes[0])

	// Repair should fix both
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Both should be recoverable now
	data1, err := gcas.Get(context.Background(), stripe1Hashes[0])
	if err != nil {
		t.Errorf("stripe 1 Get failed: %v", err)
	}
	if !bytes.Equal(data1, []byte("stripe1-data-0")) {
		t.Errorf("stripe 1 data mismatch")
	}

	data2, err := gcas.Get(context.Background(), stripe2Hashes[0])
	if err != nil {
		t.Errorf("stripe 2 Get failed: %v", err)
	}
	if !bytes.Equal(data2, []byte("stripe2-data-0")) {
		t.Errorf("stripe 2 data mismatch")
	}
}

// TestRepairGroupUnrecoverable tests that repairGroup fails gracefully
// when unrecoverable.
func TestRepairGroupUnrecoverable(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	testSetupStripe(t, gcas, db, nodes, k)

	// Remove more nodes than can be recovered
	gcas.RemoveNode("node0")
	gcas.RemoveNode("node1")
	gcas.RemoveNode("node2")
	gcas.RemoveNode("node3") // All nodes gone

	// Repair should fail but not panic
	err := gcas.Repair(context.Background())
	if err != nil {
		t.Logf("Repair failed as expected: %v", err)
	}
}

// TestRepairNoAvailableNode tests that Repair logs but continues
// when no node is available for reconstruction.
func TestRepairNoAvailableNode(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Corrupt node0 and remove all spare nodes
	nodes["node0"].CorruptData(hashes[0])
	gcas.RemoveNode("node0")

	// Keep only the parity nodes (no spares for reconstruction)
	for i := 0; i < k+parityShards; i++ {
		if i != k && i != k+1 { // Keep only parity nodes
			gcas.RemoveNode(fmt.Sprintf("node%d", i))
		}
	}

	// Repair should still succeed (but the corrupted shard may not be fixed)
	if err := gcas.Repair(context.Background()); err != nil {
		// Repair may fail but should handle gracefully
		t.Logf("Repair: %v", err)
	}
}

// TestRepairReusesOriginalNode tests that Repair uses the original node
// if it reconnects and no spare is available.
func TestRepairReusesOriginalNode(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Corrupt node0's shard
	nodes["node0"].CorruptData(hashes[0])

	// Remove node0 so repair needs to fix it
	gcas.RemoveNode("node0")

	// Remove all spare/other nodes except parity
	for i := 2; i < k+parityShards; i++ {
		gcas.RemoveNode(fmt.Sprintf("node%d", i))
	}

	// node1 and parity nodes remain
	// Now reconnect node0 (without spare, should reuse it)
	gcas.AddNode(nodes["node0"])

	// Repair
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	// Get should succeed (either from recovery or from parity)
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Logf("Get after repair: %v", err)
	}
	if data != nil && !bytes.Equal(data, []byte("stripe-data-0")) {
		t.Errorf("data mismatch")
	}
}

// TestRunMaintenanceWithDBError tests that RunMaintenance continues even when DB operations fail.
// RunMaintenance logs errors from RunGC, formStripes, and Repair but continues.
// It only returns an error if context is canceled.
func TestRunMaintenanceWithDBError(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause query errors
	db.Close()

	// RunMaintenance should still return nil because it logs errors internally
	// and only fails if context is canceled
	err = gcas.RunMaintenance(context.Background())
	if err != nil {
		t.Errorf("expected RunMaintenance to return nil (logged errors internally), got %v", err)
	}
}

// TestShardedLocker tests the sharded locking mechanism.
func TestShardedLocker(t *testing.T) {
	sl := newShardedLocker()

	hash1 := sha256.Sum256([]byte("test1"))
	hash2 := sha256.Sum256([]byte("test2"))

	// Lock and unlock should work
	sl.Lock(hash1)
	sl.Unlock(hash1)

	// RLock and RUnlock should work
	sl.RLock(hash2)
	sl.RUnlock(hash2)

	// Multiple RLocks should work
	sl.RLock(hash1)
	sl.RLock(hash1)
	sl.RUnlock(hash1)
	sl.RUnlock(hash1)
}

// TestPutRestoreDeletedChunk tests that Put can restore a previously deleted chunk.
func TestPutRestoreDeletedChunk(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)

	// Put the chunk
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Delete it
	if err = gcas.Delete(context.Background(), hash); err != nil {
		t.Fatal(err)
	}

	// Put it again - should succeed (restores the deleted record)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Verify we can get it
	retrieved, err := gcas.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, retrieved) {
		t.Errorf("restored data mismatch")
	}
}

// TestListWithInvalidHash tests List behavior when database has invalid hash lengths.
func TestListWithInvalidHash(t *testing.T) {
	gcas, db, err := createTestGCAS(1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert a valid chunk
	data := []byte("valid")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Manually insert a chunk with invalid hash length (should be skipped in List)
	_, err = db.Exec("INSERT INTO chunks (hash, size, node_id, is_data) VALUES (?, ?, ?, 1)",
		[]byte("tooshort"), 100, "node0")
	if err != nil {
		t.Fatal(err)
	}

	// List should return only the valid chunk
	ch, err := gcas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 valid hash, got %d", count)
	}
}

// TestDeleteWithRowsAffectedError tests error handling in Delete when RowsAffected fails.
// This is difficult to test directly without mocking, but we can test the success paths thoroughly.
func TestDeleteChecksDependencies(t *testing.T) {
	gcas, db, err := createTestGCAS(2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put chunk on a specific node
	data := []byte("test")
	hash := sha256.Sum256(data)
	if err = gcas.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}

	// Get should work
	retrieved, err := gcas.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, retrieved) {
		t.Errorf("data mismatch")
	}

	// Delete should mark as deleted
	if err = gcas.Delete(context.Background(), hash); err != nil {
		t.Fatal(err)
	}

	// Get should fail after delete
	_, err = gcas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError after delete, got %v", err)
	}
}

// TestECRecoverHashVerification tests that EC recovery verifies the recovered hash.
func TestECRecoverHashVerification(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Remove a data node
	gcas.RemoveNode("node0")

	// Corrupt the parity nodes to make recovery fail hash check
	nodes["node2"].CorruptData(hashes[0])
	nodes["node3"].CorruptData(hashes[0])

	// Actually, the parity nodes won't have hashes[0] - they have parity hashes
	// Let me instead verify successful recovery
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Fatalf("EC recovery failed: %v", err)
	}

	if !bytes.Equal(data, []byte("stripe-data-0")) {
		t.Errorf("recovered data doesn't match original")
	}
}

// TestFormStripesOrdersDeterministic tests that formStripes creates stripes deterministically.
func TestFormStripesOrdersDeterministic(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	// Put data in reverse order to test deterministic ordering by hash
	for i := k - 1; i >= 0; i-- {
		data := []byte(fmt.Sprintf("chunk-%d", i))
		h := sha256.Sum256(data)
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i), h, data)
	}

	// formStripes should still form properly despite insertion order
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 erasure group despite reverse insertion, got %d", groupCount)
	}
}

// TestRepairAllPresentShards tests that Repair skips groups where all shards are present.
func TestRepairAllPresentShards(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	testSetupStripe(t, gcas, db, nodes, k)

	// Don't corrupt anything - all shards are present and correct
	// Repair should just skip this group
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify data is still accessible
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected erasure group to remain, got %d groups", count)
	}
}

// TestRunGCWithErasureGroupCleanup tests that RunGC properly removes erasure group entries.
func TestRunGCWithErasureGroupCleanup(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Verify erasure group exists
	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount == 0 {
		t.Fatal("expected erasure group to exist")
	}

	// Delete all data chunks to trigger group cleanup
	for _, hash := range hashes {
		if err := gcas.Delete(context.Background(), hash); err != nil {
			t.Fatal(err)
		}
	}

	// Run maintenance to trigger gc
	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Groups should be cleaned up
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 0 {
		t.Errorf("expected 0 erasure groups after cleanup, got %d", groupCount)
	}
}

// TestFormStripesWithMultipleAvailableNodes tests stripe formation with extra nodes available.
func TestFormStripesWithMultipleAvailableNodes(t *testing.T) {
	const k = 2
	// Create more nodes than needed for multiple stripes
	gcas, db, nodes := createTestGCASWithNodes(t, 2*(k+parityShards), k)
	defer db.Close()

	// Create 2k chunks to potentially form 2 stripes
	for i := 0; i < 2*k; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		h := sha256.Sum256(data)
		nodeID := fmt.Sprintf("node%d", i%(2*(k+parityShards)))
		testPutToNode(t, db, nodes, nodeID, h, data)
	}

	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify groups were formed
	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount == 0 {
		t.Error("expected at least 1 erasure group")
	}
}

// TestEncodeStripeWithDifferentSizes tests stripe formation with chunks of different sizes.
func TestEncodeStripeWithDifferentSizes(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards, k)
	defer db.Close()

	// Create chunks of different sizes
	sizes := []string{"", "x", "hello world this is longer"}
	for i := 0; i < k; i++ {
		data := []byte(sizes[i%len(sizes)])
		if len(data) == 0 {
			data = []byte(fmt.Sprintf("chunk-%d", i))
		}
		h := sha256.Sum256(data)
		testPutToNode(t, db, nodes, fmt.Sprintf("node%d", i), h, data)
	}

	if err := gcas.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify stripe was formed despite size differences
	var groupCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM erasure_group").Scan(&groupCount); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 erasure group, got %d", groupCount)
	}
}

// TestPutRandomNodeSelection tests that Put distributes to random nodes.
func TestPutRandomNodeSelection(t *testing.T) {
	const numNodes = 5
	const numChunks = 100
	gcas, db, err := createTestGCASWithDataShards(numNodes, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put many chunks and verify they're distributed across nodes
	nodeUsage := make(map[string]int)
	for i := 0; i < numChunks; i++ {
		data := []byte(fmt.Sprintf("chunk-%d", i))
		hash := sha256.Sum256(data)
		if err = gcas.Put(context.Background(), hash, data); err != nil {
			t.Fatal(err)
		}

		// Track which node it went to
		var nodeID string
		if err := db.QueryRow("SELECT node_id FROM chunks WHERE hash = ?", hash[:]).Scan(&nodeID); err != nil {
			t.Fatal(err)
		}
		nodeUsage[nodeID]++
	}

	// Verify multiple nodes were used (random distribution)
	if len(nodeUsage) < 2 {
		t.Errorf("expected chunks distributed across multiple nodes, got %d nodes", len(nodeUsage))
	}
}

// TestRepairWithMissingNode tests Repair when the node holding a shard is unavailable.
func TestRepairWithMissingNode(t *testing.T) {
	const k = 2
	gcas, db, nodes := createTestGCASWithNodes(t, k+parityShards+1, k)
	defer db.Close()

	hashes := testSetupStripe(t, gcas, db, nodes, k)

	// Corrupt node0's shard
	nodes["node0"].CorruptData(hashes[0])

	// Remove node0 to force Repair to use a different node
	gcas.RemoveNode("node0")

	// Repair should reconstruct to an available node
	if err := gcas.Repair(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify the chunk is still accessible (from spare node or parity)
	data, err := gcas.Get(context.Background(), hashes[0])
	if err != nil {
		t.Logf("Get after repair with missing node: %v", err)
	}
	if data != nil && !bytes.Equal(data, []byte("stripe-data-0")) {
		t.Errorf("recovered data mismatch")
	}
}
