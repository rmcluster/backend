package gcas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"testing"
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
