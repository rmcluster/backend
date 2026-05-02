package gcas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
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
