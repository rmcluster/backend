package gcas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestMockCASGetPut(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	// test data
	data := []byte("hello")

	// hash data
	hash := sha256.Sum256(data)

	// put data in CAS
	err := cas.Put(ctx, hash, data)
	if err != nil {
		t.Fatal(err)
	}

	// get data from CAS
	retrievedData, err := cas.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}

	// compare retrieved data with original data
	if !bytes.Equal(data, retrievedData) {
		t.Errorf("expected %s, got %s", data, retrievedData)
	}

	// test that CAS already has the data
	err = cas.Put(ctx, hash, data)
	if !errors.Is(err, HashExistsError{}) {
		t.Errorf("expected HashExistsError, got %v", err)
	}
}

// test deletion of CAS entry
func TestMockCASDelete(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	// test data
	data := []byte("hello")

	// hash test data
	hash := sha256.Sum256(data)

	// add data to CAS
	err := cas.Put(ctx, hash, data)
	if err != nil {
		t.Fatal(err)
	}

	// test that the CAS actually has the data
	retrievedData, err := cas.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	// compare retrieved data with original data
	if !bytes.Equal(data, retrievedData) {
		t.Errorf("expected %s, got %s", data, retrievedData)
	}

	// delete data from CAS
	err = cas.Delete(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}

	// test that data is deleted
	_, err = cas.Get(ctx, hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

func TestMockCASList(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	// list should not return anything for empty CAS
	{
		list, err := cas.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for range list {
			count++
		}
		if count != 0 {
			t.Errorf("expected 0, got %d", count)
		}
	}

	// create two test data entries
	var testData []string = []string{"hello", "world"}

	// add test data to the CAS
	for _, data := range testData {
		err := cas.Put(ctx, sha256.Sum256([]byte(data)), []byte(data))
		if err != nil {
			t.Fatal(err)
		}
	}

	// check that the CAS now has 2 entries
	{
		list, err := cas.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for range list {
			count++
		}
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}
	}
}
