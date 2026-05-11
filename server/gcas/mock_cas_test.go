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

// TestMockCASGetNotFound verifies that getting a non-existent hash returns HashNotFoundError.
func TestMockCASGetNotFound(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	hash := sha256.Sum256([]byte("nonexistent"))
	_, err := cas.Get(ctx, hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

// TestMockCASDeleteNotFound verifies that deleting a non-existent hash returns HashNotFoundError.
func TestMockCASDeleteNotFound(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	hash := sha256.Sum256([]byte("nonexistent"))
	err := cas.Delete(ctx, hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

// TestMockCASFreeSpace verifies the reported free space value.
func TestMockCASFreeSpace(t *testing.T) {
	cas := NewMockCAS("test")
	free, err := cas.FreeSpace(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if free != 1<<30 {
		t.Errorf("expected %d, got %d", int64(1<<30), free)
	}
}

// TestMockCASFreeSpaceError verifies that SetFreeSpaceError causes FreeSpace to return
// the configured error.
func TestMockCASFreeSpaceError(t *testing.T) {
	cas := NewMockCAS("test")
	sentinel := errors.New("disk failure")
	cas.SetFreeSpaceError(sentinel)

	_, err := cas.FreeSpace(context.Background())
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

// TestMockCASListError verifies that SetListError causes List to return the configured error.
func TestMockCASListError(t *testing.T) {
	cas := NewMockCAS("test")
	sentinel := errors.New("list failure")
	cas.SetListError(sentinel)

	_, err := cas.List(context.Background())
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

// TestMockCASPutInvalidHash verifies that putting data with a mismatched hash returns
// DataCorruptError.
func TestMockCASPutInvalidHash(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	data := []byte("hello")
	wrongHash := sha256.Sum256([]byte("not hello"))

	err := cas.Put(ctx, wrongHash, data)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError, got %v", err)
	}
}

// TestMockCASCorruptData verifies that CorruptData causes a subsequent Get to return
// DataCorruptError.
func TestMockCASCorruptData(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	data := []byte("hello")
	hash := sha256.Sum256(data)

	if err := cas.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}

	cas.CorruptData(hash)

	_, err := cas.Get(ctx, hash)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError after corruption, got %v", err)
	}
}

// TestMockCASDirectPut verifies that DirectPut makes data retrievable even when the hash
// does not match the content (bypasses validation).
func TestMockCASDirectPut(t *testing.T) {
	cas := NewMockCAS("test")
	ctx := context.Background()

	data := []byte("hello")
	wrongHash := sha256.Sum256([]byte("not hello"))

	// Direct put bypasses hash validation.
	cas.DirectPut(wrongHash, data)

	// Get will fail with DataCorruptError because the stored data does not match wrongHash.
	_, err := cas.Get(ctx, wrongHash)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError for mismatched direct-put, got %v", err)
	}
}

// TestMockCASName verifies that Name returns the name given at construction.
func TestMockCASName(t *testing.T) {
	cas := NewMockCAS("my-node")
	if cas.Name() != "my-node" {
		t.Errorf("expected %q, got %q", "my-node", cas.Name())
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
