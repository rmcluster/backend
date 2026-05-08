// The GCAS package provides content-addressable storage systems.

package gcas

import (
	"context"
	"crypto/sha256"
)

type Hash [sha256.Size]byte

// CAS is a content-addressable storage system.
// Put, get, and delete should be thread-safe and atomic.
type CAS interface {
	// Put stores the given data in the CAS with the given hash.
	// Calling put on a hash that already exists will result in a HashExistsError.
	// Implementations may return other errors for failure modes.
	Put(ctx context.Context, hash Hash, data []byte) error
	// Get returns the chunk with the given hash.
	// Calling get on a non-existent hash will result in a HashNotFoundError.
	// Implementations may return other errors for failure modes.
	Get(ctx context.Context, hash Hash) ([]byte, error)
	// Delete deletes the chunk with the given hash.
	// Calling delete on a non-existent hash will result in a HashNotFoundError.
	// Implementations may return other errors for failure modes.
	Delete(ctx context.Context, hash Hash) error
	// List all hashes in the CAS. Implemented on a best-effort basis with no guarantees on ordering or completeness.
	List(ctx context.Context) (<-chan Hash, error)
	// Free space on this CAS (in bytes). Implemented on a best-effort basis.
	FreeSpace(ctx context.Context) (int64, error)
}

type HashNotFoundError struct{}

func (e HashNotFoundError) Error() string {
	return "Chunk not found"
}

type HashExistsError struct{}

func (e HashExistsError) Error() string {
	return "Chunk already exists"
}

type DataCorruptError struct{}

func (e DataCorruptError) Error() string {
	return "Data is corrupted"
}
