package gcas

import (
	"context"
	"crypto/sha256"
	"sync"
)

func NewMockCAS(name string) *mockCAS {
	return &mockCAS{
		data: make(map[Hash][]byte),
		name: name,
	}
}

type mockCAS struct {
	mu             sync.RWMutex
	data           map[Hash][]byte
	name           string
	freeSpaceError error
	listError      error
}

// Delete implements [CAS].
func (m *mockCAS) Delete(ctx context.Context, hash Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[hash]; !ok {
		return HashNotFoundError{}
	}
	delete(m.data, hash)
	return nil
}

// FreeSpace implements [CAS].
func (m *mockCAS) FreeSpace(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.freeSpaceError != nil {
		return 0, m.freeSpaceError
	}
	return 1 << 30, nil
}

// Get implements [CAS].
func (m *mockCAS) Get(ctx context.Context, hash Hash) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// retrieve data from CAS
	data, ok := m.data[hash]

	// if data is not found, return HashNotFoundError
	if !ok {
		return nil, HashNotFoundError{}
	}

	// if data is found, validate the hash
	if !validateHash(hash, data) {
		return nil, DataCorruptError{}
	}

	return data, nil
}

// List implements [CAS].
func (m *mockCAS) List(ctx context.Context) (<-chan Hash, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.listError != nil {
		return nil, m.listError
	}
	ch := make(chan Hash, len(m.data))
	for k := range m.data {
		ch <- k
	}
	close(ch)
	return ch, nil
}

// Put implements [CAS].
func (m *mockCAS) Put(ctx context.Context, hash Hash, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validateHash(hash, data) {
		return DataCorruptError{}
	}
	if _, ok := m.data[hash]; ok {
		return HashExistsError{}
	}
	m.data[hash] = data
	return nil
}

// Name implements [NamedCAS].
func (m *mockCAS) Name() string {
	return m.name
}

// DirectPut inserts data directly without hash validation or duplicate checks.
// Useful for seeding test state that bypasses normal invariants (e.g. testing List deduplication).
func (m *mockCAS) DirectPut(hash Hash, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[hash] = data
}

// CorruptData flips the first byte of stored data for hash, causing subsequent Get calls to return DataCorruptError.
func (m *mockCAS) CorruptData(hash Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.data[hash]; ok && len(d) > 0 {
		corrupted := make([]byte, len(d))
		copy(corrupted, d)
		corrupted[0] ^= 0xFF
		m.data[hash] = corrupted
	}
}

// SetFreeSpaceError configures FreeSpace to return the given error.
func (m *mockCAS) SetFreeSpaceError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.freeSpaceError = err
}

// SetListError configures List to return the given error.
func (m *mockCAS) SetListError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listError = err
}

var _ NamedCAS = (*mockCAS)(nil)

// validateHash checks if the hash is the correct SHA256 hash of the data.
func validateHash(h Hash, data []byte) bool {
	return h == sha256.Sum256(data)
}
