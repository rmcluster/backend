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
	mu   sync.RWMutex
	data map[Hash][]byte
	name string
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

var _ NamedCAS = (*mockCAS)(nil)

// validateHash checks if the hash is the correct SHA256 hash of the data.
func validateHash(h Hash, data []byte) bool {
	return h == sha256.Sum256(data)
}
