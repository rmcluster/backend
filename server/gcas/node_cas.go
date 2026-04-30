package gcas

import (
	"context"
)

func newRemoteCAS(ip string, port int) CAS {
	return &remoteCAS{
		ip:   ip,
		port: port,
	}
}

type remoteCAS struct {
	ip   string
	port int
}

// Delete implements [CAS].
func (n *remoteCAS) Delete(ctx context.Context, hash Hash) error {
	return nil
}

// FreeSpace implements [CAS].
func (n *remoteCAS) FreeSpace(ctx context.Context) (int64, error) {
	return 0, nil
}

// Get implements [CAS].
func (n *remoteCAS) Get(ctx context.Context, hash Hash) ([]byte, error) {
	panic("unimplemented")
}

// List implements [CAS].
func (n *remoteCAS) List(ctx context.Context) (<-chan Hash, error) {
	panic("unimplemented")
}

// Put implements [CAS].
func (n *remoteCAS) Put(ctx context.Context, hash Hash, data []byte) error {
	panic("unimplemented")
}

var _ CAS = (*remoteCAS)(nil)
