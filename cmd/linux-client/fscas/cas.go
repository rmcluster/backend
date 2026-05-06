package fscas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/wk-y/rama-swap/server/gcas"
	"golang.org/x/sys/unix"
)

// CAS implements gcas.CAS
// chunks are stored on the filesystem as follows:
// storagePath/XX/SHA256
// where XX is the first two characters of the hash
type CAS struct {
	storagePath string
}

func (c *CAS) pathForHash(hash gcas.Hash) string {
	hashHex := hex.EncodeToString(hash[:])
	return filepath.Join(c.storagePath, hashHex[:2], hashHex)
}

// Delete implements [gcas.CAS].
func (c *CAS) Delete(ctx context.Context, hash gcas.Hash) error {
	p := c.pathForHash(hash)
	err := os.Remove(p)
	if err != nil {
		if os.IsNotExist(err) {
			return &gcas.HashNotFoundError{}
		}
		return err
	}
	return nil
}

// FreeSpace implements [gcas.CAS].
func (c *CAS) FreeSpace(ctx context.Context) (int64, error) {
	if err := os.MkdirAll(c.storagePath, 0755); err != nil {
		return 0, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(c.storagePath, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// Get implements [gcas.CAS].
func (c *CAS) Get(ctx context.Context, hash gcas.Hash) ([]byte, error) {
	p := c.pathForHash(hash)
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, gcas.HashNotFoundError{}
		}
		return nil, err
	}

	// verify the checksum
	sum := sha256.Sum256(data)
	if sum != hash {
		return nil, gcas.DataCorruptError{}
	}

	return data, nil
}

// List implements [gcas.CAS].
func (c *CAS) List(ctx context.Context) (<-chan gcas.Hash, error) {
	ch := make(chan gcas.Hash)
	go func() {
		defer close(ch)
		entries, err := os.ReadDir(c.storagePath)
		if err != nil {
			return
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			subEntries, err := os.ReadDir(filepath.Join(c.storagePath, entry.Name()))
			if err != nil {
				continue
			}
			for _, subEntry := range subEntries {
				if subEntry.IsDir() {
					continue
				}
				hashBytes, err := hex.DecodeString(subEntry.Name())
				if err != nil || len(hashBytes) != sha256.Size {
					continue
				}
				var hash gcas.Hash
				copy(hash[:], hashBytes)
				select {
				case ch <- hash:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

// Put implements [gcas.CAS].
func (c *CAS) Put(ctx context.Context, hash gcas.Hash, data []byte) error {
	p := c.pathForHash(hash)
	if _, err := os.Stat(p); err == nil {
		return &gcas.HashExistsError{}
	}

	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer os.Remove(tmpName)

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Link(tmpName, p); err != nil {
		if os.IsExist(err) {
			return &gcas.HashExistsError{}
		}
		return err
	}
	return nil
}

func NewCAS(storagePath string) *CAS {
	return &CAS{
		storagePath: storagePath,
	}
}

var _ gcas.CAS = &CAS{}
