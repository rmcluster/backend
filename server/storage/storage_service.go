package storage

import (
	"context"
	"os"

	"github.com/wk-y/rama-swap/server/gcas"
	"golang.org/x/net/webdav"
)

type StorageService interface {
	webdav.FileSystem
	// GarbageCollect requests that the storage service delete unused data chunks.
	// The actual behavior of GarbageCollect is implementation defined.
	GarbageCollect(ctx context.Context) error
}

type StorageServiceImpl struct {
	// GCAS to store data chunks
	gcas gcas.GCAS
}

// GarbageCollect implements [StorageService].
func (s *StorageServiceImpl) GarbageCollect(ctx context.Context) error {
	panic("unimplemented")
}

// Mkdir implements [StorageService].
func (s *StorageServiceImpl) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	panic("unimplemented")
}

// OpenFile implements [StorageService].
func (s *StorageServiceImpl) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	panic("unimplemented")
}

// RemoveAll implements [StorageService].
func (s *StorageServiceImpl) RemoveAll(ctx context.Context, name string) error {
	panic("unimplemented")
}

// Rename implements [StorageService].
func (s *StorageServiceImpl) Rename(ctx context.Context, oldName string, newName string) error {
	panic("unimplemented")
}

// Stat implements [StorageService].
func (s *StorageServiceImpl) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	panic("unimplemented")
}

// interface check
var _ StorageService = (*StorageServiceImpl)(nil)

func NewStorageService(gcas gcas.GCAS) StorageService {
	return &StorageServiceImpl{
		gcas: gcas,
	}
}
