package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

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
	// SQL database to store metadata
	db *sql.DB
}

type metaFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (m metaFileInfo) Name() string {
	return m.name
}

func (m metaFileInfo) Size() int64 {
	return m.size
}

func (m metaFileInfo) Mode() fs.FileMode {
	return m.mode
}

func (m metaFileInfo) ModTime() time.Time {
	return m.modTime
}

func (m metaFileInfo) IsDir() bool {
	return m.isDir
}

func (m metaFileInfo) Sys() any {
	return nil
}

// GarbageCollect implements [StorageService].
func (s *StorageServiceImpl) GarbageCollect(ctx context.Context) error {
	panic("unimplemented")
}

// Mkdir implements [StorageService].
func (s *StorageServiceImpl) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	p, err := normalisePath(name)
	if err != nil {
		return err
	}
	if p == "/" {
		return os.ErrExist
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	exists, isDir, err := pathLookupTx(ctx, tx, p)
	if err != nil {
		return err
	}

	if exists {
		if isDir {
			return os.ErrExist
		}
		return fmt.Errorf("%w: file exists at %s", ErrAlreadyExists, p)
	}

	parent := parentPath(p)
	pExists, pIsDir, err := pathLookupTx(ctx, tx, parent)
	if err != nil {
		return err
	}
	if !pExists {
		return os.ErrNotExist
	}
	if !pIsDir {
		return fmt.Errorf("%w: parent %s", ErrNotDir, parent)
	}

	now := time.Now().UnixNano()
	if _, err = tx.ExecContext(ctx, `INSERT INTO directories(path, parent_path, mode, created_at_ns, updated_at_ns) VALUES (?, ?, ?, ?, ?)`,
		p, parent, int64(perm.Perm()), now, now,
	); err != nil {
		return fmt.Errorf("mkdir %s: %w", p, err)
	}

	return tx.Commit()
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
	p, err := normalisePath(name)
	if err != nil {
		return nil, err
	}

	var dMode, dUpdated int64
	err = s.db.QueryRowContext(ctx, `SELECT mode, updated_at_ns FROM directories WHERE path = ?`, p).Scan(&dMode, &dUpdated)
	if err == nil {
		return metaFileInfo{
			name:    baseName(p),
			mode:    fs.FileMode(dMode) | fs.ModeDir,
			modTime: time.Unix(0, dUpdated),
			isDir:   true,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("stat %s (dir): %w", p, err)
	}

	var fMode, fSize, fMod int64
	err = s.db.QueryRowContext(ctx, `SELECT mode, size_bytes, mod_time_ns FROM files WHERE path = ?`, p).Scan(&fMode, &fSize, &fMod)
	if err == nil {
		return metaFileInfo{
			name:    baseName(p),
			size:    fSize,
			mode:    fs.FileMode(fMode),
			modTime: time.Unix(0, fMod),
			isDir:   false,
		}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, os.ErrNotExist
	}

	return nil, fmt.Errorf("stat %s (file): %w", p, err)
}

// interface check
var _ StorageService = (*StorageServiceImpl)(nil)

func NewStorageService(db *sql.DB, gcas gcas.GCAS) (*StorageServiceImpl, error) {
	if db == nil {
		return nil, errors.New("storage: nil database")
	}
	return &StorageServiceImpl{
		db:   db,
		gcas: gcas,
	}, nil
}
