package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/wk-y/rama-swap/server/gcas"
)

const DefaultChunkSize int64 = 8 * 1024 * 1024

type commitFn func(ctx context.Context, path string, mode fs.FileMode, chunks []chunkRef, totalSize int64) error

type writeFile struct {
	path      string
	mode      fs.FileMode
	gcas      gcas.GCAS
	commit    commitFn
	ctx       context.Context
	chunkSize int64

	buf       []byte
	chunks    []chunkRef
	totalSize int64

	closed    bool
	committed bool
}

func newWriteFile(ctx context.Context, path string, mode fs.FileMode, g gcas.GCAS, commit commitFn, chunkSize int64) *writeFile {
	return &writeFile{
		path:      path,
		mode:      mode,
		gcas:      g,
		commit:    commit,
		ctx:       ctx,
		chunkSize: chunkSize,
		buf:       make([]byte, 0, chunkSize),
	}
}

func (f *writeFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}

	return metaFileInfo{
		name:    baseName(f.path),
		size:    f.totalSize + int64(len(f.buf)),
		mode:    f.mode,
		modTime: time.Now(),
		isDir:   false,
	}, nil
}

func (f *writeFile) Read(p []byte) (int, error) {
	return 0, errors.New("can't read write only file")
}

func (f *writeFile) Seek(int64, int) (int64, error) {
	return 0, errors.New("can't Seek in write only file")
}

func (f *writeFile) Readdir(int) ([]fs.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *writeFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	written := 0
	for written < len(p) {
		space := int(f.chunkSize) - len(f.buf)
		take := len(p) - written
		if take > space {
			take = space
		}

		f.buf = append(f.buf, p[written:written+take]...)
		written += take

		if int64(len(f.buf)) == f.chunkSize {
			if err := f.flushChunk(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

func (f *writeFile) flushChunk() error {
	if len(f.buf) == 0 {
		return nil
	}

	hash := sha256.Sum256(f.buf)
	size := int64(len(f.buf))

	if err := f.gcas.Put(f.ctx, hash, f.buf); err != nil {
		if !errors.Is(err, gcas.HashExistsError{}) {
			return fmt.Errorf("gcas put chunk %d: %w", len(f.chunks), err)
		}
	}

	f.chunks = append(f.chunks, chunkRef{
		hash: hash,
		size: size,
	})

	f.totalSize += size

	f.buf = f.buf[:0]
	return nil
}

func (f *writeFile) Close() error {
	if f.closed {
		return fs.ErrClosed
	}

	f.closed = true

	if err := f.flushChunk(); err != nil {
		return err
	}

	if err := f.commit(f.ctx, f.path, f.mode, f.chunks, f.totalSize); err != nil {
		return fmt.Errorf("commit file: %w", err)
	}

	f.committed = true
	return nil
}
