package storage

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/rmcluster/backend/server/gcas"
	xwebdav "golang.org/x/net/webdav"
)

type readFile struct {
	info   fs.FileInfo
	chunks []chunkRef
	gcas   gcas.GCAS
	ctx    context.Context

	pos int64

	curIdx  int
	curData []byte

	closed bool
}

var _ xwebdav.DeadPropsHolder = (*readFile)(nil)

type chunkRef struct {
	hash gcas.Hash
	size int64
}

func (f *readFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}
	return f.info, nil
}

func (f *readFile) Close() error {
	f.closed = true
	f.curData = nil
	return nil
}

func (f *readFile) Write(p []byte) (int, error) {
	return 0, errors.New("can't write to read only file")
}

func (f *readFile) Readdir(count int) ([]fs.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *readFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	if f.pos >= f.info.Size() {
		return 0, io.EOF
	}

	totalRead := 0
	for totalRead < len(p) && f.pos < f.info.Size() {
		idx, offsetInChunk, err := f.locate(f.pos)
		if err != nil {
			return totalRead, err
		}

		if f.curData == nil || f.curIdx != idx {
			data, err := f.gcas.Get(f.ctx, f.chunks[idx].hash)
			if err != nil {
				return totalRead, fmt.Errorf("fetch chunk %d: %w", idx, err)
			}

			if computed := sha256.Sum256(data); computed != f.chunks[idx].hash {
				return totalRead, fmt.Errorf("chunk %d hash mismatch", idx)
			}

			if int64(len(data)) != f.chunks[idx].size {
				return totalRead, fmt.Errorf("chunk %d size mismatch: expected %d, got %d", idx, f.chunks[idx].size, len(data))
			}

			f.curIdx = idx
			f.curData = data
		}

		n := copy(p[totalRead:], f.curData[offsetInChunk:])
		totalRead += n
		f.pos += int64(n)
	}

	return totalRead, nil
}

func (f *readFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	var abs int64

	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		abs = f.info.Size() + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if abs < 0 {
		return 0, errors.New("negative position")
	}

	f.pos = abs

	if f.curData != nil {
		idx, _, err := f.locate(abs)
		if err != nil || idx != f.curIdx {
			f.curData = nil
		}
	}

	return abs, nil
}

func (f *readFile) locate(pos int64) (int, int64, error) {
	if pos < 0 || pos > f.info.Size() {
		return 0, 0, fmt.Errorf("position %d out of range", pos)
	}

	var running int64

	for i, c := range f.chunks {
		if pos < running+c.size {
			return i, pos - running, nil
		}
		running += c.size
	}

	return len(f.chunks) - 1, f.chunks[len(f.chunks)-1].size, nil
}

func (f *readFile) DeadProps() (map[xml.Name]xwebdav.Property, error) {
	hashes := make([]gcas.Hash, 0, len(f.chunks))
	for _, chunk := range f.chunks {
		hashes = append(hashes, chunk.hash)
	}

	prop, ok, err := devicePropertyForHashes(f.ctx, f.gcas, hashes)
	if err != nil {
		return nil, err
	}
	if !ok {
		return map[xml.Name]xwebdav.Property{}, nil
	}
	return map[xml.Name]xwebdav.Property{
		prop.XMLName: prop,
	}, nil
}

func (f *readFile) Patch(_ []xwebdav.Proppatch) ([]xwebdav.Propstat, error) {
	return []xwebdav.Propstat{{
		Status: 403,
		Props: []xwebdav.Property{{
			XMLName: webdavDevicesPropName,
		}},
	}}, nil
}
