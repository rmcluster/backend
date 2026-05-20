package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"
)

type dirFile struct {
	info fs.FileInfo
	path string
	db   *sql.DB
	ctx  context.Context

	listed []fs.FileInfo
	pos    int

	closed bool
}

func (d *dirFile) Stat() (fs.FileInfo, error) {
	if d.closed {
		return nil, fs.ErrClosed
	}

	return d.info, nil
}

func (d *dirFile) Close() error {
	d.closed = true
	d.listed = nil
	return nil
}

func (d *dirFile) Read(p []byte) (int, error) {
	return 0, errors.New("can't read from directory")
}

func (d *dirFile) Write(p []byte) (int, error) {
	return 0, errors.New("can't write to directory")
}

func (d *dirFile) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("can't seek in directory")
}

func (d *dirFile) Readdir(count int) ([]fs.FileInfo, error) {
	if d.closed {
		return nil, fs.ErrClosed
	}

	if d.listed == nil {
		entries, err := listChildren(d.ctx, d.db, d.path)

		if err != nil {
			return nil, fmt.Errorf("readdir %s: %w", d.path, err)
		}

		d.listed = entries
	}

	remaining := len(d.listed) - d.pos

	if remaining == 0 {
		if count <= 0 {
			return nil, nil
		}

		return nil, io.EOF
	}

	n := remaining
	if count > 0 && count < n {
		n = count
	}

	out := d.listed[d.pos : d.pos+n]
	d.pos += n

	return out, nil
}

func listChildren(ctx context.Context, db *sql.DB, dir string) ([]fs.FileInfo, error) {
	var out []fs.FileInfo

	rows, err := db.QueryContext(ctx, `SELECT path, mode, updated_at_ns FROM directories WHERE parent_path = ? AND path != ?`, dir, dir)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var p string
		var mode, updated int64

		if err := rows.Scan(&p, &mode, &updated); err != nil {
			rows.Close()
			return nil, err
		}

		out = append(out, metaFileInfo{
			name:    baseName(p),
			mode:    fs.FileMode(mode) | fs.ModeDir,
			modTime: timeFromNs(updated),
			isDir:   true,
		})
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}

	rows.Close()

	rows, err = db.QueryContext(ctx, `SELECT path, mode, size_bytes, mod_time_ns FROM files WHERE parent_path = ?`, dir)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var p string
		var mode, size, modT int64

		if err := rows.Scan(&p, &mode, &size, &modT); err != nil {
			return nil, err
		}

		out = append(out, metaFileInfo{
			name:    baseName(p),
			size:    size,
			mode:    fs.FileMode(mode),
			modTime: timeFromNs(modT),
			isDir:   false,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func timeFromNs(ns int64) (t time.Time) {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
