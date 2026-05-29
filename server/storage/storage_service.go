package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/rmcluster/backend/server/gcas"
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
	live, err := s.snapshotLiveHashes(ctx)

	if err != nil {
		return fmt.Errorf("snapshot live hashes: %w", err)
	}

	ch, err := s.gcas.List(ctx)
	if err != nil {
		return fmt.Errorf("list GCAS hashes: %w", err)
	}

	for h := range ch {
		if _, alive := live[h]; alive {
			continue
		}
		if err := s.gcas.Delete(ctx, h); err != nil {
			if errors.Is(err, gcas.HashNotFoundError{}) {
				continue
			}
			return fmt.Errorf("delete GCAS hash %x: %w", h, err)
		}
	}

	return nil
}

func (s *StorageServiceImpl) snapshotLiveHashes(ctx context.Context) (map[gcas.Hash]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chunk_hash FROM chunk_refs WHERE ref_count > 0`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[gcas.Hash]struct{}{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}

		if len(b) != len(gcas.Hash{}) {
			return nil, fmt.Errorf("chunk hash size mismatch, expected %d, got %d", len(gcas.Hash{}), len(b))
		}

		var h gcas.Hash
		copy(h[:], b)
		out[h] = struct{}{}
	}

	return out, rows.Err()
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
	const writeMask = os.O_WRONLY | os.O_RDWR | os.O_CREATE | os.O_TRUNC | os.O_EXCL
	const writeRequired = os.O_CREATE | os.O_TRUNC

	if flag&os.O_APPEND != 0 {
		return nil, fmt.Errorf("unsupported open flags: %#o", flag)
	}

	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 {
		if flag & ^writeMask != 0 || flag&writeRequired != writeRequired {
			return nil, fmt.Errorf("unsupported open flags %#o", flag)
		}
		if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
			return nil, fmt.Errorf("unsupported open flags %#o", flag)
		}
		return s.openWrite(ctx, name, perm, flag&os.O_EXCL != 0)
	}

	p, err := normalisePath(name)
	if err != nil {
		return nil, err
	}

	var dMode, dUpdated int64
	err = s.db.QueryRowContext(ctx, `SELECT mode, updated_at_ns FROM directories WHERE path = ?`, p).Scan(&dMode, &dUpdated)
	if err == nil {
		info := metaFileInfo{
			name:    baseName(p),
			mode:    fs.FileMode(dMode) | fs.ModeDir,
			modTime: time.Unix(0, dUpdated),
			isDir:   true,
		}

		return &dirFile{
			info: info,
			path: p,
			db:   s.db,
			ctx:  ctx,
		}, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("open %s (dir): %w", p, err)
	}

	var fMode, fSize, fMod int64
	err = s.db.QueryRowContext(ctx, `SELECT mode, size_bytes, mod_time_ns FROM files WHERE path = ?`, p).Scan(&fMode, &fSize, &fMod)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("open %s (file): %w", p, err)
	}

	chunks, err := s.loadChunks(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("open %s: load chunks: %w", p, err)
	}

	info := metaFileInfo{
		name:    baseName(p),
		size:    fSize,
		mode:    fs.FileMode(fMode),
		modTime: time.Unix(0, fMod),
		isDir:   false,
	}

	return &readFile{
		info:   info,
		chunks: chunks,
		gcas:   s.gcas,
		ctx:    ctx,
	}, nil
}

func (s *StorageServiceImpl) openWrite(ctx context.Context, name string, perm os.FileMode, exclusive bool) (webdav.File, error) {
	p, err := normalisePath(name)
	if err != nil {
		return nil, err
	}

	if p == "/" {
		return nil, fmt.Errorf("%w: cannot write to root", ErrIsDir)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	exists, isDir, err := pathLookupTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}

	if exists && isDir {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, p)
	}

	if exists && exclusive {
		return nil, os.ErrExist
	}

	parent := parentPath(p)
	pExists, pIsDir, err := pathLookupTx(ctx, tx, parent)
	if err != nil {
		return nil, err
	}

	if !pExists {
		return nil, os.ErrNotExist
	}

	if !pIsDir {
		return nil, fmt.Errorf("%w: parent %s", ErrNotDir, parent)
	}

	mode := perm.Perm()
	if mode == 0 {
		mode = 0o666
	}

	return newWriteFile(ctx, p, mode, s.gcas, s.commitWrite), nil
}

func (s *StorageServiceImpl) commitWrite(ctx context.Context, p string, mode fs.FileMode, chunks []chunkRef, totalSize int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	exists, isDir, err := pathLookupTx(ctx, tx, p)
	if err != nil {
		return err
	}

	if exists && isDir {
		return fmt.Errorf("%w: %s", ErrIsDir, p)
	}

	parent := parentPath(p)
	pExists, pIsDir, err := pathLookupTx(ctx, tx, parent)
	if err != nil {
		return err
	}

	if !pExists || !pIsDir {
		return fmt.Errorf("%w: parent %s", ErrNotDir, parent)
	}

	oldHashes, err := selectChunkHashesTx(ctx, tx, p)
	if err != nil {
		return err
	}

	var oldVersion int64
	if exists {
		if err := tx.QueryRowContext(ctx, `SELECT version FROM files WHERE path = ?`, p).Scan(&oldVersion); err != nil {
			return fmt.Errorf("read version: %w", err)
		}
	}

	now := time.Now().UnixNano()

	if _, err := tx.ExecContext(ctx, `
	    INSERT INTO files(path, parent_path, mode, size_bytes, mod_time_ns, version)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			mode = excluded.mode,
			size_bytes = excluded.size_bytes,
			mod_time_ns = excluded.mod_time_ns,
			version = excluded.version`,
		p, parent, int64(mode.Perm()), totalSize, now, oldVersion+1,
	); err != nil {
		return fmt.Errorf("upsert files: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM file_chunks WHERE file_path = ?`, p); err != nil {
		return fmt.Errorf("delete old chunks: %w", err)
	}

	for i, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO file_chunks(file_path, chunk_index, chunk_hash, chunk_size) VALUES (?, ?, ?, ?)`,
			p, i, c.hash[:], c.size,
		); err != nil {
			return fmt.Errorf("insert chunk %d: %w", i, err)
		}
	}

	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, `INSERT INTO chunk_refs(chunk_hash, ref_count) VALUES (?, 1)
			ON CONFLICT(chunk_hash) DO UPDATE SET ref_count = ref_count + 1`, c.hash[:],
		); err != nil {
			return fmt.Errorf("incref chunk %w", err)
		}
	}

	for _, h := range oldHashes {
		if _, err := tx.ExecContext(ctx, `UPDATE chunk_refs SET ref_count = ref_count - 1 WHERE chunk_hash = ?`, h[:]); err != nil {
			return fmt.Errorf("decref chunk %w", err)
		}
	}

	var orphans []gcas.Hash
	for _, h := range oldHashes {
		var rc int64
		if err := tx.QueryRowContext(ctx, `SELECT ref_count FROM chunk_refs WHERE chunk_hash = ?`, h[:]).Scan(&rc); err != nil {
			return fmt.Errorf("read ref_count: %w", err)
		}
		if rc <= 0 {
			orphans = append(orphans, h)
			if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_refs WHERE chunk_hash = ?`, h[:]); err != nil {
				return fmt.Errorf("delete refcount row: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	for _, h := range orphans {
		_ = s.gcas.Delete(ctx, h)
	}

	return nil
}

func selectChunkHashesTx(ctx context.Context, tx *sql.Tx, filePath string) ([]gcas.Hash, error) {
	rows, err := tx.QueryContext(ctx, `SELECT chunk_hash FROM file_chunks WHERE file_path = ? ORDER BY chunk_index ASC`, filePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []gcas.Hash
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}

		if len(b) != len(gcas.Hash{}) {
			return nil, fmt.Errorf("chunk hash size mismatched, expected %d, got %d", len(gcas.Hash{}), len(b))
		}

		var hash gcas.Hash
		copy(hash[:], b)
		out = append(out, hash)
	}

	return out, rows.Err()
}

func (s *StorageServiceImpl) loadChunks(ctx context.Context, filePath string) ([]chunkRef, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chunk_hash, chunk_size FROM file_chunks WHERE file_path = ? ORDER BY chunk_index ASC`, filePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []chunkRef
	for rows.Next() {
		var hashBytes []byte
		var size int64
		if err := rows.Scan(&hashBytes, &size); err != nil {
			return nil, err
		}

		if len(hashBytes) != len(gcas.Hash{}) {
			return nil, fmt.Errorf("chunk hash size mismatched, expected %d, got %d", len(gcas.Hash{}), len(hashBytes))
		}

		var hash gcas.Hash
		copy(hash[:], hashBytes)
		out = append(out, chunkRef{
			hash: hash,
			size: size,
		})
	}

	return out, rows.Err()
}

// RemoveAll implements [StorageService].
func (s *StorageServiceImpl) RemoveAll(ctx context.Context, name string) error {
	p, err := normalisePath(name)

	if err != nil {
		return err
	}

	if p == "/" {
		return fmt.Errorf("%w: cannot remove root", ErrInvalidPath)
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

	if !exists {
		return os.ErrNotExist
	}

	hashes, err := collectSubtreeChunkHashesTx(ctx, tx, p)
	if err != nil {
		return err
	}

	if isDir {
		lo, hi := subtreeBounds(p)

		if _, err := tx.ExecContext(ctx, `DELETE FROM file_chunks WHERE file_path >= ? AND file_path < ?`, lo, hi); err != nil {
			return fmt.Errorf("delete file chunks: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE path >= ? AND path < ?`, lo, hi); err != nil {
			return fmt.Errorf("delete subtree files: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM directories WHERE path = ? OR (path >= ? AND path < ?)`, p, lo, hi); err != nil {
			return fmt.Errorf("delete subtree directories: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_chunks WHERE file_path = ?`, p); err != nil {
			return fmt.Errorf("delete chunks: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM files WHERE path = ?`, p); err != nil {
			return fmt.Errorf("delete file: %w", err)
		}
	}

	orphans, err := decRefAndCollectOrphansTx(ctx, tx, hashes)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	for _, h := range orphans {
		_ = s.gcas.Delete(ctx, h)
	}

	return nil
}

// Rename implements [StorageService].
func (s *StorageServiceImpl) Rename(ctx context.Context, oldName string, newName string) error {
	src, err := normalisePath(oldName)
	if err != nil {
		return err
	}

	dst, err := normalisePath(newName)
	if err != nil {
		return err
	}

	if src == "/" || dst == "/" {
		return fmt.Errorf("%w: cannot rename root", ErrInvalidPath)
	}

	if src == dst {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	srcExists, srcIsDir, err := pathLookupTx(ctx, tx, src)
	if err != nil {
		return err
	}
	if !srcExists {
		return os.ErrNotExist
	}

	dstExists, _, err := pathLookupTx(ctx, tx, dst)
	if err != nil {
		return err
	}
	if dstExists {
		return os.ErrExist
	}

	dstParent := parentPath(dst)
	dpExists, dpIsDir, err := pathLookupTx(ctx, tx, dstParent)
	if err != nil {
		return err
	}
	if !dpExists {
		return os.ErrNotExist
	}
	if !dpIsDir {
		return fmt.Errorf("%w: parent %s", ErrNotDir, dstParent)
	}

	if srcIsDir {
		lo, hi := subtreeBounds(src)
		if dst == src || (dst >= lo && dst < hi) {
			return fmt.Errorf("%w: cannot rename %s into its own descendant %s", ErrInvalidPath, src, dst)
		}
		if err := renameSubtreeTx(ctx, tx, src, dst); err != nil {
			return err
		}
	} else {
		if err := renameFileTx(ctx, tx, src, dst); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func renameFileTx(ctx context.Context, tx *sql.Tx, src, dst string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE files SET path = ?, parent_path = ? WHERE path = ?`, dst, parentPath(dst), src); err != nil {
		return fmt.Errorf("update file row: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE file_chunks SET file_path = ? WHERE file_path = ?`, dst, src); err != nil {
		return fmt.Errorf("update file_chunks rows: %w", err)
	}

	return nil
}

func renameSubtreeTx(ctx context.Context, tx *sql.Tx, src, dst string) error {
	lo, hi := subtreeBounds(src)

	if _, err := tx.ExecContext(ctx, `UPDATE directories SET path = ?, parent_path = ? WHERE path = ?`, dst, parentPath(dst), src); err != nil {
		return fmt.Errorf("update src directory row: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE directories SET path = ? || substr(path, length(?)+1), parent_path = ? || substr(parent_path, length(?)+1)
		WHERE path >= ? AND path < ?`, dst, src, dst, src, lo, hi); err != nil {
		return fmt.Errorf("update descendant directories: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE files SET path = ? || substr(path, length(?)+1), parent_path = ? || substr(parent_path, length(?)+1)
		WHERE path >= ? AND path < ?`, dst, src, dst, src, lo, hi); err != nil {
		return fmt.Errorf("update descendant files: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE file_chunks SET file_path = ? || substr(file_path, length(?)+1) WHERE file_path >= ? AND file_path < ?`,
		dst, src, lo, hi); err != nil {
		return fmt.Errorf("update descendant file_chunks: %w", err)
	}

	return nil
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
