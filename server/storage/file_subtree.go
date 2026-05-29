package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rmcluster/backend/server/gcas"
)

func subtreeBounds(dir string) (lo, hi string) {
	if dir == "/" {
		return "/", "0"
	}
	return dir + "/", dir + "0"
}

func collectSubtreeChunkHashesTx(ctx context.Context, tx *sql.Tx, dir string) ([]gcas.Hash, error) {
	lo, hi := subtreeBounds(dir)

	rows, err := tx.QueryContext(ctx, `SELECT chunk_hash FROM file_chunks WHERE file_path = ? OR file_path >= ? AND file_path < ?`, dir, lo, hi)

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
			return nil, fmt.Errorf("chunk hash size mismatch, expected %d, got %d", len(gcas.Hash{}), len(b))
		}

		var h gcas.Hash
		copy(h[:], b)
		out = append(out, h)
	}

	return out, rows.Err()
}

func decRefAndCollectOrphansTx(ctx context.Context, tx *sql.Tx, hashes []gcas.Hash) ([]gcas.Hash, error) {
	var orphans []gcas.Hash
	for _, h := range hashes {
		if _, err := tx.ExecContext(ctx, `UPDATE chunk_refs SET ref_count = ref_count - 1 WHERE chunk_hash = ?`, h[:]); err != nil {
			return nil, fmt.Errorf("decref: %w", err)
		}

		var rc int64
		if err := tx.QueryRowContext(ctx, `SELECT ref_count FROM chunk_refs WHERE chunk_hash = ?`, h[:]).Scan(&rc); err != nil {
			return nil, fmt.Errorf("read refcount: %w", err)
		}

		if rc <= 0 {
			orphans = append(orphans, h)

			if _, err := tx.ExecContext(ctx, `DELETE FROM chunk_refs WHERE chunk_hash = ?`, h[:]); err != nil {
				return nil, fmt.Errorf("delete refcount row: %w", err)
			}
		}
	}

	return orphans, nil
}
