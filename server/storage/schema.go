package storage

import (
	"context"
	"database/sql"
)

func pathLookupTx(ctx context.Context, tx *sql.Tx, p string) (bool, bool, error) {
	var one int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM directories WHERE path = ?`, p).Scan(&one)
	if err == nil {
		return true, true, nil
	}
	if err != sql.ErrNoRows {
		return false, false, err
	}

	err = tx.QueryRowContext(ctx,
		`SELECT 1 FROM files WHERE path = ?`, p).Scan(&one)
	if err == nil {
		return true, false, nil
	}
	if err != sql.ErrNoRows {
		return false, false, err
	}
	return false, false, nil
}
