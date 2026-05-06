package gcas

import (
	"path/filepath"
	"testing"
)

func TestOpenDB(t *testing.T) {
	// open a new in-memory db using OpenDB
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestDbReopen(t *testing.T) {
	// open DB at a temporary path
	tempdir := t.TempDir()
	tempDbPath := filepath.Join(tempdir, "test.db")

	db, err := OpenDB(tempDbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db, err = OpenDB(tempDbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}
