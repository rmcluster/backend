package gcas

import "testing"

func TestOpenDB(t *testing.T) {
	// open a new in-memory db using OpenDB
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}
