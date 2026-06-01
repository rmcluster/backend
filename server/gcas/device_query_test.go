package gcas

import (
	"context"
	"crypto/sha256"
	"testing"
)

func TestDevicesForHashes(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g := NewGCAS(db).(*GcasImpl)

	hashNodeA := sha256.Sum256([]byte("node-a-data"))
	hashNodeB := sha256.Sum256([]byte("node-b-data"))

	_, err = db.Exec(
		"INSERT INTO chunks (hash, size, node_id, is_data) VALUES (?, ?, ?, 1), (?, ?, ?, 1)",
		hashNodeA[:], len("node-a-data"), "node-a",
		hashNodeB[:], len("node-b-data"), "node-b",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(
		"INSERT INTO device_metadata (node_id, display_name, updated_at_ns) VALUES (?, ?, ?)",
		"node-a", "Laptop", 1,
	)
	if err != nil {
		t.Fatal(err)
	}

	devices, err := g.DevicesForHashes(context.Background(), []Hash{hashNodeA, hashNodeB})
	if err != nil {
		t.Fatal(err)
	}

	if len(devices) != 2 {
		t.Fatalf("expected 2 devices, got %d", len(devices))
	}

	names := map[string]bool{}
	for _, device := range devices {
		names[device.DisplayName] = true
	}
	if !names["Laptop"] {
		t.Fatal("expected Laptop display name")
	}
	if !names["Unknown device"] {
		t.Fatal("expected Unknown device fallback display name")
	}

	empty, err := g.DevicesForHashes(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty result for no hashes, got %d", len(empty))
	}
}
