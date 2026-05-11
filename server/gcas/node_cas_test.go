package gcas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// newCASFromServer creates a remoteCAS pointing at the given httptest server.
func newCASFromServer(t *testing.T, server *httptest.Server) *remoteCAS {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return &remoteCAS{
		name:   "test-node",
		ip:     u.Hostname(),
		port:   port,
		client: server.Client(),
	}
}

func TestRemoteCASName(t *testing.T) {
	cas := NewRemoteCAS("mynode", "127.0.0.1", 9999)
	if cas.Name() != "mynode" {
		t.Errorf("expected %q, got %q", "mynode", cas.Name())
	}
}

func TestRemoteCASDeleteOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("hello"))
	if err := cas.Delete(context.Background(), hash); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestRemoteCASDeleteNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("hello"))
	err := cas.Delete(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

func TestRemoteCASDeleteServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("hello"))
	if err := cas.Delete(context.Background(), hash); err == nil {
		t.Error("expected error for 500 response, got nil")
	}
}

func TestRemoteCASFreeSpaceOK(t *testing.T) {
	const expected int64 = 1024 * 1024
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"available_space": expected})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	free, err := cas.FreeSpace(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if free != expected {
		t.Errorf("expected %d, got %d", expected, free)
	}
}

func TestRemoteCASFreeSpaceServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	if _, err := cas.FreeSpace(context.Background()); err == nil {
		t.Error("expected error for non-200 status, got nil")
	}
}

func TestRemoteCASFreeSpaceBadJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	if _, err := cas.FreeSpace(context.Background()); err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestRemoteCASGetOK(t *testing.T) {
	data := []byte("hello")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256(data)
	got, err := cas.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("expected %q, got %q", data, got)
	}
}

func TestRemoteCASGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not_found"})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("missing"))
	_, err := cas.Get(context.Background(), hash)
	if !errors.Is(err, HashNotFoundError{}) {
		t.Errorf("expected HashNotFoundError, got %v", err)
	}
}

func TestRemoteCASGetCorrupted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "corrupted_chunk"})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("corrupt"))
	_, err := cas.Get(context.Background(), hash)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError, got %v", err)
	}
}

func TestRemoteCASGetServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	hash := sha256.Sum256([]byte("hello"))
	if _, err := cas.Get(context.Background(), hash); err == nil {
		t.Error("expected error for 500 status, got nil")
	}
}

func TestRemoteCASListOK(t *testing.T) {
	data := []byte("hello")
	hash := sha256.Sum256(data)
	hashHex := hex.EncodeToString(hash[:])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{hashHex})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	ch, err := cas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 hash, got %d", count)
	}
}

func TestRemoteCASListEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	ch, err := cas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 hashes, got %d", count)
	}
}

func TestRemoteCASListServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	if _, err := cas.List(context.Background()); err == nil {
		t.Error("expected error for 500 status, got nil")
	}
}

func TestRemoteCASListBadJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	if _, err := cas.List(context.Background()); err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestRemoteCASPutOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify body was sent
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err := cas.Put(context.Background(), hash, data); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestRemoteCASPutChecksumError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "checksum_incorrect"})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	data := []byte("hello")
	hash := sha256.Sum256(data)
	err := cas.Put(context.Background(), hash, data)
	if !errors.Is(err, DataCorruptError{}) {
		t.Errorf("expected DataCorruptError, got %v", err)
	}
}

func TestRemoteCASPutServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "internal"})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err := cas.Put(context.Background(), hash, data); err == nil {
		t.Error("expected error for 500 status, got nil")
	}
}

// Tests for client.Do errors (server closed before request).

func TestRemoteCASDeleteConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	cas := newCASFromServer(t, server)
	server.Close()

	hash := sha256.Sum256([]byte("hello"))
	if err := cas.Delete(context.Background(), hash); err == nil {
		t.Error("expected error when server is closed, got nil")
	}
}

func TestRemoteCASFreeSpaceConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	cas := newCASFromServer(t, server)
	server.Close()

	if _, err := cas.FreeSpace(context.Background()); err == nil {
		t.Error("expected error when server is closed, got nil")
	}
}

func TestRemoteCASGetConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	cas := newCASFromServer(t, server)
	server.Close()

	hash := sha256.Sum256([]byte("hello"))
	if _, err := cas.Get(context.Background(), hash); err == nil {
		t.Error("expected error when server is closed, got nil")
	}
}

func TestRemoteCASListConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	cas := newCASFromServer(t, server)
	server.Close()

	if _, err := cas.List(context.Background()); err == nil {
		t.Error("expected error when server is closed, got nil")
	}
}

func TestRemoteCASPutConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	cas := newCASFromServer(t, server)
	server.Close()

	data := []byte("hello")
	hash := sha256.Sum256(data)
	if err := cas.Put(context.Background(), hash, data); err == nil {
		t.Error("expected error when server is closed, got nil")
	}
}

// TestRemoteCASListSkipsInvalidHex verifies that hex strings that can't be decoded or
// have the wrong length are silently skipped in List output.
func TestRemoteCASListSkipsInvalidHex(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// "nothex" is invalid hex; "aabb" is valid hex but too short
		json.NewEncoder(w).Encode([]string{"nothex", "aabb"})
	}))
	defer server.Close()

	cas := newCASFromServer(t, server)
	ch, err := cas.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 valid hashes, got %d", count)
	}
}
