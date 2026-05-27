package uiapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/tracker"
)

type stubStorageChunkControl struct {
	chunkSize int64
	err       error
}

func (s *stubStorageChunkControl) GetChunkSize() int64 {
	return s.chunkSize
}

func (s *stubStorageChunkControl) SetChunkSize(size int64) error {
	if s.err != nil {
		return s.err
	}
	s.chunkSize = size
	return nil
}

func newTestUI(t *testing.T, tr *tracker.Tracker, storage *stubStorageChunkControl) *UIApi {
	t.Helper()
	t.Setenv("RMD_METADATA_DB_PATH", filepath.Join(t.TempDir(), "metadata.db"))
	return New(tr, llama.Llama{}, nil, storage)
}

func TestHandleStorageChunkSize(t *testing.T) {
	ui := newTestUI(t, tracker.NewTracker(), &stubStorageChunkControl{chunkSize: 8 << 20})

	req := httptest.NewRequest(http.MethodGet, "/api/ui/storage-chunk-size", nil)
	rec := httptest.NewRecorder()
	ui.handleStorageChunkSize(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp storageChunkSizeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ChunkSizeBytes != 8<<20 {
		t.Fatalf("chunk_size_bytes = %d, want %d", resp.ChunkSizeBytes, 8<<20)
	}
}

func TestHandleStorageChunkSizeRejectsInvalidValue(t *testing.T) {
	ui := newTestUI(t, tracker.NewTracker(), &stubStorageChunkControl{err: errors.New("invalid chunk size")})

	req := httptest.NewRequest(http.MethodPost, "/api/ui/storage-chunk-size", strings.NewReader(`{"chunk_size_bytes":0}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ui.handleStorageChunkSize(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
