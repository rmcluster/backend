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
	"github.com/wk-y/rama-swap/microservices/metrics"
	"github.com/wk-y/rama-swap/tracker"
)

type stubScheduler struct {
	target      int
	allocations map[string][]string
}

func (s *stubScheduler) GetParallelismTarget() int {
	return s.target
}

func (s *stubScheduler) SetParallelismTarget(n int) {
	s.target = n
}

func (s *stubScheduler) GetAllocatedNodesForModel(model string) []string {
	return append([]string(nil), s.allocations[model]...)
}

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

func newTestUI(t *testing.T, tr *tracker.Tracker, scheduler *stubScheduler, storage *stubStorageChunkControl, collector *metrics.Collector) *UIApi {
	t.Helper()
	t.Setenv("RMD_METADATA_DB_PATH", filepath.Join(t.TempDir(), "metadata.db"))
	return New(tr, llama.Llama{}, nil, scheduler, storage, collector)
}

func TestHandleParallelismTarget(t *testing.T) {
	tr := tracker.NewTracker()
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-1", Ip: "10.0.0.1", Port: 9001})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-2", Ip: "10.0.0.2", Port: 9002})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-3", Ip: "10.0.0.3", Port: 9003})
	ui := newTestUI(t, tr, &stubScheduler{target: 3}, &stubStorageChunkControl{chunkSize: 8 << 20}, metrics.NewCollector(10))

	req := httptest.NewRequest(http.MethodGet, "/api/ui/parallelism-target", nil)
	rec := httptest.NewRecorder()
	ui.handleParallelismTarget(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp parallelismTargetResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ParallelismTarget != 3 {
		t.Fatalf("parallelism target = %d, want 3", resp.ParallelismTarget)
	}
}

func TestHandleParallelismTargetRejectsValueAboveConnectedNodeCount(t *testing.T) {
	tr := tracker.NewTracker()
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-1", Ip: "10.0.0.1", Port: 9001})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-2", Ip: "10.0.0.2", Port: 9002})
	ui := newTestUI(t, tr, &stubScheduler{target: 1}, &stubStorageChunkControl{chunkSize: 8 << 20}, metrics.NewCollector(10))

	req := httptest.NewRequest(http.MethodPost, "/api/ui/parallelism-target", strings.NewReader(`{"parallelism_target":3}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ui.handleParallelismTarget(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleStorageChunkSize(t *testing.T) {
	ui := newTestUI(t, tracker.NewTracker(), &stubScheduler{target: 1}, &stubStorageChunkControl{chunkSize: 8 << 20}, metrics.NewCollector(10))

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
	ui := newTestUI(t, tracker.NewTracker(), &stubScheduler{target: 1}, &stubStorageChunkControl{err: errors.New("invalid chunk size")}, metrics.NewCollector(10))

	req := httptest.NewRequest(http.MethodPost, "/api/ui/storage-chunk-size", strings.NewReader(`{"chunk_size_bytes":0}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ui.handleStorageChunkSize(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleAllocations(t *testing.T) {
	tr := tracker.NewTracker()
	tr.RegisterNode(tracker.RpcServerInfo{
		Id:            "tracker-node-1",
		Ip:            "10.0.0.2",
		Port:          9001,
		HardwareModel: "Phone A",
	})

	ui := newTestUI(t, tr, &stubScheduler{
		target:      2,
		allocations: map[string][]string{"demo-model": {"10.0.0.2:9001", "10.0.0.3:9002"}},
	}, &stubStorageChunkControl{chunkSize: 8 << 20}, metrics.NewCollector(10))

	req := httptest.NewRequest(http.MethodGet, "/api/ui/allocations?model=demo-model", nil)
	rec := httptest.NewRecorder()
	ui.handleAllocations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp allocationsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Devices) != 2 {
		t.Fatalf("device count = %d, want 2", len(resp.Devices))
	}
	if resp.Devices[0].HardwareModel != "Phone A" {
		t.Fatalf("first device hardware model = %q, want %q", resp.Devices[0].HardwareModel, "Phone A")
	}
	if resp.Devices[1].Label != "10.0.0.3:9002" {
		t.Fatalf("unmapped device label = %q, want node id fallback", resp.Devices[1].Label)
	}
}

func TestHandleMetrics(t *testing.T) {
	collector := metrics.NewCollector(10)
	collector.Record(metrics.RequestMetric{
		Model:           "demo-model",
		Path:            "/v1/chat/completions",
		DurationMs:      1200,
		ResponseBytes:   2048,
		TokensStreamed:  512,
		TokensPerSecond: 42.5,
	})

	ui := newTestUI(t, tracker.NewTracker(), &stubScheduler{target: 1}, &stubStorageChunkControl{chunkSize: 8 << 20}, collector)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/metrics", nil)
	rec := httptest.NewRecorder()
	ui.handleMetrics(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Summary struct {
			RequestCount int `json:"request_count"`
		} `json:"summary"`
		Requests []struct {
			Model string `json:"model"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Summary.RequestCount != 1 {
		t.Fatalf("request_count = %d, want 1", resp.Summary.RequestCount)
	}
	if len(resp.Requests) != 1 || resp.Requests[0].Model != "demo-model" {
		t.Fatalf("unexpected requests payload: %+v", resp.Requests)
	}
}
