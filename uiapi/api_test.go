package uiapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/tracker"
)

type stubScheduler struct {
	target int
}

func (s *stubScheduler) GetParallelismTarget() int {
	return s.target
}

func (s *stubScheduler) SetParallelismTarget(n int) {
	s.target = n
}

func newTestUI(t *testing.T, tr *tracker.Tracker, scheduler *stubScheduler) *UIApi {
	t.Helper()
	t.Setenv("RMD_METADATA_DB_PATH", filepath.Join(t.TempDir(), "metadata.db"))
	return New(tr, llama.Llama{}, nil, scheduler)
}

func TestHandleParallelismTarget(t *testing.T) {
	tr := tracker.NewTracker()
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-1", Ip: "10.0.0.1", Port: 9001})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-2", Ip: "10.0.0.2", Port: 9002})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-3", Ip: "10.0.0.3", Port: 9003})
	ui := newTestUI(t, tr, &stubScheduler{target: 3})

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
	ui := newTestUI(t, tr, &stubScheduler{target: 1})

	req := httptest.NewRequest(http.MethodPost, "/api/ui/parallelism-target", strings.NewReader(`{"parallelism_target":3}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ui.handleParallelismTarget(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
