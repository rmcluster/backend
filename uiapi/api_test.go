package uiapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/rmcluster/backend/llama"
	"github.com/rmcluster/backend/server/scheduling"
	"github.com/rmcluster/backend/tracker"
)

type stubTunableScheduler struct {
	name   string
	specs  []scheduling.TunableSpec
	values map[string]any
}

func (s *stubTunableScheduler) OnNewTask(scheduling.Task)        {}
func (s *stubTunableScheduler) OnTaskCancelled(scheduling.Task)  {}
func (s *stubTunableScheduler) OnNodeConnect(scheduling.Node)    {}
func (s *stubTunableScheduler) OnNodeDisconnect(scheduling.Node) {}

func (s *stubTunableScheduler) SchedulerName() string {
	return s.name
}

func (s *stubTunableScheduler) TunableSpecs() []scheduling.TunableSpec {
	return s.specs
}

func (s *stubTunableScheduler) TunableValues() map[string]any {
	return s.values
}

func (s *stubTunableScheduler) ApplyTunables(values map[string]any) error {
	for key, value := range values {
		s.values[key] = value
	}
	return nil
}

type stubStorageTunables struct {
	chunkSize int64
}

func (s *stubStorageTunables) TunableSpecs() []scheduling.TunableSpec {
	return []scheduling.TunableSpec{
		{Key: "chunk_size_mib", Label: "Chunk size", Kind: scheduling.TunableKindInt, Unit: "MiB"},
	}
}

func (s *stubStorageTunables) TunableValues() map[string]any {
	return map[string]any{"chunk_size_mib": s.chunkSize / (1024 * 1024)}
}

func (s *stubStorageTunables) ApplyTunables(values map[string]any) error {
	if raw, ok := values["chunk_size_mib"]; ok {
		switch v := raw.(type) {
		case int:
			s.chunkSize = int64(v) * 1024 * 1024
		case float64:
			s.chunkSize = int64(v) * 1024 * 1024
		}
	}
	return nil
}

func newTestUI(
	t *testing.T,
	tr *tracker.Tracker,
	scheduler scheduling.TunableScheduler,
	storage *stubStorageTunables,
) *UIApi {
	t.Helper()
	t.Setenv("RMD_METADATA_DB_PATH", filepath.Join(t.TempDir(), "metadata.db"))
	return New(tr, llama.Llama{}, nil, scheduler, storage)
}

func TestHandleTunables(t *testing.T) {
	tr := tracker.NewTracker()
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-1", Ip: "10.0.0.1", Port: 9001})
	tr.RegisterNode(tracker.RpcServerInfo{Id: "node-2", Ip: "10.0.0.2", Port: 9002})

	scheduler := &stubTunableScheduler{
		name: "stub",
		specs: []scheduling.TunableSpec{
			{Key: scheduling.TunableParallelismTarget, Label: "Parallelism target", Kind: scheduling.TunableKindInt},
		},
		values: map[string]any{
			scheduling.TunableParallelismTarget: float64(2),
		},
	}
	const bytesPerMiB = 1024 * 1024
	storage := &stubStorageTunables{chunkSize: 8 * bytesPerMiB}
	ui := newTestUI(t, tr, scheduler, storage)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/tunables", nil)
	rec := httptest.NewRecorder()
	ui.handleTunables(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp tunablesResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Sections) != 2 {
		t.Fatalf("sections = %d, want 2", len(resp.Sections))
	}

	body, err := json.Marshal(tunablesRequest{
		Section: tunablesSectionStorage,
		Values: map[string]any{
			"chunk_size_mib": 16,
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/ui/tunables", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	ui.handleTunables(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusOK)
	}
	if storage.chunkSize != 16*bytesPerMiB {
		t.Fatalf("chunk size = %d, want %d", storage.chunkSize, 16*bytesPerMiB)
	}
}
