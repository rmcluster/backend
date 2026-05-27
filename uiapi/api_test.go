package uiapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/microservices/metrics"
	"github.com/wk-y/rama-swap/tracker"
)

func newTestUI(t *testing.T, tr *tracker.Tracker, collector *metrics.Collector) *UIApi {
	t.Helper()
	t.Setenv("RMD_METADATA_DB_PATH", filepath.Join(t.TempDir(), "metadata.db"))
	return New(tr, llama.Llama{}, nil, collector)
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

	ui := newTestUI(t, tracker.NewTracker(), collector)
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
