package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wk-y/rama-swap/microservices/metrics"
	"github.com/wk-y/rama-swap/microservices/scheduling"
)

type proxyTask struct {
	model            string
	w                http.ResponseWriter
	r                *http.Request
	metricsCollector *metrics.Collector
	allocations      allocationViewer
	allocatedNodeIDs []string
}

type allocationViewer interface {
	GetAllocatedNodesForModel(model string) []string
}

func newProxyTask(model string, w http.ResponseWriter, r *http.Request, metricsCollector *metrics.Collector, allocations allocationViewer) *proxyTask {
	return &proxyTask{
		model:            model,
		w:                w,
		r:                r,
		metricsCollector: metricsCollector,
		allocations:      allocations,
	}
}

// Model implements [scheduling.Task].
func (p *proxyTask) Model() string {
	return p.model
}

func (p *proxyTask) SetAllocatedNodes(nodes []scheduling.Node) {
	nodeIDs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.Id())
	}
	p.allocatedNodeIDs = nodeIDs
}

// Fail writes a 503 error response so the waiting HTTP handler can unblock.
func (p *proxyTask) Fail(err error) {
	p.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	p.w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(p.w).Encode(map[string]string{"error": err.Error()})
}

// PerformInference implements [scheduling.Task].
func (p *proxyTask) PerformInference(instance scheduling.Instance) (err error) {
	startedAt := time.Now()
	writer := newInstrumentedFlushingWriter(p.w)

	// ServeHTTP can panic if the connection to the llama.cpp instance is broken, so we need to handle it and return an error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("error proxying request for model %v: %v", p.model, r)
		}
	}()
	defer p.recordMetrics(startedAt, writer)
	log.Printf("Proxying request for model %v", p.model)
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("Proxy aborted for model %v: %v", p.model, recovered)
		}
	}()

	// Forward the request to the upstream llama server. The Go reverse proxy
	// uses a panic-based abort path for client disconnects and other write
	// failures; recover so that a cancelled stream does not crash the server.
	//
	// Wrap the writer so each chunk is flushed immediately; without this, gin's
	// buffered ResponseWriter holds SSE/streaming tokens until the buffer fills
	// rather than delivering them token-by-token.
	instance.ReverseProxy().ServeHTTP(writer, p.r)
	return
}

var _ scheduling.Task = (*proxyTask)(nil)

func (p *proxyTask) recordMetrics(startedAt time.Time, writer *instrumentedFlushingWriter) {
	if p.metricsCollector == nil {
		return
	}

	nodeIDs := []string(nil)
	if len(p.allocatedNodeIDs) > 0 {
		nodeIDs = append(nodeIDs, p.allocatedNodeIDs...)
	} else if p.allocations != nil {
		nodeIDs = p.allocations.GetAllocatedNodesForModel(p.model)
	}

	duration := time.Since(startedAt)
	tokensPerSecond := 0.0
	if writer.tokensStreamed > 0 && duration > 0 {
		tokensPerSecond = float64(writer.tokensStreamed) / duration.Seconds()
	}

	p.metricsCollector.Record(metrics.RequestMetric{
		ClientRequestID:    p.r.Header.Get("X-Benchmark-Request-Id"),
		Model:              p.model,
		Path:               p.r.URL.Path,
		StartedAt:          startedAt.UTC().Format(time.RFC3339Nano),
		CompletedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		DurationMs:         duration.Milliseconds(),
		ResponseBytes:      writer.responseBytes,
		StreamedTextBytes:  writer.streamTextBytes,
		TokensStreamed:     writer.tokensStreamed,
		TokensPerSecond:    tokensPerSecond,
		AllocatedNodeCount: len(nodeIDs),
		AllocatedNodeIDs:   nodeIDs,
	})
}

// instrumentedFlushingWriter wraps an http.ResponseWriter, flushes after every
// Write, and opportunistically parses streamed OpenAI SSE deltas for lightweight
// throughput metrics.
type instrumentedFlushingWriter struct {
	http.ResponseWriter
	responseBytes   int64
	streamTextBytes int64
	tokensStreamed  int64
	sseBuffer       bytes.Buffer
}

func newInstrumentedFlushingWriter(w http.ResponseWriter) *instrumentedFlushingWriter {
	return &instrumentedFlushingWriter{ResponseWriter: w}
}

func (fw *instrumentedFlushingWriter) Write(b []byte) (int, error) {
	fw.responseBytes += int64(len(b))
	fw.consumeSSE(b)
	n, err := fw.ResponseWriter.Write(b)
	if f, ok := fw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

func (fw *instrumentedFlushingWriter) consumeSSE(b []byte) {
	fw.sseBuffer.Write(b)
	lines := strings.Split(fw.sseBuffer.String(), "\n")
	if len(lines) == 0 {
		return
	}

	fw.sseBuffer.Reset()
	if tail := lines[len(lines)-1]; tail != "" {
		fw.sseBuffer.WriteString(tail)
	}

	for _, line := range lines[:len(lines)-1] {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if payload == "" || payload == "[DONE]" {
			continue
		}

		var parsed struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
			continue
		}
		for _, choice := range parsed.Choices {
			fw.addDelta(choice.Delta.Content)
			fw.addDelta(choice.Delta.ReasoningContent)
		}
	}
}

func (fw *instrumentedFlushingWriter) addDelta(text string) {
	if text == "" {
		return
	}
	fw.streamTextBytes += int64(len(text))
	fw.tokensStreamed += int64(estimateTokenCount(text))
}

func estimateTokenCount(text string) int {
	count := utf8.RuneCountInString(text) / 4
	if count < 1 {
		return 1
	}
	return count
}
