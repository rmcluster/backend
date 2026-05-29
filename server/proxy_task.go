package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/rmcluster/backend/microservices/scheduling"
)

type proxyTask struct {
	model string
	w     http.ResponseWriter
	r     *http.Request
}

func newProxyTask(model string, w http.ResponseWriter, r *http.Request) *proxyTask {
	return &proxyTask{
		model: model,
		w:     w,
		r:     r,
	}
}

// Model implements [scheduling.Task].
func (p *proxyTask) Model() string {
	return p.model
}

// Fail writes a 503 error response so the waiting HTTP handler can unblock.
func (p *proxyTask) Fail(err error) {
	p.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	p.w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(p.w).Encode(map[string]string{"error": err.Error()})
}

// PerformInference implements [scheduling.Task].
func (p *proxyTask) PerformInference(instance scheduling.Instance) (err error) {
	// ServeHTTP can panic if the connection to the llama.cpp instance is broken, so we need to handle it and return an error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("error proxying request for model %v: %v", p.model, r)
		}
	}()
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
	instance.ReverseProxy().ServeHTTP(flushingWriter{p.w}, p.r)
	return
}

var _ scheduling.Task = (*proxyTask)(nil)

// flushingWriter wraps an http.ResponseWriter and flushes after every Write so
// streaming responses (SSE, token-by-token JSON) reach the client immediately.
type flushingWriter struct {
	http.ResponseWriter
}

func (fw flushingWriter) Write(b []byte) (int, error) {
	n, err := fw.ResponseWriter.Write(b)
	if f, ok := fw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}
