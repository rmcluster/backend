package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/wk-y/rama-swap/microservices/scheduling"
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
func (p *proxyTask) PerformInference(instance scheduling.Instance) error {
	log.Printf("Proxying request for model %v", p.model)
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("Proxy aborted for model %v: %v", p.model, recovered)
		}
	}()

	// Forward the request to the upstream llama server. The Go reverse proxy
	// uses a panic-based abort path for client disconnects and other write
	// failures; recover so that a cancelled stream does not crash the server.
	instance.ReverseProxy().ServeHTTP(p.w, p.r)
	return nil
}

var _ scheduling.Task = (*proxyTask)(nil)
