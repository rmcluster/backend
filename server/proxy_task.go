package server

import (
	"fmt"
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
	instance.ReverseProxy().ServeHTTP(p.w, p.r)
	return
}

var _ scheduling.Task = (*proxyTask)(nil)
