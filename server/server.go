package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/wk-y/rama-swap/internal/util"
	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/microservices/scheduling"
)

type Server struct {
	ModelNameMangler func(string) string
	BasePort         int // starting port number to use for underlying instances

	ramalama  llama.Llama
	scheduler scheduling.Scheduler

	demangleCacheLock sync.RWMutex
	demangleCache     map[string]string
}

func NewServer(r llama.Llama, scheduler scheduling.Scheduler) *Server {
	return &Server{
		ramalama:      r,
		scheduler:     scheduler,
		demangleCache: map[string]string{},
	}
}

func (s *Server) HandleHttp(mux *http.ServeMux) {
	// OpenAI-compatible endpoints
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	mux.HandleFunc("POST /v1/completions", s.handleCompletions)

	// Loading status (polled by the frontend while a model is starting up)
	mux.HandleFunc("GET /api/ui/loading-status", s.handleLoadingStatus)

	// llama-swap style endpoint
	mux.HandleFunc("/upstream/{model}/{rest...}", s.serveUpstream)
	mux.HandleFunc("/upstream/{$}", s.serveUpstreamSelect)
}

func (s *Server) proxyEndpoint(w http.ResponseWriter, r *http.Request, modelFinder func(body io.Reader) (model string, err error)) {
	var decoderRead bytes.Buffer
	tee := io.TeeReader(r.Body, &decoderRead)

	model, err := modelFinder(tee)

	if err != nil {
		log.Println("Failed to determine model for request:", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing or invalid 'model' key"))
		return
	}

	body := r.Body
	r.Body = util.ReadCloserWrapper{
		Reader: io.MultiReader(&decoderRead, body),
		Closer: body.Close,
	}

	task := newTaskWithCompletion(newProxyTask(model, w, r))

	s.scheduler.OnNewTask(task)

	<-task.done
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	s.proxyEndpoint(w, r, func(body io.Reader) (model string, err error) {
		var modelGet struct {
			Model *string
		}

		err = json.NewDecoder(body).Decode(&modelGet)
		if err != nil {
			return
		}

		if modelGet.Model == nil {
			return "", fmt.Errorf("missing model key")
		}

		log.Printf("Chat completion requested for model %s", *modelGet.Model)
		return *modelGet.Model, nil
	})
}

func (s *Server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	s.proxyEndpoint(w, r, func(body io.Reader) (model string, err error) {
		var modelGet struct {
			Model *string
		}

		err = json.NewDecoder(body).Decode(&modelGet)
		if err != nil {
			return
		}

		if modelGet.Model == nil {
			return "", fmt.Errorf("missing model key")
		}

		return *modelGet.Model, nil
	})
}

func (s *Server) handleLoadingStatus(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Model string `json:"model"`
		Phase string `json:"phase"`
	}

	var resp response
	type statusProvider interface {
		GetLoadingStatus() (model, phase string)
	}
	if p, ok := s.scheduler.(statusProvider); ok {
		resp.Model, resp.Phase = p.GetLoadingStatus()
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	internalServerError := func(reason string) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error: "))
		w.Write([]byte(reason))
	}

	ramaModels, err := s.ramalama.GetModels()
	if err != nil {
		log.Printf("Failed to get models: %v\n", ramaModels)
		internalServerError("E_MODEL_GET")
		return
	}

	models, err := convertModelList(ramaModels)
	if err != nil {
		log.Printf("Failed to convert models: %v\n", models)
		internalServerError("E_MODEL_LIST_CONVERT")
		return
	}

	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(models)

	if err != nil {
		log.Printf("Failed to reply: %v\n", err)
	}
}
