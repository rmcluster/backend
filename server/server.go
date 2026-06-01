package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/rmcluster/backend/internal/util"
	"github.com/rmcluster/backend/llama"
	"github.com/rmcluster/backend/server/conversations"
	"github.com/rmcluster/backend/server/scheduling"
)

type Server struct {
	ModelNameMangler func(string) string
	BasePort         int // starting port number to use for underlying instances

	ramalama      llama.Llama
	scheduler     scheduling.Scheduler
	Conversations *conversations.ConversationsService

	demangleCacheLock sync.RWMutex
	demangleCache     map[string]string
}

func NewServer(r llama.Llama, scheduler scheduling.Scheduler, conversationsSvc *conversations.ConversationsService) *Server {
	return &Server{
		ramalama:      r,
		scheduler:     scheduler,
		Conversations: conversationsSvc,
		demangleCache: map[string]string{},
	}
}

func (s *Server) HandleHttp(mux *http.ServeMux) {
	// OpenAI-compatible endpoints
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)
	mux.HandleFunc("/v1/completions", s.handleCompletions)
	mux.HandleFunc("/v1/conversations", s.handleConversations)
	mux.HandleFunc("/v1/conversations/", s.handleConversationByID)
	mux.HandleFunc("/v1/responses", s.handleResponses)
	mux.HandleFunc("/v1/responses/", s.handleResponseByID)

	// llama-swap style endpoint
	mux.HandleFunc("/upstream/", s.serveUpstream)
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
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

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

func (s *Server) handleConversations(w http.ResponseWriter, r *http.Request) {
	if s.Conversations == nil {
		http.Error(w, "conversations service unavailable", http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handleCreateConversation(w, r)
	case http.MethodGet:
		s.handleListConversations(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleConversationByID(w http.ResponseWriter, r *http.Request) {
	if s.Conversations == nil {
		http.Error(w, "conversations service unavailable", http.StatusInternalServerError)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/conversations/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetConversation(w, r, id)
	case http.MethodDelete:
		s.handleDeleteConversation(w, r, id)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	if s.Conversations == nil {
		http.Error(w, "conversations service unavailable", http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handleCreateResponse(w, r)
	case http.MethodGet:
		s.handleListResponses(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleResponseByID(w http.ResponseWriter, r *http.Request) {
	if s.Conversations == nil {
		http.Error(w, "conversations service unavailable", http.StatusInternalServerError)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/responses/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetResponse(w, r, id)
	default:
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleCreateConversation(w http.ResponseWriter, r *http.Request) {
	var conv conversations.Conversation
	if err := json.NewDecoder(r.Body).Decode(&conv); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request body"))
		return
	}

	if conv.Metadata == nil {
		conv.Metadata = map[string]string{}
	}

	if err := s.Conversations.CreateConversation(&conv); err != nil {
		if err == conversations.ErrInvalidConversationId || err == conversations.ErrInvalidConversationObject || err == conversations.ErrInvalidConversationCreatedAt {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		log.Printf("Failed to create conversation: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(conv)
}

func (s *Server) handleListConversations(w http.ResponseWriter, r *http.Request) {
	conversationsList, err := s.Conversations.ListConversations()
	if err != nil {
		log.Printf("Failed to list conversations: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	response := map[string]any{
		"object": "list",
		"data":   conversationsList,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetConversation(w http.ResponseWriter, r *http.Request, id string) {
	conversation, err := s.Conversations.GetConversation(id)
	if err != nil {
		if err == conversations.ErrConversationNotFound {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("conversation not found"))
			return
		}
		log.Printf("Failed to get conversation %s: %v", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(conversation)
}

func (s *Server) handleDeleteConversation(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.Conversations.DeleteConversation(id); err != nil {
		if err == conversations.ErrConversationNotFound {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("conversation not found"))
			return
		}
		log.Printf("Failed to delete conversation %s: %v", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]any{"id": id, "deleted": true})
}

func (s *Server) handleCreateResponse(w http.ResponseWriter, r *http.Request) {
	var resp conversations.Response
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request body"))
		return
	}

	if resp.Metadata == nil {
		resp.Metadata = map[string]string{}
	}

	if err := s.Conversations.CreateResponse(&resp); err != nil {
		if err == conversations.ErrInvalidResponseId || err == conversations.ErrInvalidResponseObject || err == conversations.ErrInvalidResponseCreatedAt || err == conversations.ErrInvalidResponseModel {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		log.Printf("Failed to create response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleListResponses(w http.ResponseWriter, r *http.Request) {
	responsesList, err := s.Conversations.ListResponses()
	if err != nil {
		log.Printf("Failed to list responses: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	response := map[string]any{
		"object": "list",
		"data":   responsesList,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetResponse(w http.ResponseWriter, r *http.Request, id string) {
	response, err := s.Conversations.GetResponse(id)
	if err != nil {
		if err == conversations.ErrResponseNotFound {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("response not found"))
			return
		}
		log.Printf("Failed to get response %s: %v", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
