package uiapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ---- API types ----

type apiModel struct {
	Model            string `json:"model"`
	DisplayName      string `json:"display_name"`
	Parameters       string `json:"parameters,omitempty"`
	Architecture     string `json:"architecture,omitempty"`
	Quantization     string `json:"quantization,omitempty"`
	Source           string `json:"source"`
	LinkHref         string `json:"link_href"`
	LinkLabel        string `json:"link_label"`
	SupportsThinking bool   `json:"supports_thinking"`
}

type apiModelsResponse struct {
	Models []apiModel `json:"models"`
}

type apiSearchModel struct {
	Model       string `json:"model"`
	DisplayName string `json:"display_name"`
	Downloads   int    `json:"downloads"`
	LinkHref    string `json:"link_href"`
}

type apiSearchResponse struct {
	Results []apiSearchModel `json:"results"`
}

type apiErrorResponse struct {
	Error string `json:"error"`
}

type apiAddModelRequest struct {
	Model string `json:"model"`
}

type dashboardServerSnapshot struct {
	Id            string   `json:"id"`
	Ip            string   `json:"ip"`
	Port          int      `json:"port"`
	Nickname      string   `json:"nickname,omitempty"`
	HardwareModel string   `json:"hardware_model"`
	MaxSize       *int64   `json:"max_size"`
	Battery       *float64 `json:"battery"`
	Temperature   *float64 `json:"temperature"`
}

type dashboardDataResponse struct {
	Servers []dashboardServerSnapshot `json:"servers"`
}

type connectInfoResponse struct {
	Host                  string `json:"host"`
	Port                  int    `json:"port"`
	Token                 string `json:"token"`
	ConnectURI            string `json:"connect_uri"`
	TokenExpiresInSeconds int    `json:"token_expires_in_seconds"`
}

// ---- Chat session types ----

type startChatRequest struct {
	ChatID    string `json:"chat_id"`
	Model     string `json:"model"`
	StartedAt string `json:"started_at,omitempty"`
	UserID    string `json:"user_id,omitempty"`
}

type chatEventRequest struct {
	EventType string `json:"event_type"`
	MessageID string `json:"message_id,omitempty"`
	Role      string `json:"role,omitempty"`
	Content   string `json:"content,omitempty"`
	Token     string `json:"token,omitempty"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

type chatEvent struct {
	chatEventRequest
	Sequence int `json:"sequence"`
}

type chatSessionRecord struct {
	ChatID    string      `json:"chat_id"`
	Model     string      `json:"model"`
	StartedAt string      `json:"started_at"`
	Status    string      `json:"status"`
	Events    []chatEvent `json:"events"`
}

type chatConversationResponse struct {
	ID        string                    `json:"id"`
	Title     string                    `json:"title"`
	Model     string                    `json:"model"`
	Messages  []chatConversationMessage `json:"messages"`
	CreatedAt string                    `json:"created_at"`
	UpdatedAt string                    `json:"updated_at"`
	Status    string                    `json:"status"`
}

type chatConversationMessage struct {
	Role         string   `json:"role"`
	Content      string   `json:"content"`
	TokensPerSec *float64 `json:"tokensPerSec,omitempty"`
}

// ---- Core handlers ----

func (s *UIApi) handleAPIRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeAPIJSON(w, http.StatusOK, map[string]any{
		"models":    "/api/ui/models",
		"search":    "/api/ui/models/search",
		"dashboard": "/api/ui/dashboard",
		"connect":   "/api/ui/connect-info",
	})
}

func (s *UIApi) handleAPIConnectInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	host, port := preferredConnectHostPort(r)
	token, err := s.issueConnectToken()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to mint connect token")
		return
	}

	connectURI := fmt.Sprintf("rmcluster://connect?url=%s&port=%d&token=%s", host, port, token)
	writeAPIJSON(w, http.StatusOK, connectInfoResponse{
		Host:                  host,
		Port:                  port,
		Token:                 token,
		ConnectURI:            connectURI,
		TokenExpiresInSeconds: 120,
	})
}

// ---- Chat session handlers ----

func (s *UIApi) handleAPIStartChat(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method == http.MethodGet {
		s.listChatSessions(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req startChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if strings.TrimSpace(req.ChatID) == "" || strings.TrimSpace(req.Model) == "" {
		writeAPIError(w, http.StatusBadRequest, "chat_id and model are required")
		return
	}

	startedAt := req.StartedAt
	if startedAt == "" {
		startedAt = time.Now().UTC().Format(time.RFC3339)
	}

	if err := s.chatStore.createConversation(r.Context(), req.ChatID, req.Model, startedAt); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to create chat session")
		return
	}

	writeAPIJSON(w, http.StatusCreated, map[string]any{
		"chat_id":    req.ChatID,
		"model":      req.Model,
		"started_at": startedAt,
		"status":     "active",
	})
}

func (s *UIApi) handleAPIChatRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE")
		w.WriteHeader(http.StatusOK)
		return
	}
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/ui/chats/")
	parts := strings.SplitN(strings.Trim(trimmed, "/"), "/", 2)

	chatID := parts[0]
	if chatID == "" {
		writeAPIError(w, http.StatusNotFound, "not found")
		return
	}

	if len(parts) == 2 && parts[1] == "events" {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.appendChatEvent(w, r, chatID)
		return
	}

	if len(parts) == 2 && len(parts[1]) >= 4 && parts[1][:4] == "runs" {
		s.handleChatRunRoute(w, r, chatID, parts[1])
		return
	}

	if len(parts) == 1 {
		if r.Method == http.MethodDelete {
			s.deleteChatSession(w, r, chatID)
			return
		}
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.getChatSession(w, r, chatID)
		return
	}

	writeAPIError(w, http.StatusNotFound, "not found")
}

func (s *UIApi) appendChatEvent(w http.ResponseWriter, r *http.Request, chatID string) {
	var req chatEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := s.appendChatEventRecord(chatID, req); err != nil {
		switch err.Error() {
		case "event_type and timestamp are required", "invalid event_type", "event limit reached for this session":
			writeAPIError(w, http.StatusBadRequest, err.Error())
		case "chat session limit reached":
			writeAPIError(w, http.StatusTooManyRequests, err.Error())
		default:
			writeAPIError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	writeAPIJSON(w, http.StatusAccepted, map[string]any{"status": "accepted"})
}

func (s *UIApi) appendChatEventRecord(chatID string, req chatEventRequest) error {
	if req.EventType == "" || req.Timestamp == "" {
		return fmt.Errorf("event_type and timestamp are required")
	}

	validEventTypes := map[string]struct{}{
		"message_sent": {}, "token_received": {}, "message_completed": {},
		"stream_error": {}, "chat_closed": {}, "message_stopped": {},
	}
	if _, ok := validEventTypes[req.EventType]; !ok {
		return fmt.Errorf("invalid event_type")
	}

	if err := s.chatStore.createConversation(context.Background(), chatID, "unknown", time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	_, err := s.chatStore.appendEvent(context.Background(), chatID, req)
	return err
}

func (s *UIApi) getChatSession(w http.ResponseWriter, r *http.Request, chatID string) {
	conversation, err := s.chatStore.getConversation(r.Context(), chatID)
	if err != nil {
		if isNotFoundError(err) {
			writeAPIError(w, http.StatusNotFound, "chat session not found")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "failed to read chat session")
		return
	}

	writeAPIJSON(w, http.StatusOK, conversationResponse(conversation))
}

func (s *UIApi) listChatSessions(w http.ResponseWriter, r *http.Request) {
	conversations, err := s.chatStore.listConversations(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to list chat sessions")
		return
	}
	payload := make([]chatConversationResponse, 0, len(conversations))
	for _, conversation := range conversations {
		payload = append(payload, conversationResponse(conversation))
	}
	writeAPIJSON(w, http.StatusOK, map[string]any{"conversations": payload})
}

func (s *UIApi) deleteChatSession(w http.ResponseWriter, r *http.Request, chatID string) {
	if err := s.chatStore.deleteConversation(r.Context(), chatID); err != nil {
		if isNotFoundError(err) {
			writeAPIError(w, http.StatusNotFound, "chat session not found")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "failed to delete chat session")
		return
	}
	s.runLock.Lock()
	delete(s.activeRuns, chatID)
	s.runLock.Unlock()
	writeAPIJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func conversationResponse(conversation persistedConversation) chatConversationResponse {
	messages := make([]chatConversationMessage, 0, len(conversation.Messages))
	for _, message := range conversation.Messages {
		messages = append(messages, chatConversationMessage{
			Role:         message.Role,
			Content:      message.Content,
			TokensPerSec: message.TokensPerSec,
		})
	}
	return chatConversationResponse{
		ID:        conversation.ChatID,
		Title:     conversation.Title,
		Model:     conversation.Model,
		Messages:  messages,
		CreatedAt: conversation.CreatedAt,
		UpdatedAt: conversation.UpdatedAt,
		Status:    conversation.Status,
	}
}

// ---- noopResponseWriter ----

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header        { return make(http.Header) }
func (noopResponseWriter) Write([]byte) (int, error)  { return 0, nil }
func (noopResponseWriter) WriteHeader(statusCode int) {}

// ---- Token management ----

func (s *UIApi) issueConnectToken() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)

	now := time.Now()
	expires := now.Add(2 * time.Minute)

	s.connectLock.Lock()
	for k, v := range s.connectTokens {
		if now.After(v) {
			delete(s.connectTokens, k)
		}
	}
	s.connectTokens[token] = expires
	s.connectLock.Unlock()

	return token, nil
}

func (s *UIApi) consumeConnectToken(token string) bool {
	if token == "" {
		return false
	}
	now := time.Now()

	s.connectLock.Lock()
	defer s.connectLock.Unlock()

	expires, ok := s.connectTokens[token]
	if !ok || now.After(expires) {
		delete(s.connectTokens, token)
		return false
	}

	delete(s.connectTokens, token)
	return true
}

// ---- Network utilities ----

func preferredConnectHostPort(r *http.Request) (string, int) {
	if host := strings.TrimSpace(os.Getenv("RMD_CONNECT_HOST")); host != "" {
		return strings.Trim(host, "[]"), 4917
	}

	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0]); forwarded != "" {
		if host, _, err := net.SplitHostPort(forwarded); err == nil {
			forwarded = host
		}
		forwarded = strings.Trim(forwarded, "[]")
		if forwarded != "" {
			return preferredConnectHost(forwarded), 4917
		}
	}

	host := strings.TrimSpace(r.Host)
	port := 4917

	if parsedHost, parsedPort, err := net.SplitHostPort(host); err == nil {
		host = strings.Trim(parsedHost, "[]")
		if p, err := strconv.Atoi(parsedPort); err == nil && p > 0 {
			port = p
		}
	} else {
		host = strings.Trim(host, "[]")
	}

	if host == "" {
		host = "localhost"
	}

	return preferredConnectHost(host), port
}

func preferredConnectHost(host string) string {
	if !isLoopbackHost(host) {
		return host
	}
	if lanIP, ok := firstNonLoopbackIPv4(); ok {
		return lanIP
	}
	return host
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") || strings.EqualFold(host, "host.docker.internal") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func firstNonLoopbackIPv4() (string, bool) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", false
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if strings.HasPrefix(iface.Name, "docker") || strings.HasPrefix(iface.Name, "br-") || strings.HasPrefix(iface.Name, "veth") {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			if v4 := ip.To4(); v4 != nil {
				return v4.String(), true
			}
		}
	}

	return "", false
}

// ---- Model handlers ----

func (s *UIApi) handleAPIModels(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	entries := s.listModelEntries()
	models := make([]apiModel, 0, len(entries))
	for _, entry := range entries {
		params := entry.Parameters
		arch := entry.Architecture
		quant := entry.Quantization

		if strings.HasPrefix(entry.Model, "hf:") && (params == "" || arch == "" || quant == "") {
			repo, variant, ok := parseHFModelRef(entry.Model)
			if ok {
				meta := fetchHFMetadata(repo, variant)
				if params == "" {
					params = meta.Parameters
				}
				if arch == "" {
					arch = meta.Architecture
				}
				if quant == "" {
					quant = meta.Quantization
				}
			}
		}

		supportsThinking := false
		if strings.HasPrefix(entry.Model, "hf:") {
			if repo, variant, ok := parseHFModelRef(entry.Model); ok {
				supportsThinking = fetchHFMetadata(repo, variant).SupportsThinking
			}
		}

		models = append(models, apiModel{
			Model:            entry.Model,
			DisplayName:      entry.DisplayName,
			Parameters:       params,
			Architecture:     arch,
			Quantization:     quant,
			Source:           entry.Source,
			LinkHref:         entry.LinkHref,
			LinkLabel:        entry.LinkLabel,
			SupportsThinking: supportsThinking,
		})
	}

	writeAPIJSON(w, http.StatusOK, apiModelsResponse{Models: models})
}

func (s *UIApi) handleAPISearchModels(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeAPIJSON(w, http.StatusOK, apiSearchResponse{Results: []apiSearchModel{}})
		return
	}

	results, err := searchHFModels(query, 12)
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	items := make([]apiSearchModel, 0, len(results))
	for _, result := range results {
		hfRef := "hf:" + result.ID
		items = append(items, apiSearchModel{
			Model:       hfRef,
			DisplayName: simplifyModelDisplayName(hfRef),
			Downloads:   result.Downloads,
			LinkHref:    "https://huggingface.co/" + result.ID,
		})
	}

	writeAPIJSON(w, http.StatusOK, apiSearchResponse{Results: items})
}

func (s *UIApi) handleAPIAddHFModel(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req apiAddModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	entry, err := parseHFModelAddInput(req.Model)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	if hfStore == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "metadata store unavailable")
		return
	}
	if err := hfStore.AddCustomModel(entry); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to persist model")
		return
	}

	writeAPIJSON(w, http.StatusCreated, apiModel{
		Model:       entry.Model,
		DisplayName: entry.DisplayName,
		Source:      entry.Source,
		LinkHref:    entry.LinkHref,
		LinkLabel:   entry.LinkLabel,
	})
}

func (s *UIApi) handleAPILocalModelUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	const maxUploadBytes = 50 << 30 // 50 GiB
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	file, header, err := r.FormFile("model_file")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "model_file is required")
		return
	}
	defer file.Close()

	entry, err := uploadLocalModel(r, file, header)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	if hfStore == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "metadata store unavailable")
		return
	}
	if err := hfStore.AddCustomModel(entry); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to persist model")
		return
	}

	writeAPIJSON(w, http.StatusCreated, apiModel{
		Model:        entry.Model,
		DisplayName:  entry.DisplayName,
		Parameters:   entry.Parameters,
		Architecture: entry.Architecture,
		Quantization: entry.Quantization,
		Source:       entry.Source,
		LinkHref:     entry.LinkHref,
		LinkLabel:    entry.LinkLabel,
	})
}

func (s *UIApi) handleAPIDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	servers := s.tracker.GetServers()
	payload := dashboardDataResponse{Servers: make([]dashboardServerSnapshot, 0, len(servers))}
	for _, server := range servers {
		snapshot := dashboardServerSnapshot{
			Id:            server.Id,
			Ip:            server.Ip,
			Port:          server.Port,
			Nickname:      server.Nickname,
			HardwareModel: server.HardwareModel,
		}
		if server.MaxSize >= 0 {
			value := server.MaxSize
			snapshot.MaxSize = &value
		}
		if !math.IsNaN(server.Battery) {
			value := server.Battery
			snapshot.Battery = &value
		}
		if !math.IsNaN(server.Temperature) {
			value := server.Temperature
			snapshot.Temperature = &value
		}
		payload.Servers = append(payload.Servers, snapshot)
	}

	writeAPIJSON(w, http.StatusOK, payload)
}

// ---- JSON helpers ----

func writeAPIJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeAPIJSON(w, status, apiErrorResponse{Error: message})
}

func uploadLocalModel(r *http.Request, file multipart.File, header *multipart.FileHeader) (customModelEntry, error) {
	if !strings.EqualFold(filepath.Ext(header.Filename), ".gguf") {
		return customModelEntry{}, fmt.Errorf("only .gguf models are allowed")
	}

	storageDir := localModelStorageDir()
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return customModelEntry{}, err
	}

	baseName := filepath.Base(header.Filename)
	ext := filepath.Ext(baseName)
	stem := strings.TrimSuffix(baseName, ext)
	var (
		destinationPath string
		destination     *os.File
		err             error
	)
	for i := 0; ; i++ {
		candidate := baseName
		if i > 0 {
			candidate = fmt.Sprintf("%s-%d%s", stem, i, ext)
		}
		destinationPath = filepath.Join(storageDir, candidate)
		destination, err = os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return customModelEntry{}, err
		}
	}

	if _, err := io.Copy(destination, file); err != nil {
		_ = destination.Close()
		_ = os.Remove(destinationPath)
		return customModelEntry{}, err
	}
	if err := destination.Close(); err != nil {
		return customModelEntry{}, err
	}

	name := strings.TrimSpace(r.FormValue("name"))
	parameters := strings.TrimSpace(r.FormValue("parameters"))
	quantization := strings.TrimSpace(r.FormValue("quantization"))
	entry, err := parseLocalModelInput(name, destinationPath, parameters, quantization)
	if err != nil {
		_ = os.Remove(destinationPath)
		return customModelEntry{}, err
	}
	return entry, nil
}
