package uiapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/openai/openai-go/v2"
	"github.com/rmcluster/backend/server/scheduling"
)

const chatRunRetryMillis = 1500

type chatRunStatus string

const (
	chatRunStatusWaiting   chatRunStatus = "waiting_for_capacity"
	chatRunStatusStarting  chatRunStatus = "starting"
	chatRunStatusStreaming chatRunStatus = "streaming"
	chatRunStatusStopped   chatRunStatus = "stopped"
	chatRunStatusCompleted chatRunStatus = "completed"
	chatRunStatusError     chatRunStatus = "error"
)

type chatRunMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type startChatRunRequest struct {
	Model           string           `json:"model"`
	Messages        []chatRunMessage `json:"messages"`
	ThinkingEnabled *bool            `json:"thinking_enabled,omitempty"`
}

type stopChatRunResponse struct {
	Status string `json:"status"`
}

type chatRunSnapshot struct {
	ChatID           string        `json:"chat_id"`
	Model            string        `json:"model"`
	Status           chatRunStatus `json:"status"`
	AssistantContent string        `json:"assistant_content"`
	LoadingPhase     string        `json:"loading_phase"`
	LoadingProgress  float64       `json:"loading_progress"`
	LayersOnRPC      int           `json:"layers_on_rpc"`
	StartedAt        string        `json:"started_at"`
	UpdatedAt        string        `json:"updated_at"`
	Error            string        `json:"error,omitempty"`
	Sequence         int           `json:"sequence"`
}

type chatRunStreamEvent struct {
	Type     string          `json:"type"`
	Snapshot chatRunSnapshot `json:"snapshot"`
	Delta    string          `json:"delta,omitempty"`
}

type chatRunState struct {
	ui                       *UIApi
	mu                       sync.Mutex
	chatID                   string
	model                    string
	loadingScopeID           string
	status                   chatRunStatus
	assistantContent         string
	loadingPhase             string
	loadingProgress          float64
	layersOnRPC              int
	startedAt                string
	updatedAt                string
	err                      string
	sequence                 int
	assistantMessageSequence int
	cancel                   context.CancelFunc
	task                     *chatRunTask
	subscribers              map[chan chatRunStreamEvent]struct{}
}

func (r *chatRunState) snapshotLocked() chatRunSnapshot {
	return chatRunSnapshot{
		ChatID:           r.chatID,
		Model:            r.model,
		Status:           r.status,
		AssistantContent: r.assistantContent,
		LoadingPhase:     r.loadingPhase,
		LoadingProgress:  r.loadingProgress,
		LayersOnRPC:      r.layersOnRPC,
		StartedAt:        r.startedAt,
		UpdatedAt:        r.updatedAt,
		Error:            r.err,
		Sequence:         r.sequence,
	}
}

func (r *chatRunState) snapshot() chatRunSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

func (r *chatRunState) addSubscriber(ch chan chatRunStreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.subscribers == nil {
		r.subscribers = make(map[chan chatRunStreamEvent]struct{})
	}
	r.subscribers[ch] = struct{}{}
}

func (r *chatRunState) removeSubscriber(ch chan chatRunStreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subscribers, ch)
}

func (r *chatRunState) emit(eventType string, delta string) {
	r.mu.Lock()
	r.sequence++
	r.updatedAt = time.Now().UTC().Format(time.RFC3339)
	snapshot := r.snapshotLocked()
	assistantSequence := r.assistantMessageSequence
	subscribers := make([]chan chatRunStreamEvent, 0, len(r.subscribers))
	for ch := range r.subscribers {
		subscribers = append(subscribers, ch)
	}
	r.mu.Unlock()

	if r.ui != nil {
		if err := r.ui.chatStore.saveRunSnapshot(context.Background(), snapshot, assistantSequence); err != nil {
			log.Printf("failed to persist run snapshot for %s: %v", r.chatID, err)
		}
	}

	event := chatRunStreamEvent{Type: eventType, Snapshot: snapshot, Delta: delta}
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (r *chatRunState) bindInstance(instance scheduling.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.loadingScopeID = instance.LoadingStatusScopeID()
	if r.status != chatRunStatusWaiting && r.status != chatRunStatusStarting {
		return
	}

	r.status = chatRunStatusStarting
	_, phase, progress, layersOnRPC := instance.GetLoadingStatus()
	r.loadingPhase = phase
	r.loadingProgress = progress
	r.layersOnRPC = layersOnRPC
}

func (r *chatRunState) updateLoading(scopeID, phase string, progress float64, layersOnRPC int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loadingScopeID == "" || r.loadingScopeID != scopeID || r.status != chatRunStatusStarting {
		return false
	}
	if phase == "" {
		return false
	}
	r.loadingPhase = phase
	r.loadingProgress = progress
	r.layersOnRPC = layersOnRPC
	return true
}

func (r *chatRunState) markStreaming(delta string) {
	r.mu.Lock()
	if r.status == chatRunStatusStopped || r.status == chatRunStatusCompleted || r.status == chatRunStatusError {
		r.mu.Unlock()
		return
	}
	r.status = chatRunStatusStreaming
	r.loadingPhase = ""
	r.loadingProgress = 0
	r.err = ""
	r.assistantContent += delta
	r.mu.Unlock()
	r.emit("delta", delta)
}

func (r *chatRunState) markCompleted() {
	r.mu.Lock()
	if r.status == chatRunStatusStopped || r.status == chatRunStatusCompleted || r.status == chatRunStatusError {
		r.mu.Unlock()
		return
	}
	r.status = chatRunStatusCompleted
	r.loadingPhase = ""
	r.loadingProgress = 0
	r.err = ""
	r.mu.Unlock()
	r.emit("completed", "")
}

func (r *chatRunState) markStopped() {
	r.mu.Lock()
	if r.status == chatRunStatusStopped || r.status == chatRunStatusCompleted || r.status == chatRunStatusError {
		r.mu.Unlock()
		return
	}
	r.status = chatRunStatusStopped
	r.loadingPhase = ""
	r.loadingProgress = 0
	r.err = ""
	r.mu.Unlock()
	r.emit("stopped", "")
}

func (r *chatRunState) markErrored(message string) {
	r.mu.Lock()
	if r.status == chatRunStatusStopped || r.status == chatRunStatusCompleted || r.status == chatRunStatusError {
		r.mu.Unlock()
		return
	}
	r.status = chatRunStatusError
	r.loadingPhase = ""
	r.loadingProgress = 0
	r.err = message
	r.mu.Unlock()
	r.emit("error", "")
}

func (s *UIApi) onLoadingStatusUpdate(scopeID, model, phase string, progress float64, layersOnRPC int) {
	s.runLock.Lock()
	runs := make([]*chatRunState, 0, len(s.activeRuns))
	for _, run := range s.activeRuns {
		runs = append(runs, run)
	}
	s.runLock.Unlock()

	for _, run := range runs {
		if run.updateLoading(scopeID, phase, progress, layersOnRPC) {
			run.emit("snapshot", "")
		}
	}
}

func (s *UIApi) handleChatRunRoute(w http.ResponseWriter, r *http.Request, chatID string, suffix string) {
	switch suffix {
	case "runs":
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.startChatRun(w, r, chatID)
	case "runs/current":
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.getCurrentChatRun(w, r, chatID)
	case "runs/current/stream":
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.streamCurrentChatRun(w, r, chatID)
	case "runs/current/stop":
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.stopCurrentChatRun(w, r, chatID)
	default:
		writeAPIError(w, http.StatusNotFound, "not found")
	}
}

func (s *UIApi) startChatRun(w http.ResponseWriter, r *http.Request, chatID string) {
	if s.scheduler == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "chat scheduler unavailable")
		return
	}

	var req startChatRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Model == "" || len(req.Messages) == 0 {
		writeAPIError(w, http.StatusBadRequest, "model and messages are required")
		return
	}

	params, err := buildChatCompletionParams(req.Model, req.Messages, req.ThinkingEnabled)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UTC().Format(time.RFC3339)
	run := &chatRunState{
		ui:          s,
		chatID:      chatID,
		model:       req.Model,
		status:      chatRunStatusWaiting,
		startedAt:   now,
		updatedAt:   now,
		cancel:      cancel,
		subscribers: make(map[chan chatRunStreamEvent]struct{}),
	}
	task := &chatRunTask{
		ctx:    ctx,
		ui:     s,
		run:    run,
		params: params,
	}
	run.task = task

	s.runLock.Lock()
	if existing := s.activeRuns[chatID]; existing != nil {
		snapshot := existing.snapshot()
		if snapshot.Status == chatRunStatusWaiting || snapshot.Status == chatRunStatusStarting || snapshot.Status == chatRunStatusStreaming {
			s.runLock.Unlock()
			cancel()
			writeAPIError(w, http.StatusConflict, "chat already has an active run")
			return
		}
	}
	s.activeRuns[chatID] = run
	s.runLock.Unlock()

	if err := s.chatStore.createConversation(r.Context(), chatID, req.Model, now); err != nil {
		s.releaseActiveRun(chatID, run)
		cancel()
		writeAPIError(w, http.StatusInternalServerError, "failed to create chat session")
		return
	}

	if lastUser := lastUserMessage(req.Messages); lastUser != "" {
		if err := s.chatStore.appendUserMessage(r.Context(), chatID, lastUser, now); err != nil {
			s.releaseActiveRun(chatID, run)
			cancel()
			writeAPIError(w, http.StatusInternalServerError, "failed to persist user message")
			return
		}
		if err := s.appendChatEventRecord(chatID, chatEventRequest{
			EventType: "message_sent",
			Role:      "user",
			Content:   lastUser,
			Timestamp: now,
		}); err != nil {
			s.releaseActiveRun(chatID, run)
			cancel()
			writeAPIError(w, http.StatusInternalServerError, "failed to persist chat event")
			return
		}
	}

	assistantSequence, err := s.chatStore.createRun(r.Context(), run.snapshot())
	if err != nil {
		s.releaseActiveRun(chatID, run)
		cancel()
		writeAPIError(w, http.StatusInternalServerError, "failed to persist chat run")
		return
	}
	run.mu.Lock()
	run.assistantMessageSequence = assistantSequence
	run.mu.Unlock()

	s.scheduler.OnNewTask(task)
	writeAPIJSON(w, http.StatusAccepted, run.snapshot())
}

func (s *UIApi) getCurrentChatRun(w http.ResponseWriter, r *http.Request, chatID string) {
	if run := s.getActiveRun(chatID); run != nil {
		writeAPIJSON(w, http.StatusOK, run.snapshot())
		return
	}

	persisted, err := s.chatStore.getRun(r.Context(), chatID)
	if err != nil {
		writeAPIError(w, http.StatusNotFound, "chat run not found")
		return
	}
	writeAPIJSON(w, http.StatusOK, persisted.Snapshot)
}

func (s *UIApi) streamCurrentChatRun(w http.ResponseWriter, r *http.Request, chatID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	run := s.getActiveRun(chatID)
	if run == nil {
		persisted, err := s.chatStore.getRun(r.Context(), chatID)
		if err != nil {
			writeAPIError(w, http.StatusNotFound, "chat run not found")
			return
		}
		if err := writeSSE(w, chatRunStreamEvent{
			Type:     "snapshot",
			Snapshot: persisted.Snapshot,
		}); err != nil {
			return
		}
		flusher.Flush()
		return
	}

	ch := make(chan chatRunStreamEvent, 16)
	run.addSubscriber(ch)
	defer run.removeSubscriber(ch)

	initial := chatRunStreamEvent{
		Type:     "snapshot",
		Snapshot: run.snapshot(),
	}
	if err := writeSSE(w, initial); err != nil {
		return
	}
	flusher.Flush()
	switch initial.Snapshot.Status {
	case chatRunStatusCompleted, chatRunStatusStopped, chatRunStatusError:
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			if err := writeSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
			switch event.Snapshot.Status {
			case chatRunStatusCompleted, chatRunStatusStopped, chatRunStatusError:
				return
			}
		}
	}
}

func (s *UIApi) stopCurrentChatRun(w http.ResponseWriter, r *http.Request, chatID string) {
	run := s.getActiveRun(chatID)
	if run == nil {
		persisted, err := s.chatStore.getRun(r.Context(), chatID)
		if err != nil {
			writeAPIError(w, http.StatusNotFound, "chat run not found")
			return
		}
		switch persisted.Snapshot.Status {
		case chatRunStatusCompleted, chatRunStatusStopped, chatRunStatusError:
			writeAPIJSON(w, http.StatusOK, stopChatRunResponse{Status: "already_finished"})
		default:
			writeAPIJSON(w, http.StatusOK, stopChatRunResponse{Status: "already_finished"})
		}
		return
	}

	run.mu.Lock()
	cancel := run.cancel
	task := run.task
	status := run.status
	run.mu.Unlock()

	if status == chatRunStatusCompleted || status == chatRunStatusStopped || status == chatRunStatusError {
		writeAPIJSON(w, http.StatusOK, stopChatRunResponse{Status: "already_finished"})
		return
	}

	if cancel != nil {
		cancel()
	}
	if task != nil && s.scheduler != nil {
		s.scheduler.OnTaskCancelled(task)
	}
	run.markStopped()
	_ = s.appendChatEventRecord(chatID, chatEventRequest{
		EventType: "message_stopped",
		Role:      "assistant",
		Content:   run.snapshot().AssistantContent,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	s.releaseActiveRun(chatID, run)
	writeAPIJSON(w, http.StatusOK, stopChatRunResponse{Status: "stopped"})
}

func (s *UIApi) getActiveRun(chatID string) *chatRunState {
	s.runLock.Lock()
	run := s.activeRuns[chatID]
	s.runLock.Unlock()
	return run
}

func (s *UIApi) releaseActiveRun(chatID string, run *chatRunState) {
	s.runLock.Lock()
	defer s.runLock.Unlock()
	if s.activeRuns[chatID] == run {
		delete(s.activeRuns, chatID)
	}
}

func (s *UIApi) finishRunCompleted(run *chatRunState) {
	run.markCompleted()
	_ = s.appendChatEventRecord(run.chatID, chatEventRequest{
		EventType: "message_completed",
		Role:      "assistant",
		Content:   run.snapshot().AssistantContent,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	s.releaseActiveRun(run.chatID, run)
}

func (s *UIApi) finishRunStopped(run *chatRunState) {
	run.mu.Lock()
	status := run.status
	run.mu.Unlock()
	if status == chatRunStatusStopped {
		return
	}
	run.markStopped()
	_ = s.appendChatEventRecord(run.chatID, chatEventRequest{
		EventType: "message_stopped",
		Role:      "assistant",
		Content:   run.snapshot().AssistantContent,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	s.releaseActiveRun(run.chatID, run)
}

func (s *UIApi) finishRunErrored(run *chatRunState, err error) {
	run.markErrored(err.Error())
	_ = s.appendChatEventRecord(run.chatID, chatEventRequest{
		EventType: "stream_error",
		Error:     err.Error(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	s.releaseActiveRun(run.chatID, run)
}

func writeSSE(w http.ResponseWriter, event chatRunStreamEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		w,
		"id: %d\nretry: %d\ndata: %s\n\n",
		event.Snapshot.Sequence,
		chatRunRetryMillis,
		payload,
	); err != nil {
		return err
	}
	return nil
}

func buildChatCompletionParams(
	model string,
	messages []chatRunMessage,
	thinkingEnabled *bool,
) (openai.ChatCompletionNewParams, error) {
	params := openai.ChatCompletionNewParams{
		Model:    model,
		Messages: make([]openai.ChatCompletionMessageParamUnion, 0, len(messages)+1),
	}
	if thinkingEnabled != nil && !*thinkingEnabled {
		params.Messages = append(params.Messages, openai.SystemMessage("/no_think"))
	}
	for _, message := range messages {
		switch message.Role {
		case "system":
			params.Messages = append(params.Messages, openai.SystemMessage(message.Content))
		case "user":
			params.Messages = append(params.Messages, openai.UserMessage(message.Content))
		case "assistant":
			params.Messages = append(params.Messages, openai.AssistantMessage(message.Content))
		default:
			return openai.ChatCompletionNewParams{}, fmt.Errorf("unsupported chat role %q", message.Role)
		}
	}
	return params, nil
}

func lastUserMessage(messages []chatRunMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			return messages[i].Content
		}
	}
	return ""
}

type chatRunTask struct {
	ctx    context.Context
	ui     *UIApi
	run    *chatRunState
	params openai.ChatCompletionNewParams
}

func (t *chatRunTask) Model() string {
	return t.run.model
}

func (t *chatRunTask) Fail(err error) {
	t.ui.finishRunErrored(t.run, err)
}

func (t *chatRunTask) OnInstanceAssigned(instance scheduling.Instance) {
	t.run.bindInstance(instance)
	t.run.emit("snapshot", "")
}

func (t *chatRunTask) PerformInference(instance scheduling.Instance) error {
	client := instance.GetOpenAIClient()
	stream := client.Chat.Completions.NewStreaming(t.ctx, t.params)
	hadContent := false
	for stream.Next() {
		chunk := stream.Current()
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta.Content
		if delta == "" {
			continue
		}
		hadContent = true
		t.run.markStreaming(delta)
	}

	if err := stream.Err(); err != nil {
		if errors.Is(t.ctx.Err(), context.Canceled) {
			t.ui.finishRunStopped(t.run)
			return nil
		}
		t.ui.finishRunErrored(t.run, err)
		return err
	}

	if errors.Is(t.ctx.Err(), context.Canceled) {
		t.ui.finishRunStopped(t.run)
		return nil
	}

	if !hadContent {
		t.run.emit("snapshot", "")
	}
	t.ui.finishRunCompleted(t.run)
	return nil
}

var _ scheduling.Task = (*chatRunTask)(nil)
var _ scheduling.InstanceAssignmentAware = (*chatRunTask)(nil)
