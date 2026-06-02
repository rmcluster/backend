package uiapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/openai/openai-go/v2"
	"github.com/rmcluster/backend/llama"
	"github.com/rmcluster/backend/server/scheduling"
)

type fakeScheduler struct {
	mu        sync.Mutex
	tasks     []scheduling.Task
	cancelled []scheduling.Task
}

func (f *fakeScheduler) OnNewTask(task scheduling.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = append(f.tasks, task)
}

func (f *fakeScheduler) OnTaskCancelled(task scheduling.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, task)
}

func (*fakeScheduler) OnNodeConnect(scheduling.Node)    {}
func (*fakeScheduler) OnNodeDisconnect(scheduling.Node) {}

type fakeAssignedInstance struct {
	model       string
	scopeID     string
	phase       string
	progress    float64
	layersOnRPC int
}

func (f *fakeAssignedInstance) Model() string                      { return f.model }
func (f *fakeAssignedInstance) LoadingStatusScopeID() string       { return f.scopeID }
func (*fakeAssignedInstance) GetOpenAIClient() openai.Client       { return openai.Client{} }
func (*fakeAssignedInstance) ReverseProxy() *httputil.ReverseProxy { return nil }
func (*fakeAssignedInstance) WaitReady() error                     { return nil }
func (*fakeAssignedInstance) Stop()                                {}
func (*fakeAssignedInstance) Kill()                                {}
func (*fakeAssignedInstance) AwaitTermination()                    {}
func (*fakeAssignedInstance) GetUsedNodes() []scheduling.Node      { return nil }
func (f *fakeAssignedInstance) GetLoadingStatus() (model, phase string, progress float64, layersOnRpc int) {
	return f.model, f.phase, f.progress, f.layersOnRPC
}

func newTestUIAPI(t *testing.T) (*UIApi, *http.ServeMux, *scheduling.LoadingStatusTracker, *fakeScheduler, func()) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "chat.db")
	db, err := OpenChatDB(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenChatDB: %v", err)
	}

	tracker := &scheduling.LoadingStatusTracker{}
	scheduler := &fakeScheduler{}
	api := New(nil, llama.Llama{}, scheduler, tracker, db)
	mux := http.NewServeMux()
	api.RegisterHandlers(mux)

	cleanup := func() {
		_ = db.Close()
	}
	return api, mux, tracker, scheduler, cleanup
}

func startRunRequestBody(chatID, model, content string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"` + content + `"}]}`
}

func decodeSnapshotResponse(t *testing.T, res *http.Response) chatRunSnapshot {
	t.Helper()
	defer res.Body.Close()
	var snapshot chatRunSnapshot
	if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	return snapshot
}

func performRequest(t *testing.T, mux *http.ServeMux, method, path, body string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

func TestPersistedRunSnapshotDoesNotPopulateActiveRuns(t *testing.T) {
	api, mux, _, _, cleanup := newTestUIAPI(t)
	defer cleanup()

	const chatID = "persisted-chat"
	if err := api.chatStore.createConversation(context.Background(), chatID, "model-a", "2026-06-01T00:00:00Z"); err != nil {
		t.Fatalf("createConversation: %v", err)
	}
	if _, err := api.chatStore.createRun(context.Background(), chatRunSnapshot{
		ChatID:           chatID,
		Model:            "model-a",
		Status:           chatRunStatusCompleted,
		AssistantContent: "done",
		StartedAt:        "2026-06-01T00:00:00Z",
		UpdatedAt:        "2026-06-01T00:00:01Z",
		Sequence:         3,
	}); err != nil {
		t.Fatalf("createRun: %v", err)
	}

	res := performRequest(t, mux, http.MethodGet, "/api/ui/chats/"+chatID+"/runs/current", "")
	snapshot := decodeSnapshotResponse(t, res)
	if snapshot.Status != chatRunStatusCompleted || snapshot.AssistantContent != "done" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if got := len(api.activeRuns); got != 0 {
		t.Fatalf("expected no active runs after persisted lookup, got %d", got)
	}

	streamRes := performRequest(t, mux, http.MethodGet, "/api/ui/chats/"+chatID+"/runs/current/stream", "")
	defer streamRes.Body.Close()
	bodyBytes, err := io.ReadAll(streamRes.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	text := string(bodyBytes)
	if !strings.Contains(text, `"type":"snapshot"`) || !strings.Contains(text, `"status":"completed"`) {
		t.Fatalf("unexpected stream body: %s", text)
	}
	if got := len(api.activeRuns); got != 0 {
		t.Fatalf("expected no active runs after persisted stream, got %d", got)
	}
}

func TestQueuedRunReportsWaitingUntilInstanceAssigned(t *testing.T) {
	_, mux, _, scheduler, cleanup := newTestUIAPI(t)
	defer cleanup()

	const (
		chatID = "queued-chat"
		model  = "model-a"
	)

	res := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs", startRunRequestBody(chatID, model, "hello there"))
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected start status: %d", res.StatusCode)
	}
	snapshot := decodeSnapshotResponse(t, res)
	if snapshot.Status != chatRunStatusWaiting {
		t.Fatalf("expected waiting snapshot before assignment, got %+v", snapshot)
	}

	currentRes := performRequest(t, mux, http.MethodGet, "/api/ui/chats/"+chatID+"/runs/current", "")
	current := decodeSnapshotResponse(t, currentRes)
	if current.Status != chatRunStatusWaiting {
		t.Fatalf("expected persisted waiting snapshot before assignment, got %+v", current)
	}

	scheduler.mu.Lock()
	if len(scheduler.tasks) != 1 {
		scheduler.mu.Unlock()
		t.Fatalf("expected 1 scheduled task, got %d", len(scheduler.tasks))
	}
	task := scheduler.tasks[0]
	scheduler.mu.Unlock()

	aware, ok := task.(scheduling.InstanceAssignmentAware)
	if !ok {
		t.Fatal("scheduled task does not implement InstanceAssignmentAware")
	}
	aware.OnInstanceAssigned(&fakeAssignedInstance{
		model:       model,
		scopeID:     "model-a@10001",
		phase:       scheduling.PhaseStarting,
		progress:    0,
		layersOnRPC: 0,
	})

	assignedRes := performRequest(t, mux, http.MethodGet, "/api/ui/chats/"+chatID+"/runs/current", "")
	assigned := decodeSnapshotResponse(t, assignedRes)
	if assigned.Status != chatRunStatusStarting {
		t.Fatalf("expected starting snapshot after assignment, got %+v", assigned)
	}
	if assigned.LoadingPhase != scheduling.PhaseStarting {
		t.Fatalf("expected assigned run to inherit starting phase, got %+v", assigned)
	}
}

func TestConcurrentStartSameChatOnlyAcceptsOneRun(t *testing.T) {
	api, mux, _, scheduler, cleanup := newTestUIAPI(t)
	defer cleanup()

	const (
		chatID = "shared-chat"
		model  = "model-a"
	)

	const attempts = 8
	var wg sync.WaitGroup
	statuses := make(chan int, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs", startRunRequestBody(chatID, model, "hello there"))
			defer res.Body.Close()
			statuses <- res.StatusCode
		}()
	}
	wg.Wait()
	close(statuses)

	var accepted, conflicted int
	for status := range statuses {
		switch status {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicted++
		default:
			t.Fatalf("unexpected status: %d", status)
		}
	}
	if accepted != 1 || conflicted != attempts-1 {
		t.Fatalf("expected 1 accepted and %d conflicts, got %d accepted and %d conflicts", attempts-1, accepted, conflicted)
	}

	conversation, err := api.chatStore.getConversation(context.Background(), chatID)
	if err != nil {
		t.Fatalf("getConversation: %v", err)
	}
	if len(conversation.Messages) != 1 {
		t.Fatalf("expected 1 persisted message, got %d", len(conversation.Messages))
	}
	if conversation.Messages[0].Role != "user" || conversation.Messages[0].Content != "hello there" {
		t.Fatalf("unexpected persisted message: %+v", conversation.Messages[0])
	}
	if len(conversation.Events) != 1 || conversation.Events[0].EventType != "message_sent" {
		t.Fatalf("unexpected events: %+v", conversation.Events)
	}
	if got := len(api.activeRuns); got != 1 {
		t.Fatalf("expected 1 active run, got %d", got)
	}

	scheduler.mu.Lock()
	taskCount := len(scheduler.tasks)
	scheduler.mu.Unlock()
	if taskCount != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", taskCount)
	}
}

func TestParallelSameModelLoadingUpdatesStayScopedToAssignedInstance(t *testing.T) {
	api, mux, tracker, _, cleanup := newTestUIAPI(t)
	defer cleanup()

	for _, chatID := range []string{"chat-a", "chat-b"} {
		res := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs", startRunRequestBody(chatID, "shared-model", "hello "+chatID))
		res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("unexpected start status for %s: %d", chatID, res.StatusCode)
		}
	}

	taskA := api.activeRuns["chat-a"].task
	taskB := api.activeRuns["chat-b"].task
	taskA.OnInstanceAssigned(&fakeAssignedInstance{
		model:       "shared-model",
		scopeID:     "shared-model@10001",
		phase:       scheduling.PhaseStarting,
		progress:    0,
		layersOnRPC: 0,
	})
	taskB.OnInstanceAssigned(&fakeAssignedInstance{
		model:       "shared-model",
		scopeID:     "shared-model@10002",
		phase:       scheduling.PhaseStarting,
		progress:    0,
		layersOnRPC: 0,
	})

	tracker.OnPhaseUpdate("shared-model@10001", "shared-model", "download", 0.25)
	tracker.OnLayersKnown("shared-model@10001", 7)

	resA := performRequest(t, mux, http.MethodGet, "/api/ui/chats/chat-a/runs/current", "")
	snapshotA := decodeSnapshotResponse(t, resA)
	if snapshotA.Status != chatRunStatusStarting ||
		snapshotA.LoadingPhase != "download" ||
		snapshotA.LoadingProgress != 0.25 ||
		snapshotA.LayersOnRPC != 7 {
		t.Fatalf("unexpected snapshot for chat-a: %+v", snapshotA)
	}

	resB := performRequest(t, mux, http.MethodGet, "/api/ui/chats/chat-b/runs/current", "")
	snapshotB := decodeSnapshotResponse(t, resB)
	if snapshotB.Status != chatRunStatusStarting ||
		snapshotB.LoadingPhase != scheduling.PhaseStarting ||
		snapshotB.LoadingProgress != 0 ||
		snapshotB.LayersOnRPC != 0 {
		t.Fatalf("unexpected snapshot for chat-b after chat-a update: %+v", snapshotB)
	}

	tracker.OnPhaseUpdate("shared-model@10002", "shared-model", "loading_model", 0)
	tracker.OnLayersKnown("shared-model@10002", 11)

	resA = performRequest(t, mux, http.MethodGet, "/api/ui/chats/chat-a/runs/current", "")
	snapshotA = decodeSnapshotResponse(t, resA)
	if snapshotA.LoadingPhase != "download" || snapshotA.LoadingProgress != 0.25 || snapshotA.LayersOnRPC != 7 {
		t.Fatalf("unexpected snapshot for chat-a after chat-b update: %+v", snapshotA)
	}

	resB = performRequest(t, mux, http.MethodGet, "/api/ui/chats/chat-b/runs/current", "")
	snapshotB = decodeSnapshotResponse(t, resB)
	if snapshotB.LoadingPhase != "loading_model" || snapshotB.LoadingProgress != 0 || snapshotB.LayersOnRPC != 11 {
		t.Fatalf("unexpected snapshot for chat-b after own update: %+v", snapshotB)
	}

	tracker.OnPhaseUpdate("shared-model@10001", "shared-model", scheduling.PhaseReady, 0)

	resA = performRequest(t, mux, http.MethodGet, "/api/ui/chats/chat-a/runs/current", "")
	snapshotA = decodeSnapshotResponse(t, resA)
	if snapshotA.LoadingPhase != "download" || snapshotA.LoadingProgress != 0.25 || snapshotA.LayersOnRPC != 7 {
		t.Fatalf("ready transition should not clear loading status before streaming: %+v", snapshotA)
	}

	if got := len(api.activeRuns); got != 2 {
		t.Fatalf("expected 2 active runs, got %d", got)
	}
}

func TestStopAndRestartNormalizationReleaseActiveRuns(t *testing.T) {
	api, mux, _, _, cleanup := newTestUIAPI(t)
	defer cleanup()

	const chatID = "stoppable-chat"
	res := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs", startRunRequestBody(chatID, "model-a", "stop me"))
	res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected start status: %d", res.StatusCode)
	}

	stopRes := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs/current/stop", "")
	stopRes.Body.Close()
	if stopRes.StatusCode != http.StatusOK {
		t.Fatalf("unexpected stop status: %d", stopRes.StatusCode)
	}
	if got := len(api.activeRuns); got != 0 {
		t.Fatalf("expected no active runs after stop, got %d", got)
	}

	currentRes := performRequest(t, mux, http.MethodGet, "/api/ui/chats/"+chatID+"/runs/current", "")
	snapshot := decodeSnapshotResponse(t, currentRes)
	if snapshot.Status != chatRunStatusStopped {
		t.Fatalf("expected stopped snapshot, got %+v", snapshot)
	}

	stopAgainRes := performRequest(t, mux, http.MethodPost, "/api/ui/chats/"+chatID+"/runs/current/stop", "")
	defer stopAgainRes.Body.Close()
	var stopPayload stopChatRunResponse
	if err := json.NewDecoder(stopAgainRes.Body).Decode(&stopPayload); err != nil {
		t.Fatalf("decode second stop: %v", err)
	}
	if stopPayload.Status != "already_finished" {
		t.Fatalf("expected already_finished on second stop, got %+v", stopPayload)
	}
}

func TestRestartNormalizesInterruptedRunsToPersistedError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chat.db")

	db, err := OpenChatDB(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenChatDB initial: %v", err)
	}
	tracker := &scheduling.LoadingStatusTracker{}
	scheduler := &fakeScheduler{}
	api := New(nil, llama.Llama{}, scheduler, tracker, db)
	mux := http.NewServeMux()
	api.RegisterHandlers(mux)

	res := performRequest(t, mux, http.MethodPost, "/api/ui/chats/restart-chat/runs", startRunRequestBody("restart-chat", "model-a", "resume me"))
	res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected start status: %d", res.StatusCode)
	}

	_ = db.Close()

	db2, err := OpenChatDB(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenChatDB restart: %v", err)
	}
	defer db2.Close()

	api2 := New(nil, llama.Llama{}, scheduler, tracker, db2)
	mux2 := http.NewServeMux()
	api2.RegisterHandlers(mux2)

	if got := len(api2.activeRuns); got != 0 {
		t.Fatalf("expected no active runs after restart, got %d", got)
	}

	currentRes := performRequest(t, mux2, http.MethodGet, "/api/ui/chats/restart-chat/runs/current", "")
	snapshot := decodeSnapshotResponse(t, currentRes)
	if snapshot.Status != chatRunStatusError {
		t.Fatalf("expected error snapshot after restart normalization, got %+v", snapshot)
	}
	if !strings.Contains(snapshot.Error, "server restart") {
		t.Fatalf("expected restart error message, got %+v", snapshot)
	}
}
