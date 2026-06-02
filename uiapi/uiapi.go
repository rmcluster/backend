package uiapi

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rmcluster/backend/llama"
	"github.com/rmcluster/backend/server/scheduling"
	"github.com/rmcluster/backend/tracker"
)

type UIApi struct {
	tracker       *tracker.Tracker
	llama         llama.Llama
	scheduler     scheduling.Scheduler
	loadingStatus scheduling.LoadingStatusProvider // may be nil
	chatStore     *chatStore

	connectLock   sync.Mutex
	connectTokens map[string]time.Time
	chatLock      sync.Mutex
	chatSessions  map[string]chatSessionRecord
	runLock       sync.Mutex
	activeRuns    map[string]*chatRunState
}

var (
	hfStoreOnce sync.Once
	hfStore     *hfMetadataStore
)

func New(
	tracker *tracker.Tracker,
	llama llama.Llama,
	scheduler scheduling.Scheduler,
	loadingStatus scheduling.LoadingStatusProvider,
	chatDB *sql.DB,
) *UIApi {
	initHFMetadataStoreFromEnv()
	store := newChatStore(chatDB)
	_ = store.normalizeInterruptedRuns(context.Background())
	api := &UIApi{
		tracker:       tracker,
		llama:         llama,
		scheduler:     scheduler,
		loadingStatus: loadingStatus,
		chatStore:     store,
		connectTokens: make(map[string]time.Time),
		chatSessions:  make(map[string]chatSessionRecord),
		activeRuns:    make(map[string]*chatRunState),
	}
	if broadcaster, ok := loadingStatus.(scheduling.LoadingStatusBroadcaster); ok {
		broadcaster.Subscribe(api.onLoadingStatusUpdate)
	}
	return api
}

func initHFMetadataStoreFromEnv() {
	hfStoreOnce.Do(func() {
		path := strings.TrimSpace(os.Getenv("RMD_METADATA_DB_PATH"))
		if path == "" {
			path = defaultMetadataDBPath()
		}

		store, err := newHFMetadataStore(path)
		if err != nil {
			log.Printf("metadata cache disabled (db init failed): %v", err)
			return
		}

		hfStore = store
		log.Printf("metadata cache enabled at %s", path)
	})
}

func defaultMetadataDBPath() string {
	if cacheDir, err := os.UserCacheDir(); err == nil && cacheDir != "" {
		return filepath.Join(cacheDir, "rmd", "metadata.db")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".cache", "rmd", "metadata.db")
	}
	return filepath.Join(".", ".rmd", "metadata.db")
}

func (s *UIApi) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/ui", s.handleAPIRoot)
	mux.HandleFunc("/api/ui/models", s.handleAPIModels)
	mux.HandleFunc("/api/ui/models/search", s.handleAPISearchModels)
	mux.HandleFunc("/api/ui/models/hf", s.handleAPIAddHFModel)
	mux.HandleFunc("/api/ui/models/local", s.handleAPILocalModelUpload)
	mux.HandleFunc("/api/ui/dashboard", s.handleAPIDashboard)
	mux.HandleFunc("/api/ui/connect-info", s.handleAPIConnectInfo)
	mux.HandleFunc("/api/ui/chats", s.handleAPIStartChat)
	mux.HandleFunc("/api/ui/chats/", s.handleAPIChatRoute)
}

func (s *UIApi) listModelEntries() []modelEntry {
	baseModels, err := s.llama.GetModels()
	if err != nil {
		baseModels = nil
	}

	entries := builtinModelEntries(baseModels)
	if hfStore == nil {
		return entries
	}

	stored, err := hfStore.ListCustomModels()
	if err != nil {
		return entries
	}

	return mergeModelEntries(entries, customModelEntries(stored))
}
