package uiapi

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/tracker"
)

type UIApi struct {
	tracker *tracker.Tracker
	llama   llama.Llama
}

var (
	hfStoreOnce sync.Once
	hfStore     *hfMetadataStore
)

func New(tracker *tracker.Tracker, llama llama.Llama) *UIApi {
	initHFMetadataStoreFromEnv()
	return &UIApi{tracker: tracker, llama: llama}
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
