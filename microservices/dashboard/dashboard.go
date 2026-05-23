package dashboard

import (
	"net/http"
	"time"

	"github.com/wk-y/rama-swap/microservices"
	"github.com/wk-y/rama-swap/tracker"
)

type Dashboard struct {
	tracker *tracker.Tracker
}

func NewDashboard(tracker *tracker.Tracker) *Dashboard {
	return &Dashboard{tracker: tracker}
}

func (d *Dashboard) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodOptions {
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Refresh", "5")
	clients := d.tracker.GetServers()
	t := time.Now()
	templDashboard(clients, t).Render(r.Context(), w)
}

// RegisterHandlers implements [microservices.Microservice].
func (d *Dashboard) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/dashboard", d.HandleDashboard)
}

var _ microservices.Microservice = (*Dashboard)(nil)
