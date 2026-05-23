package homepage

import (
	"net/http"

	"github.com/wk-y/rama-swap/microservices"
)

const HomepageUrl = "/"

type Homepage struct{}

var _ microservices.Microservice = (*Homepage)(nil)

func NewHomepage() *Homepage {
	return &Homepage{}
}

func (h *Homepage) HandleHomepage(w http.ResponseWriter, r *http.Request) {
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

	homepageTempl().Render(r.Context(), w)
}

func (h *Homepage) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc(HomepageUrl+"{$}", h.HandleHomepage)
}
