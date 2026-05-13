package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/microservices/dashboard"
	"github.com/wk-y/rama-swap/microservices/homepage"
	"github.com/wk-y/rama-swap/microservices/scheduling"
	"github.com/wk-y/rama-swap/server"
	"github.com/wk-y/rama-swap/server/gcas"
	gcassubscriber "github.com/wk-y/rama-swap/server/gcas_subscriber"
	"github.com/wk-y/rama-swap/server/openapi"
	schedulersubscriber "github.com/wk-y/rama-swap/server/scheduler_subscriber"
	"github.com/wk-y/rama-swap/tracker"
	"github.com/wk-y/rama-swap/uiapi"
)

const EX_USAGE = 64

// corsMiddleware wraps an http.Handler to add CORS headers for development
func corsMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE, PUT")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// requestLogger logs every incoming request method and path.
func requestLogger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		handler.ServeHTTP(w, r)
	})
}

func main() {
	args, rest, err := parseArgs(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(EX_USAGE)
	}

	if len(rest) > 0 {
		fmt.Fprintf(os.Stderr, "%s: unexpected positional argument %v\n", os.Args[0], rest[0])
		os.Exit(EX_USAGE)
	}

	setDefaults(&args)

	// set up server
	router := openapi.NewRouter()
	mux := http.NewServeMux()

	// Forward everything gin doesn't own (/announce, /servers) to the mux.
	// A root-level /*path wildcard would conflict with gin's /announce and /servers
	// nodes, so we enumerate the prefixes used by the mux handlers instead.
	// NoRoute alone is insufficient: gin pre-sets 404 on the writer before calling
	// NoRoute handlers, which prevents the mux from writing a successful response.
	for _, prefix := range []string{"/api", "/v1", "/upstream", "/dashboard"} {
		router.Any(prefix+"/*path", gin.WrapH(mux))
		router.Any(prefix, gin.WrapH(mux))
	}

	ramalama := llama.Llama{
		Command: args.Ramalama,
	}

	if args.Gcasdb == nil {
		log.Fatalf("No GCAS database specified")
	}

	gcasdb, err := gcas.OpenDB(*args.Gcasdb)
	if err != nil {
		log.Fatalf("Failed to open GCAS database: %v", err)
	}
	defer func() {
		if err := gcasdb.Close(); err != nil {
			log.Printf("Failed to close GCAS database: %v", err)
		}
	}()

	tracker := tracker.NewTracker()
	tracker.AddRoutes(mux)
	factory := scheduling.NewInstanceFactory(&ramalama, 49170)
	loadingTracker := &scheduling.LoadingStatusTracker{}
	if setter, ok := factory.(scheduling.PhaseCallbackSetter); ok {
		setter.SetPhaseCallback(loadingTracker.OnPhaseUpdate)
	}
	scheduler := scheduling.NewPartitioningScheduler(factory, 3)
	tracker.Subscribe(schedulersubscriber.NewSchedulerSubscriber(scheduler))
	cas := gcas.NewGCAS(gcasdb)
	tracker.Subscribe(gcassubscriber.NewGCASSubscriber(cas))
	server := server.NewServer(ramalama, scheduler, loadingTracker)
	dashboard := dashboard.NewDashboard(tracker)
	dashboard.RegisterHandlers(mux)
	homepage := homepage.NewHomepage()
	homepage.RegisterHandlers(mux)
	ui := uiapi.New(tracker, ramalama)
	ui.RegisterHandlers(mux)

	server.ModelNameMangler = func(s string) string {
		return strings.ReplaceAll(s, "/", "_")
	}

	// serve on the configured host/port
	log.Printf("Listening on http://%s:%d\n", *args.Host, *args.Port)

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *args.Host, *args.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer l.Close()

	server.HandleHttp(mux)
	err = http.Serve(l, requestLogger(corsMiddleware(mux)))

	log.Fatalf("Failed to serve: %v", err)
}
