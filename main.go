package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

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

func allowedCORSOrigins() map[string]struct{} {
	origins := make(map[string]struct{})
	for _, origin := range strings.Split(os.Getenv("CORS_ALLOWED_ORIGINS"), ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			origins[origin] = struct{}{}
		}
	}
	return origins
}

// corsMiddleware adds CORS headers for origins listed in CORS_ALLOWED_ORIGINS (comma-separated).
func corsMiddleware(handler http.Handler) http.Handler {
	allowed := allowedCORSOrigins()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if _, ok := allowed[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE, PUT")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
			} else if r.Method == http.MethodOptions {
				http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}
		}
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
	_ = godotenv.Load()

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
	router.Any("/", gin.WrapH(mux))

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

	factory := scheduling.NewInstanceFactory(&ramalama, 49170)
	loadingTracker := &scheduling.LoadingStatusTracker{}
	if setter, ok := factory.(scheduling.PhaseCallbackSetter); ok {
		setter.SetPhaseCallback(loadingTracker.OnPhaseUpdate)
		setter.SetLayersCallback(loadingTracker.OnLayersKnown)
	}
	scheduler := scheduling.NewPartitioningScheduler(factory, 3)
	tracker.DefaultTracker.Subscribe(schedulersubscriber.NewSchedulerSubscriber(scheduler))
	cas := gcas.NewGCAS(gcasdb)
	tracker.DefaultTracker.Subscribe(gcassubscriber.NewGCASSubscriber(cas))
	server := server.NewServer(ramalama, scheduler)
	dashboard := dashboard.NewDashboard(tracker.DefaultTracker)
	dashboard.RegisterHandlers(mux)
	homepage := homepage.NewHomepage()
	homepage.RegisterHandlers(mux)
	ui := uiapi.New(tracker.DefaultTracker, ramalama, loadingTracker)
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
	err = http.Serve(l, requestLogger(corsMiddleware(router)))

	log.Fatalf("Failed to serve: %v", err)
}
