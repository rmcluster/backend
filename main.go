package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/coreos/go-systemd/v22/activation"

	"github.com/wk-y/rama-swap/llama"
	"github.com/wk-y/rama-swap/microservices/scheduling"
	"github.com/wk-y/rama-swap/server"
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
	mux := http.NewServeMux()

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

	ramalama := llama.Llama{
		Command: args.Ramalama,
	}
	tracker := tracker.NewTracker()
	tracker.AddRoutes(mux)
	factory := scheduling.NewInstanceFactory(&ramalama, 49170)
	loadingTracker := &scheduling.LoadingStatusTracker{}
	if setter, ok := factory.(scheduling.PhaseCallbackSetter); ok {
		setter.SetPhaseCallback(loadingTracker.OnPhaseUpdate)
	}
	scheduler := scheduling.NewPartitioningScheduler(factory, 1)
	tracker.Subscribe(schedulersubscriber.NewSchedulerSubscriber(scheduler))
	server := server.NewServer(ramalama, scheduler, loadingTracker)
	ui := uiapi.New(tracker, ramalama)
	ui.RegisterHandlers(mux)

	server.ModelNameMangler = func(s string) string {
		return strings.ReplaceAll(s, "/", "_")
	}

	// serve on all systemd sockets
	listeners, err := activation.Listeners()
	if err != nil {
		log.Fatalf("Failed checking for socket activation: %v", err)
	}

	for i, listener := range listeners {
		log.Printf("Listening on socket activation (%d)", i)
		mux := http.NewServeMux()
		server.HandleHttp(mux)

		go func() {
			defer listener.Close()

			err = http.Serve(listener, mux)

			log.Fatalf("Failed to serve: %v", err)
		}()
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
