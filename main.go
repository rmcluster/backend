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
)

const EX_USAGE = 64

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

	// wrap mux with router
	router.NoRoute(gin.WrapH(mux))

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

	scheduler := scheduling.NewPartitioningScheduler(scheduling.NewInstanceFactory(&ramalama, 49170), 3)
	tracker.DefaultTracker.Subscribe(schedulersubscriber.NewSchedulerSubscriber(scheduler))
	cas := gcas.NewGCAS(gcasdb)
	tracker.DefaultTracker.Subscribe(gcassubscriber.NewGCASSubscriber(cas))
	server := server.NewServer(ramalama, scheduler)
	dashboard := dashboard.NewDashboard(tracker.DefaultTracker)
	dashboard.RegisterHandlers(mux)
	homepage := homepage.NewHomepage()
	homepage.RegisterHandlers(mux)

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
	err = router.RunListener(l)

	log.Fatalf("Failed to serve: %v", err)
}
