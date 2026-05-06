package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/wk-y/rama-swap/cmd/linux-client/fscas"
	"github.com/wk-y/rama-swap/cmd/linux-client/openapi"
)

// time to wait after failed announcement
const retrySleep = time.Second

func main() {
	id := flag.String("id", "", "the id of the node")
	tracker := flag.String("tracker", "127.0.0.1:4917", "ip:port of the tracker")
	rpcPort := flag.Int("port", 1984, "port to run the RPC server on")
	casPath := flag.String("cas-path", "", "path to the CAS directory. If empty, CAS is disabled.")
	casPort := flag.Int("cas-port", 1985, "port to run the CAS server on")
	rpcCommand := flag.String("cmd", "rpc-server", "command to run the RPC server")
	flag.Parse()

	if *id == "" {
		log.Fatal("missing id")
	}

	args := []string{
		"--port", fmt.Sprint(*rpcPort),
	}
	args = append(args, flag.Args()...)

	// start RPC server
	cmd := exec.Command(*rpcCommand, args...)
	cmd.Stdin = nil
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		panic(err)
	}

	// print command
	log.Printf("Running command: %s %v\n", *rpcCommand, args)

	if *casPath != "" {
		// start CAS server
		cas := fscas.NewCAS(*casPath)
		go func() {
			log.Printf("Starting CAS server on %s", fmt.Sprintf("0.0.0.0:%d", *casPort))
			if err := openapi.NewRouter(cas).Run(fmt.Sprintf("0.0.0.0:%d", *casPort)); err != nil {
				log.Fatal(err)
			}
		}()
	}

	// start announcement loop
	go func() {

		query := make(url.Values)
		query.Add("id", *id)
		query.Add("port", fmt.Sprint(*rpcPort))

		if *casPath != "" {
			query.Add("cas-port", fmt.Sprint(*casPort))
		}

		announceUrl := url.URL{
			Scheme:   "http",
			Host:     *tracker,
			Path:     "/announce",
			RawQuery: query.Encode(),
		}

		for {
			// send announce request
			log.Printf("Announcing: %s", announceUrl.String())
			resp, err := http.Get(announceUrl.String())
			if err != nil {
				log.Printf("Failed to announce to tracker: %v\n", err)
				time.Sleep(retrySleep)
				continue
			}

			// parse response
			var response announcementResponse
			if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
				log.Printf("Failed to parse announcement response: %v\n", err)
				time.Sleep(retrySleep)
				continue
			}

			log.Printf("Announced to server, reannouncing in %v seconds\n", response.Interval)

			// wait for next announcement time
			time.Sleep(time.Duration(response.Interval * float64(time.Second)))
		}
	}()

	cmd.Wait()
}

type announcementResponse struct {
	Interval float64 `json:"interval"`
}
