package main

import (
	"log"
	"os"
	"strings"
	"time"
)

const defaultPort = 4917
const defaultInstancePort = 49170
const defaultHost = "0.0.0.0"

func setDefaults(args *args) {

	// set default values for unspecified flags
	if args.Host == nil {
		host := defaultHost
		args.Host = &host
	}

	if args.Port == nil {
		port := defaultPort
		args.Port = &port
	}

	if args.InstancePort == nil {
		port := defaultInstancePort
		args.InstancePort = &port
	}

	if args.IdleTimeout == nil {
		timeout := time.Duration(0)
		args.IdleTimeout = &timeout
	}

	if args.Ramalama == nil {
		if env := strings.TrimSpace(os.Getenv("LLAMA_SERVER_PATH")); env != "" {
			args.Ramalama = []string{env}
		} else if env := os.Getenv("LLAMA_COMMAND"); env != "" {
			args.Ramalama = strings.Split(env, " ")
			if len(args.Ramalama) == 0 {
				log.Fatalln("LLAMA_COMMAND environment variable should not be all whitespace")
			}
		} else {
			args.Ramalama = []string{"llama-server"}
		}
	}
}
