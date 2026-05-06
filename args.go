package main

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type args struct {
	Gcasdb      *string
	Ramalama    []string
	Port        *int
	Host        *string
	IdleTimeout *time.Duration
}

// cli should include the name of the command itself
func parseArgs(cli []string) (a args, rest []string, err error) {
	commandName, cli := cli[0], cli[1:]

	for len(cli) > 0 {
		switch cli[0] {
		case "-h", "-help", "--help":
			printHelp(commandName)
			os.Exit(0)

		case "-ramalama":
			if a.Ramalama != nil {
				return args{}, nil, errors.New("--ramalama may only be passed at most once")
			}

			cli = cli[1:]
			end := slices.Index(cli, ";")

			if end < 0 {
				return args{}, nil, errors.New("expected terminating \";\" for --ramalama")
			}

			if end == 0 {
				return args{}, nil, errors.New("expected non-empty command after --ramalama")
			}

			a.Ramalama = cli[:end]
			cli = cli[end+1:]

		case "-port":
			if a.Port != nil {
				return args{}, nil, fmt.Errorf("%s may only be passed at most once", cli[0])
			}

			if len(cli) < 2 {
				return args{}, nil, fmt.Errorf("expected port number after %s", cli[0])
			}

			port, err := strconv.Atoi(cli[1])
			if err != nil {
				return args{}, nil, fmt.Errorf("invalid port number after %s: %v", cli[0], err)
			}

			a.Port = &port

			cli = cli[2:]

		case "-host":
			if a.Host != nil {
				return args{}, nil, fmt.Errorf("%s may only be passed at most once", cli[0])
			}

			if len(cli) < 2 {
				return args{}, nil, fmt.Errorf("expected host after %s", cli[0])
			}

			a.Host = &cli[1]

			cli = cli[2:]

		case "-idle-timeout":
			if a.IdleTimeout != nil {
				return args{}, nil, fmt.Errorf("%s may only be passed at most once", cli[0])
			}

			if len(cli) < 2 {
				return args{}, nil, fmt.Errorf("expected duration after %s", cli[0])
			}

			timeout, err := time.ParseDuration(cli[1])
			if err != nil {
				return args{}, nil, fmt.Errorf("invalid duration %v: %w", cli[1], err)
			}
			a.IdleTimeout = &timeout

			cli = cli[2:]

		case "-gcasdb":
			if a.Gcasdb != nil {
				return args{}, nil, fmt.Errorf("%s may only be passed at most once", cli[0])
			}

			if len(cli) < 2 {
				return args{}, nil, fmt.Errorf("expected path to gcas database after %s", cli[0])
			}

			a.Gcasdb = &cli[1]

			cli = cli[2:]

		case "--":
			rest = append(rest, cli...)
			return a, rest, nil

		default:
			if strings.HasPrefix(cli[0], "-") {
				return args{}, nil, fmt.Errorf("unrecognized flag %s. Use -help to list flags.", cli[0])
			}

			rest = append(rest, cli[0])

			cli = cli[1:]
		}
	}

	return a, rest, nil
}

func printHelp(commandName string) {
	fmt.Printf("Usage: %s [OPTION]...\n\n", commandName)
	fmt.Println(help)
}

//go:embed HELP.txt
var help string
