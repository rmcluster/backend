package llama

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"strings"
)

type ServeArgs struct {
	Model         string // required
	Port          int
	Alias         *string
	RpcNodes      []RpcNode
	OffloadLayers *int
	TensorSplit   []float64
	NoHost        bool
}

type RpcNode struct {
	Ip   string
	Port int
}

func (c Llama) ServeCommand(ctx context.Context, args ServeArgs) *exec.Cmd {
	cliArgs := slices.Concat(c.Command[1:], []string{})

	var nodes strings.Builder
	sep := ""
	for _, node := range args.RpcNodes {
		fmt.Fprintf(&nodes, "%s%s:%d", sep, node.Ip, node.Port)
		sep = ","
	}

	offloadLayers := 0
	if args.OffloadLayers != nil {
		offloadLayers = *args.OffloadLayers
	}

	// -c 4096: cap context window so KV cache stays ~140 MB on phone instead of
	// the model's default (32K-64K ctx = 4+ GB KV cache that OOMs the phone).
	cliArgs = append(cliArgs, "-ngl", fmt.Sprint(offloadLayers), "-c", "4096", "--rpc", nodes.String())
	if offloadLayers > 0 {
		// Keep all layers on devices while dropping the default 1 GiB fit margin.
		// If the model still cannot fit, llama.cpp will fail instead of spilling.
		cliArgs = append(cliArgs, "--fit-target", "0")
	}
	if len(args.TensorSplit) > 0 {
		var split strings.Builder
		sep = ""
		for _, weight := range args.TensorSplit {
			fmt.Fprintf(&split, "%s%.4f", sep, weight)
			sep = ","
		}
		cliArgs = append(cliArgs, "--split-mode", "layer", "--tensor-split", split.String())
	}
	if args.NoHost {
		cliArgs = append(cliArgs, "--no-host")
	}

	if args.Alias != nil {
		cliArgs = append(cliArgs, "-n", *args.Alias)
	}

	cliArgs = append(cliArgs, "--port", fmt.Sprint(args.Port))

	// Prefer a fully cached local GGUF path when available. This keeps launches
	// deterministic and avoids relying on the runtime Hugging Face resolution
	// path once a model has already been downloaded.
	if strings.HasPrefix(args.Model, "hf:") {
		repo, filename, parsed := parseHFModelRef(args.Model)
		if cachedPath, ok, err := resolveCachedHFModelPath(args.Model); err == nil && ok {
			log.Printf("Using cached Hugging Face model for %s at %s", args.Model, cachedPath)
			cliArgs = append(cliArgs, "--model", cachedPath)
		} else if parsed {
			if err != nil {
				log.Printf("Failed to inspect Hugging Face cache for %s: %v", args.Model, err)
			}
			cliArgs = append(cliArgs, "--hf-repo", repo, "--hf-file", filename)
		} else {
			if err != nil {
				log.Printf("Failed to inspect Hugging Face cache for %s: %v", args.Model, err)
			}
			cliArgs = append(cliArgs, "-hf", args.Model[3:])
		}
	} else {
		cliArgs = append(cliArgs, "--model", args.Model)
	}

	return exec.CommandContext(ctx, c.Command[0], cliArgs...)
}
