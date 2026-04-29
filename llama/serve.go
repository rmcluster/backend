package llama

import (
	"context"
	"fmt"
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

	offloadLayers := 8
	if args.OffloadLayers != nil {
		offloadLayers = *args.OffloadLayers
	}

	// -c 4096: cap context window so KV cache stays ~140 MB on phone instead of
	// the model's default (32K-64K ctx = 4+ GB KV cache that OOMs the phone).
	cliArgs = append(cliArgs, "-ngl", fmt.Sprint(offloadLayers), "-c", "4096", "--rpc", nodes.String())

	if args.Alias != nil {
		cliArgs = append(cliArgs, "-n", *args.Alias)
	}

	cliArgs = append(cliArgs, "--port", fmt.Sprint(args.Port))

	// temporary: if model name starts with hf: use -hf to load huggingface model
	if strings.HasPrefix(args.Model, "hf:") {
		cliArgs = append(cliArgs, "-hf", args.Model[3:])
	} else {
		cliArgs = append(cliArgs, "--model", args.Model)
	}

	return exec.CommandContext(ctx, c.Command[0], cliArgs...)
}
