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
	Ip      string
	Port    int
	MaxSize int64
}

func (c Llama) ServeCommand(ctx context.Context, args ServeArgs) *exec.Cmd {
	cliArgs := slices.Concat(c.Command[1:], []string{})

	var nodes strings.Builder
	var rpcDevices strings.Builder
	sep := ""
	for i, node := range args.RpcNodes {
		fmt.Fprintf(&nodes, "%s%s:%d", sep, node.Ip, node.Port)
		fmt.Fprintf(&rpcDevices, "%sRPC%d", sep, i)
		sep = ","
	}

	// -c 4096: cap context window so KV cache stays ~140 MB on phone instead of
	// the model's default (32K-64K ctx = 4+ GB KV cache that OOMs the phone).
	cliArgs = append(cliArgs, "-ngl", "99", "-c", "4096")
	if len(args.RpcNodes) > 0 {
		cliArgs = append(cliArgs, "--rpc", nodes.String())
		//restrict to use RPC devices ONLY, don't use local PC. 
		cliArgs = append(cliArgs, "--device", rpcDevices.String())
	}

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
