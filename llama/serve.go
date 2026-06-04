package llama

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

	// offloadLayers := 8
	// if args.OffloadLayers != nil {
	// 	offloadLayers = *args.OffloadLayers
	// }

	// -c 4096: cap context window so KV cache stays ~140 MB on phone instead of
	// the model's default (32K-64K ctx = 4+ GB KV cache that OOMs the phone).
	cliArgs = append(cliArgs, "-ngl", "99", "-c", "4096", "--rpc", nodes.String())

	if args.Alias != nil {
		cliArgs = append(cliArgs, "-n", *args.Alias)
	}

	cliArgs = append(cliArgs, "--port", fmt.Sprint(args.Port))

	// temporary: if model name starts with hf: use -hf to load huggingface model
	if strings.HasPrefix(args.Model, "hf:") {
		hfRef := args.Model[3:]
		if localPath := resolveHFCachePath(hfRef); localPath != "" {
			cliArgs = append(cliArgs, "--model", localPath)
		} else {
			cliArgs = append(cliArgs, "-hf", hfRef)
		}
	} else {
		cliArgs = append(cliArgs, "--model", args.Model)
	}

	return exec.CommandContext(ctx, c.Command[0], cliArgs...)
}

// resolveHFCachePath checks the HuggingFace hub cache for a locally downloaded
// model matching the given hfRef (format: "owner/repo:filename_filter").
// Returns the absolute path to the .gguf file if found, otherwise "".
func resolveHFCachePath(hfRef string) string {
	owner, rest, _ := strings.Cut(hfRef, "/")
	repo, filter, _ := strings.Cut(rest, ":")

	hfHome := os.Getenv("HF_HOME")
	if hfHome == "" {
		hfHome = filepath.Join(os.Getenv("HOME"), ".cache", "huggingface")
	}
	hubDir := filepath.Join(hfHome, "hub", "models--"+owner+"--"+repo, "snapshots")

	// Walk all snapshots, newest first isn't guaranteed — just return first match.
	snapshots, err := os.ReadDir(hubDir)
	if err != nil {
		return ""
	}
	for _, snap := range snapshots {
		if !snap.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(hubDir, snap.Name()))
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if strings.HasSuffix(name, ".gguf") && (filter == "" || strings.Contains(name, filter)) {
				return filepath.Join(hubDir, snap.Name(), name)
			}
		}
	}
	return ""
}
