package llama

import (
	"context"
	"strings"
	"testing"
)

func TestServeCommandUsesConfiguredOffloadLayers(t *testing.T) {
	llm := Llama{Command: []string{"llama-server"}}
	offloadLayers := 8

	cmd := llm.ServeCommand(context.Background(), ServeArgs{
		Model:         "/tmp/model.gguf",
		Port:          8080,
		RpcNodes:      []RpcNode{{Ip: "192.168.1.10", Port: 5000}},
		OffloadLayers: &offloadLayers,
	})

	args := cmd.Args
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-ngl" {
			if got := args[i+1]; got != "8" {
				t.Fatalf("ServeCommand -ngl = %s, want 8 (args=%v)", got, args)
			}
			return
		}
	}

	t.Fatalf("ServeCommand args missing -ngl: %v", args)
}

func TestServeCommandAddsTensorSplitAndNoHost(t *testing.T) {
	llm := Llama{Command: []string{"llama-server"}}
	offloadLayers := 99

	cmd := llm.ServeCommand(context.Background(), ServeArgs{
		Model:         "/tmp/model.gguf",
		Port:          8080,
		RpcNodes:      []RpcNode{{Ip: "192.168.1.10", Port: 5000}, {Ip: "192.168.1.11", Port: 5001}},
		OffloadLayers: &offloadLayers,
		TensorSplit:   []float64{0.75, 0.25},
		NoHost:        true,
	})

	args := cmd.Args
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--split-mode layer") {
		t.Fatalf("ServeCommand args missing split mode: %v", args)
	}
	if !strings.Contains(joined, "--tensor-split 0.7500,0.2500") {
		t.Fatalf("ServeCommand args missing tensor split: %v", args)
	}
	if !strings.Contains(joined, "--no-host") {
		t.Fatalf("ServeCommand args missing --no-host: %v", args)
	}
}
