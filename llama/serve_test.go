package llama

import (
	"context"
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
