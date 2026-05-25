//go:build !linux

package scheduler

import (
	"log"
	"os/exec"
)

func setProcAttrs(cmd *exec.Cmd) {
	log.Println("[WARN] Graceful shutdown of ramalama not supported for OS, switching may not work correctly")
}
