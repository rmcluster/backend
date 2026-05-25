//go:build linux

package scheduler

import (
	"os"
	"os/exec"
	"syscall"
)

func setProcAttrs(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		return cmd.Process.Signal(os.Interrupt)
	}
}
