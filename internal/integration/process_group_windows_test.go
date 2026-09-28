//go:build windows

package integration

import (
	"os"
	"os/exec"
)

func configureInteractiveCommand(_ *exec.Cmd) {}

func signalInteractiveGroup(process *os.Process) error {
	return process.Signal(os.Interrupt)
}
