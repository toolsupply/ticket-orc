//go:build !windows

package integration

import (
	"os"
	"os/exec"
	"syscall"
)

func configureInteractiveCommand(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func signalInteractiveGroup(process *os.Process) error {
	return syscall.Kill(-process.Pid, syscall.SIGINT)
}
