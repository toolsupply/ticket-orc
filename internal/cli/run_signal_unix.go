//go:build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func supervisorSignalContext(parent context.Context, interactive bool) (context.Context, context.CancelFunc) {
	if interactive {
		return signal.NotifyContext(parent, syscall.SIGTERM)
	}
	return roleSignalContext(parent)
}

func consoleSignalChannel() (<-chan os.Signal, func()) {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	return interrupts, func() { signal.Stop(interrupts) }
}

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func requestChildStop(process *os.Process) error {
	return signalProcessGroup(process, syscall.SIGINT)
}

func forceChildStop(process *os.Process) error {
	return signalProcessGroup(process, syscall.SIGKILL)
}

func signalProcessGroup(process *os.Process, signal syscall.Signal) error {
	if process == nil {
		return os.ErrProcessDone
	}
	if pgid, err := syscall.Getpgid(process.Pid); err == nil && pgid > 0 {
		if parentGroup, parentErr := syscall.Getpgid(0); parentErr == nil && pgid == parentGroup {
			return process.Signal(signal)
		}
		return syscall.Kill(-pgid, signal)
	}
	return process.Signal(signal)
}
