//go:build windows

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
		return context.WithCancel(parent)
	}
	return roleSignalContext(parent)
}

func consoleSignalChannel() (<-chan os.Signal, func()) {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	return interrupts, func() { signal.Stop(interrupts) }
}

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// Windows does not provide a portable child interrupt signal. Kill is the
// first fallback request there; startRunChild assigns every process to a Job
// Object so force stops include descendants, and the supervisor still waits
// for every child to be reaped before releasing its daemon lock.
func requestChildStop(process *os.Process) error {
	return process.Kill()
}

func forceChildStop(process *os.Process) error {
	return process.Kill()
}
