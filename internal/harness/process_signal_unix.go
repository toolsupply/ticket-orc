//go:build unix

package harness

import (
	"os"
	"syscall"
)

func processSignal(state *os.ProcessState) int {
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return int(status.Signal())
	}
	return 0
}
