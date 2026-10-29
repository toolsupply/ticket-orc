//go:build linux

package cli

import (
	"os"
	"syscall"
	"testing"
)

func prepareOuterTreeReaper(t *testing.T) {
	t.Helper()
	// Only the isolated fixture owner adopts orphaned descendants. Production
	// remains the ordinary outer group, with no nested guardian or reaper.
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36 /* PR_SET_CHILD_SUBREAPER */, 1, 0, 0, 0, 0)
	if errno != 0 {
		t.Fatal(os.NewSyscallError("fixture subreaper", errno))
	}
}

func observeOuterTreeExit(t *testing.T, pid int) func() error {
	t.Helper()
	return func() error {
		for {
			_, err := syscall.Wait4(pid, nil, 0, nil)
			if err != syscall.EINTR {
				return os.NewSyscallError("reap fixture descendant", err)
			}
		}
	}
}
