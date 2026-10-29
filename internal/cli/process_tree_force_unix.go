//go:build !windows && !darwin

package cli

import "syscall"

func forceExitedProcessGroup(pgid int) error {
	return signalProcessGroupID(pgid, syscall.SIGKILL)
}
