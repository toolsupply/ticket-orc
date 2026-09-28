//go:build linux

package cli

import (
	"os"
	"syscall"
	"time"
	"unsafe"
)

// waitForChildExitBeforeReap observes only the leader. An unreaped Linux
// child remains a zombie and keeps its process group identity alive, so force
// can signal that group without polling descendants or a stale-PGID reuse
// window. The caller must invoke cmd.Wait only after this function returns.
func waitForChildExitBeforeReap(process *os.Process) bool {
	if process == nil || process.Pid <= 0 {
		return false
	}
	for {
		exited, err := childExitedWithoutReap(process.Pid)
		if err == syscall.ECHILD {
			// The child is no longer waitable by this parent. Let cmd.Wait
			// report the authoritative result instead of guessing at a
			// potentially reused process-group ID.
			return false
		}
		if err == nil && exited {
			return true
		}
		time.Sleep(time.Millisecond)
	}
}

// childExitedWithoutReap uses waitid(WNOWAIT) so the leader remains a zombie
// until cmd.Wait reaps it. This preserves the process-group identity while
// descendants are contained and works in PID namespaces where /proc PIDs do
// not match the parent's view.
func childExitedWithoutReap(pid int) (bool, error) {
	var info [128]byte
	_, _, errno := syscall.RawSyscall6(
		syscall.SYS_WAITID,
		uintptr(1), // P_PID
		uintptr(pid),
		uintptr(unsafe.Pointer(&info)),
		uintptr(syscall.WEXITED|syscall.WNOWAIT|syscall.WNOHANG),
		0,
		0,
	)
	if errno != 0 {
		return false, errno
	}
	return info[0] != 0, nil
}
