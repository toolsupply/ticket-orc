//go:build linux

package cli

import (
	"os"
	"syscall"
	"unsafe"
)

// Keep the syscall boundary replaceable so tests can control interruption and
// permanent failures without racing a real child reaper or sending signals.
var waitidChildExit = childExitWithoutReap

// waitForChildExitBeforeReap observes only the leader. An unreaped Linux
// child remains a zombie and keeps its process group identity alive, so force
// can signal that group without polling descendants or a stale-PGID reuse
// window. The caller must invoke cmd.Wait only after this function returns.
func waitForChildExitBeforeReap(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return os.ErrProcessDone
	}
	for {
		err := waitidChildExit(process.Pid)
		if err == syscall.EINTR {
			continue
		}
		// ECHILD means ownership is lost. Other permanent failures also do
		// not establish an unreaped leader: never retry them or authorize
		// forceAfterLeaderExit. cmd.Wait still owns the final reap.
		return os.NewSyscallError("waitid", err)
	}
}

// childExitWithoutReap uses waitid(WNOWAIT) so the leader remains a zombie
// until cmd.Wait reaps it. This preserves the process-group identity while
// descendants are contained and works in PID namespaces where /proc PIDs do
// not match the parent's view.
func childExitWithoutReap(pid int) error {
	// Linux siginfo_t is 128 bytes. We need no architecture-dependent fields:
	// a successful blocking WEXITED wait itself proves exit. Use aligned storage
	// rather than decoding a byte of si_signo (which also depends on endianness).
	var info [16]uint64
	// Syscall6 enters/exits the runtime's syscall state, allowing other
	// goroutines to run while this thread blocks. RawSyscall6 would retain a P.
	_, _, errno := syscall.Syscall6(
		syscall.SYS_WAITID,
		uintptr(1), // P_PID
		uintptr(pid),
		uintptr(unsafe.Pointer(&info)),
		uintptr(syscall.WEXITED|syscall.WNOWAIT),
		0,
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}
