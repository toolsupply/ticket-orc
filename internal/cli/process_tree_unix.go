//go:build !windows

package cli

import (
	"os"
	"syscall"
)

// signalProcessGroupID is a seam for proving that a post-reap containment
// path never signals a remembered numeric group. The real operation is kept
// in one place so callers cannot accidentally bypass the parent-group guard.
var signalProcessGroupID = func(pgid int, signal syscall.Signal) error {
	return syscall.Kill(-pgid, signal)
}

type childContainment struct {
	pgid int
}

func attachChildContainment(process *os.Process) (childContainment, error) {
	if process == nil {
		return childContainment{}, os.ErrProcessDone
	}
	pgid, err := syscall.Getpgid(process.Pid)
	if err != nil {
		return childContainment{}, err
	}
	return childContainment{pgid: pgid}, nil
}

func (containment childContainment) force(process *os.Process) error {
	if containment.pgid <= 0 {
		return forceChildStop(process)
	}
	if processAlive(process) {
		if parentGroup, err := syscall.Getpgid(0); err == nil && containment.pgid != parentGroup {
			return signalProcessGroupID(containment.pgid, syscall.SIGKILL)
		}
	}
	if processAlive(process) {
		return forceChildStop(process)
	}
	return os.ErrProcessDone
}

// forceAfterLeaderExit is used only after a platform-specific exit observer
// has established that the leader is still unreaped. The remembered group ID
// therefore cannot have been reused, even if the platform does not report a
// zombie as alive to kill(pid, 0).
func (containment childContainment) forceAfterLeaderExit(process *os.Process) error {
	if containment.pgid <= 0 {
		return forceChildStop(process)
	}
	if parentGroup, err := syscall.Getpgid(0); err == nil && containment.pgid != parentGroup {
		return signalProcessGroupID(containment.pgid, syscall.SIGKILL)
	}
	return os.ErrProcessDone
}

func (containment childContainment) request(process *os.Process) error {
	if containment.pgid <= 0 {
		return requestChildStop(process)
	}
	if processAlive(process) {
		if parentGroup, err := syscall.Getpgid(0); err == nil && containment.pgid != parentGroup {
			err := signalProcessGroupID(containment.pgid, syscall.SIGINT)
			if err == nil || err == syscall.ESRCH {
				return nil
			}
			return err
		}
	}
	if processAlive(process) {
		return requestChildStop(process)
	}
	return os.ErrProcessDone
}

func (containment childContainment) close() error { return nil }

func processAlive(process *os.Process) bool {
	if process == nil || process.Pid <= 0 {
		return false
	}
	return syscall.Kill(process.Pid, 0) == nil
}
