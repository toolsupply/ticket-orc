//go:build !windows

package cli

import (
	"fmt"
	"os"
	"syscall"
)

// Keep the kernel operation in one place so tests can prove that failed
// observation and released child identities never authorize group signals.
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
		return childContainment{}, fmt.Errorf("attach worker group: getpgid leader %d: %w", process.Pid, err)
	}
	if pgid <= 0 || pgid != process.Pid {
		return childContainment{}, fmt.Errorf("worker process does not own its process group")
	}
	return childContainment{pgid: pgid}, nil
}

func (containment childContainment) signal(process *os.Process, signal syscall.Signal) error {
	if process == nil || process.Pid <= 0 {
		return os.ErrProcessDone
	}
	// os.Process rejects a handle that cmd.Wait has already released. Checking
	// the numeric PID alone could instead find an unrelated, reused process.
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return fmt.Errorf("signal worker group: probe leader %d with signal 0: %w", process.Pid, err)
	}
	pgid, err := syscall.Getpgid(process.Pid)
	if err != nil {
		return fmt.Errorf("signal worker group: getpgid leader %d: %w", process.Pid, err)
	}
	if pgid <= 0 || pgid != containment.pgid || pgid != process.Pid {
		return fmt.Errorf("worker process group identity changed")
	}
	parentGroup, err := syscall.Getpgid(0)
	if err != nil {
		return fmt.Errorf("signal worker group: getpgid supervisor %d: %w", os.Getpid(), err)
	}
	if pgid == parentGroup {
		return fmt.Errorf("refusing to signal supervisor process group")
	}
	if err := signalProcessGroupID(pgid, signal); err != nil {
		return fmt.Errorf("signal worker group: kill group %d with signal %d: %w", pgid, signal, err)
	}
	return nil
}

func (containment childContainment) force(process *os.Process) error {
	return containment.signal(process, syscall.SIGKILL)
}

// The caller must own the unreaped leader, having observed its exit without
// consuming its status. Group signaling remains a request, never proof that
// descendants have stopped. Darwin disambiguates its zombie-only EPERM below;
// denied signals to live members remain errors.
func (containment childContainment) forceAfterLeaderExit(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return os.ErrProcessDone
	}
	// Darwin may omit an exited child from Getpgid. The successful exit
	// observer and our exclusive wait ownership pin the group instead. The
	// os.Process check still rejects a handle already released by cmd.Wait.
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return fmt.Errorf("post-exit worker cleanup: probe leader %d with signal 0: %w", process.Pid, err)
	}
	if containment.pgid <= 0 || containment.pgid != process.Pid {
		return fmt.Errorf("worker process group identity changed")
	}
	parentGroup, err := syscall.Getpgid(0)
	if err != nil {
		return fmt.Errorf("post-exit worker cleanup: getpgid supervisor %d: %w", os.Getpid(), err)
	}
	if containment.pgid == parentGroup {
		return fmt.Errorf("refusing to signal supervisor process group")
	}
	if err := forceExitedProcessGroup(containment.pgid); err != nil {
		return fmt.Errorf("post-exit worker cleanup: kill group %d with signal %d: %w", containment.pgid, syscall.SIGKILL, err)
	}
	return nil
}

func (containment childContainment) request(process *os.Process) error {
	return containment.signal(process, syscall.SIGINT)
}

func (containment childContainment) close() error { return nil }
