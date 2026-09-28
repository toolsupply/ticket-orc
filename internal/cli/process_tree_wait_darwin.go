//go:build darwin

package cli

import (
	"os"
	"syscall"
	"time"
)

// waitForChildExitBeforeReap observes the leader with kqueue. Darwin does not
// expose Linux's procfs zombie state, but EVFILT_PROC/NOTE_EXIT reports the
// leader's exit while the parent still owns the unreaped process. That keeps
// the remembered process-group identity verifiable while descendants are
// contained, before cmd.Wait releases the identity.
func waitForChildExitBeforeReap(process *os.Process) bool {
	if process == nil || process.Pid <= 0 {
		return false
	}
	kqueue, err := syscall.Kqueue()
	if err != nil {
		return false
	}
	defer syscall.Close(kqueue)
	change := syscall.Kevent_t{}
	syscall.SetKevent(&change, process.Pid, syscall.EVFILT_PROC, syscall.EV_ADD|syscall.EV_ONESHOT)
	change.Fflags = syscall.NOTE_EXIT
	if _, err := syscall.Kevent(kqueue, []syscall.Kevent_t{change}, nil, nil); err != nil {
		return false
	}
	for {
		events := make([]syscall.Kevent_t, 1)
		timeout := syscall.NsecToTimespec(int64(100 * time.Millisecond))
		n, err := syscall.Kevent(kqueue, nil, events, &timeout)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return false
		}
		if n == 1 && events[0].Filter == syscall.EVFILT_PROC && events[0].Fflags&syscall.NOTE_EXIT != 0 {
			return true
		}
		// The parent has not called Wait yet, so a vanished PID still belongs
		// to this child and its process-group identity cannot have been reused.
		if !processAlive(process) {
			return true
		}
	}
}
