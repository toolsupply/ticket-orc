//go:build darwin

package cli

import (
	"fmt"
	"os"
	"syscall"
)

// Observe the exact leader without reaping. Only NOTE_EXIT establishes exit;
// neither a failed liveness probe nor a kqueue error authorizes group signals.
func waitForChildExitBeforeReap(process *os.Process) error {
	if process == nil || process.Pid <= 0 {
		return os.ErrProcessDone
	}
	queue, err := syscall.Kqueue()
	if err != nil {
		return fmt.Errorf("observe leader %d exit: %w", process.Pid, os.NewSyscallError("kqueue child exit", err))
	}
	defer syscall.Close(queue)
	change := syscall.Kevent_t{}
	syscall.SetKevent(&change, process.Pid, syscall.EVFILT_PROC, syscall.EV_ADD|syscall.EV_ONESHOT)
	change.Fflags = syscall.NOTE_EXIT
	for {
		_, err = syscall.Kevent(queue, []syscall.Kevent_t{change}, nil, nil)
		if err != syscall.EINTR {
			break
		}
	}
	// Kqueue can no longer attach an already-exited child. This caller owns
	// the exact unreaped child and is its sole waiter, so ESRCH at registration
	// establishes leader exit, never descendant completion. EPERM and other
	// registration errors provide no such evidence.
	if err == syscall.ESRCH {
		return nil
	}
	if err != nil {
		return fmt.Errorf("observe leader %d exit: %w", process.Pid, os.NewSyscallError("kevent register child exit", err))
	}
	for {
		events := make([]syscall.Kevent_t, 1)
		n, err := syscall.Kevent(queue, nil, events, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("observe leader %d exit: %w", process.Pid, os.NewSyscallError("kevent wait child exit", err))
		}
		if n == 1 && events[0].Filter == syscall.EVFILT_PROC && events[0].Ident == uint64(process.Pid) {
			if events[0].Flags&syscall.EV_ERROR != 0 {
				return fmt.Errorf("observe leader %d exit: %w", process.Pid, os.NewSyscallError("kevent child exit event", syscall.Errno(events[0].Data)))
			}
			if events[0].Fflags&syscall.NOTE_EXIT != 0 {
				return nil
			}
		}
	}
}
