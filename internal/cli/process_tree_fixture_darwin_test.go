//go:build darwin

package cli

import (
	"os"
	"syscall"
	"testing"
)

func prepareOuterTreeReaper(t *testing.T) {}

func observeOuterTreeExit(t *testing.T, pid int) func() error {
	t.Helper()
	queue, err := syscall.Kqueue()
	if err != nil {
		t.Fatal(err)
	}
	change := syscall.Kevent_t{}
	syscall.SetKevent(&change, pid, syscall.EVFILT_PROC, syscall.EV_ADD|syscall.EV_ONESHOT)
	change.Fflags = syscall.NOTE_EXIT
	if _, err := syscall.Kevent(queue, []syscall.Kevent_t{change}, nil, nil); err != nil {
		_ = syscall.Close(queue)
		t.Fatal(err)
	}
	return func() error {
		defer syscall.Close(queue)
		for {
			events := make([]syscall.Kevent_t, 1)
			n, err := syscall.Kevent(queue, nil, events, nil)
			if err == syscall.EINTR {
				continue
			}
			if err != nil {
				return err
			}
			if n == 1 && events[0].Flags&syscall.EV_ERROR != 0 {
				return os.NewSyscallError("fixture exit observation", syscall.Errno(events[0].Data))
			}
			if n == 1 && events[0].Ident == uint64(pid) && events[0].Fflags&syscall.NOTE_EXIT != 0 {
				return nil
			}
		}
	}
}
