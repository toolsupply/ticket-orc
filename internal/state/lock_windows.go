//go:build windows

package state

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockFileExclusiveLock   = 2
	lockFileFailImmediately = 1
	errLockViolation        = 33
)

func lockFile(ctx context.Context, file *os.File, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var overlapped syscall.Overlapped
		result, _, callErr := procLockFileEx.Call(
			file.Fd(),
			uintptr(lockFileExclusiveLock|lockFileFailImmediately),
			0,
			1,
			0,
			uintptr(unsafe.Pointer(&overlapped)),
		)
		if result != 0 {
			return nil
		}
		err := windowsLastError(callErr)
		if errno, ok := err.(syscall.Errno); !ok || int(errno) != errLockViolation {
			return err
		}
		if time.Now().Add(lockPollInterval).After(deadline) {
			return ErrLockTimeout
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func unlockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	result, _, callErr := procUnlockFileEx.Call(
		file.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if result == 0 {
		return fmt.Errorf("UnlockFileEx failed: %w", windowsLastError(callErr))
	}
	return nil
}
