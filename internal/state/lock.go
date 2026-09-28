package state

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const lockPollInterval = 10 * time.Millisecond

type fileLock struct {
	file *os.File
}

// Lock is an exclusive cross-process lock held until Release is called.
// Implementations use the platform's native advisory/mandatory file locking
// primitive while retaining one lock-file protocol across supported systems.
type Lock interface {
	Release() error
}

// AcquireLock acquires the state-directory lock for a bounded period. It is
// exposed for long-lived owners such as the run supervisor; ordinary state
// mutations should continue to use Store methods so each operation holds the
// lock only for its atomic read/modify/write transaction.
func AcquireLock(ctx context.Context, dir string, timeout time.Duration) (Lock, error) {
	return acquireLockWithTimeout(ctx, dir, timeout, false)
}

// TryAcquireLock acquires the state-directory lock without waiting when it is
// already held. It is for ownership decisions; ordinary state transactions
// should keep using their bounded AcquireLock path.
func TryAcquireLock(ctx context.Context, dir string) (Lock, error) {
	return acquireLockWithTimeout(ctx, dir, 0, true)
}

func acquireLockWithTimeout(ctx context.Context, dir string, timeout time.Duration, allowImmediate bool) (Lock, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("state directory must not be empty")
	}
	if ctx == nil {
		return nil, fmt.Errorf("state context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if timeout < 0 || (timeout == 0 && !allowImmediate) {
		return nil, fmt.Errorf("state lock timeout must be positive")
	}
	if err := prepareDir(dir); err != nil {
		return nil, err
	}
	return acquireLock(ctx, dir, timeout)
}

func acquireLock(ctx context.Context, dir string, timeout time.Duration) (*fileLock, error) {
	path := filepath.Join(dir, lockFileName)
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("state lock must be a regular file: %s", path)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect state lock %s: %w", path, err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open state lock %s: %w", path, err)
	}
	if err := securePrivateFile(file, path, 0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("secure state lock %s: %w", path, err)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("inspect opened state lock %s: %w", path, err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) {
		file.Close()
		if err != nil {
			return nil, fmt.Errorf("verify state lock path %s: %w", path, err)
		}
		return nil, fmt.Errorf("state lock changed while opening: %s", path)
	}
	if err := lockFile(ctx, file, timeout); err != nil {
		file.Close()
		return nil, fmt.Errorf("acquire state lock %s: %w", path, err)
	}
	return &fileLock{file: file}, nil
}

func (l *fileLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	path := l.file.Name()
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return fmt.Errorf("unlock state lock %s: %w", path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close state lock %s: %w", path, closeErr)
	}
	return nil
}
