package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
)

const consoleActivityLogMaxBytes = 1 << 20
const consoleActivityLogLockTimeout = 5 * time.Second

func appendConsoleWatchActivity(stateDir string, event daemon.Event) (resultErr error) {
	if stateDir == "" {
		return fmt.Errorf("activity log state directory is empty")
	}
	lock, err := state.AcquireLock(context.Background(), stateDir, consoleActivityLogLockTimeout)
	if err != nil {
		return fmt.Errorf("lock activity log state directory %s: %w", stateDir, err)
	}
	defer func() {
		if err := lock.Release(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("unlock activity log: %w", err)
		}
	}()
	return appendConsoleWatchActivityLocked(stateDir, event)
}

func appendConsoleWatchActivityLocked(stateDir string, event daemon.Event) error {
	logDir := filepath.Join(stateDir, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("create activity log directory %s: %w", logDir, err)
	}
	info, err := os.Lstat(logDir)
	if err != nil {
		return fmt.Errorf("inspect activity log directory %s: %w", logDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("activity log path is not a directory: %s", logDir)
	}
	if err := secureInstanceDirectory(logDir); err != nil {
		return fmt.Errorf("secure activity log directory %s: %w", logDir, err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode activity record: %w", err)
	}
	data = append(data, '\n')
	if len(data) > consoleActivityLogMaxBytes {
		return fmt.Errorf("activity record exceeds %d-byte log limit", consoleActivityLogMaxBytes)
	}
	path := filepath.Join(logDir, "activity.jsonl")
	before, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("inspect activity log %s: %w", path, err)
	}
	if err == nil && (before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular()) {
		return fmt.Errorf("activity log is not a regular file: %s", path)
	}
	// Writers hold the shared interprocess state lock, so the file offset can
	// provide append semantics here. Avoid O_APPEND: Windows handles opened for
	// append can reject Truncate during rollover.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open activity log %s: %w", path, err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return fmt.Errorf("inspect opened activity log %s: %w", path, statErr)
	}
	pathInfo, pathErr := os.Lstat(path)
	if pathErr != nil {
		_ = file.Close()
		return fmt.Errorf("verify activity log path %s: %w", path, pathErr)
	}
	if !info.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() || !os.SameFile(info, pathInfo) || (before != nil && !os.SameFile(before, info)) {
		_ = file.Close()
		return fmt.Errorf("activity log changed while opening: %s", path)
	}
	if err := secureInstanceFile(file, path, 0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure activity log %s: %w", path, err)
	}
	if info.Size()+int64(len(data)) > consoleActivityLogMaxBytes {
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			return fmt.Errorf("bound activity log %s: %w", path, err)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			_ = file.Close()
			return fmt.Errorf("seek bounded activity log %s: %w", path, err)
		}
	} else if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return fmt.Errorf("seek activity log %s: %w", path, err)
	}
	written, writeErr := file.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = fmt.Errorf("short write")
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("append activity log %s: %w", path, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close activity log %s: %w", path, closeErr)
	}
	return nil
}

func renderConsoleWatchActivity(out io.Writer, renderer *consoleWatchRenderer, stateDir string, event daemon.Event, at time.Time) {
	if err := appendConsoleWatchActivity(stateDir, event); err != nil {
		fmt.Fprintln(out, "activity log: failed to append watch event")
	}
	renderer.render(out, event, at)
}
