package harness

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RawLog owns one private JSONL stream and its file lifecycle. Provider event
// interpretation stays in the adapter packages.
type RawLog struct {
	file     *os.File
	path     string
	provider string
	err      error
	closeErr error
	closed   bool
}

// OpenRawLog creates a collision-safe, private log file beneath stateDir.
func OpenRawLog(provider, stateDir, ticket, role string, now time.Time, components ...string) (*RawLog, error) {
	if err := validateLogComponent(provider, "ticket", ticket); err != nil {
		return nil, err
	}
	if err := validateLogComponent(provider, "role", role); err != nil {
		return nil, err
	}
	if strings.TrimSpace(stateDir) == "" {
		return nil, fmt.Errorf("%s state directory must not be empty", provider)
	}
	if err := ensurePrivateLogDir(provider, stateDir); err != nil {
		return nil, err
	}
	logDir := filepath.Join(stateDir, "logs")
	if err := ensurePrivateLogDir(provider, logDir); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	parts := []string{ticket, role}
	for _, component := range components {
		if component == "" {
			continue
		}
		if err := validateLogComponent(provider, "log component", component); err != nil {
			return nil, err
		}
		parts = append(parts, component)
	}
	parts = append(parts, now.UTC().Format("20060102T150405.000000000Z"))
	base := strings.Join(parts, ".")
	for attempt := 0; attempt < 100; attempt++ {
		name := base + ".jsonl"
		if attempt > 0 {
			name = fmt.Sprintf("%s-%d.jsonl", base, attempt)
		}
		path := filepath.Join(logDir, name)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("create %s raw log %s: %w", provider, path, err)
		}
		if err := secureLogFile(file, path); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("secure %s raw log %s: %w", provider, path, err)
		}
		return &RawLog{file: file, path: path, provider: provider}, nil
	}
	return nil, fmt.Errorf("create %s raw log: filename collisions exceeded retry budget", provider)
}

// Path returns the on-disk path for this log.
func (l *RawLog) Path() string { return l.path }

// Write appends raw bytes and preserves provider context for file errors.
func (l *RawLog) Write(data []byte) (int, error) {
	if l.closed {
		return 0, fmt.Errorf("%s execution output is closed", l.provider)
	}
	if l.err != nil {
		return 0, l.err
	}
	written, err := l.file.Write(data)
	if err != nil {
		l.err = fmt.Errorf("write %s raw log %s: %w", l.provider, l.path, err)
	} else if written != len(data) {
		l.err = fmt.Errorf("write %s raw log %s: %w", l.provider, l.path, io.ErrShortWrite)
	}
	return written, l.err
}

// Close syncs and closes the raw log. Repeated calls return the same result.
func (l *RawLog) Close() error {
	if l.closed {
		return l.closeErr
	}
	l.closed = true
	if err := l.file.Sync(); err != nil {
		l.closeErr = errors.Join(l.closeErr, fmt.Errorf("sync %s raw log %s: %w", l.provider, l.path, err))
	}
	if err := l.file.Close(); err != nil {
		l.closeErr = errors.Join(l.closeErr, fmt.Errorf("close %s raw log %s: %w", l.provider, l.path, err))
	}
	l.err = errors.Join(l.err, l.closeErr)
	return l.closeErr
}

func ensurePrivateLogDir(provider, path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create %s log directory %s: %w", provider, path, err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect %s log directory %s: %w", provider, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%s log directory must be a real directory: %s", provider, path)
	}
	if err := secureLogDirectory(path); err != nil {
		return fmt.Errorf("secure %s log directory %s: %w", provider, path, err)
	}
	info, err = os.Lstat(path)
	if err != nil {
		return fmt.Errorf("verify %s log directory %s: %w", provider, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !logDirectoryModeMatches(info) {
		return fmt.Errorf("%s log directory is not private: %s", provider, path)
	}
	return nil
}

func validateLogComponent(provider, name, value string) error {
	if value == "" || value == "." || value == ".." {
		return fmt.Errorf("%s log %s is invalid", provider, name)
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return fmt.Errorf("%s log %s contains an unsafe character", provider, name)
	}
	return nil
}
