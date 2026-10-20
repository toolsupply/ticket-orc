// Package executionlog follows private managed execution logs for live viewers.
package executionlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	// PollInterval is the live viewer polling cadence.
	PollInterval              = 200 * time.Millisecond
	workerWatchRescanInterval = 5 * time.Second
	workerWatchMaxWorkers     = 256
	workerWatchReadLimit      = 1 << 20
	workerWatchDirBatch       = 128
	workerWatchPendingLimit   = 4096
)

type workerLogKey struct {
	started   time.Time
	collision int
}

type workerLogExecution struct {
	name string
	key  workerLogKey
}

type workerLogReader struct {
	file           *os.File
	path           string
	worker         Worker
	ticket         string
	execution      string
	offset         int64
	framer         harness.LineFramer
	discardPartial bool
	emit           func(Observation) error
	now            func() time.Time
}

// Follower owns the open readers and bounded pending inventory for one subscription.
type Follower struct {
	logDir     string
	workers    map[string]Worker
	latest     map[string]workerLogKey
	readers    map[string]*workerLogReader
	pending    map[string][]workerLogExecution
	lastMod    time.Time
	lastRescan time.Time
	emit       func(Observation) error
	closed     bool
}

// Worker is an authoritative runtime catalog entry. Names, role, harness and
// repository identity are fixed for the lifetime of this subscription.
type Worker struct {
	Name         string
	Role         string
	Harness      string
	RepositoryID string
}

// Observation identifies a semantic record without exposing filesystem paths.
type Observation struct {
	Worker       string            `json:"worker"`
	RepositoryID string            `json:"repository_id"`
	Harness      string            `json:"harness"`
	TicketID     string            `json:"ticket_id"`
	ExecutionID  string            `json:"execution_id"`
	ObservedAt   time.Time         `json:"observed_at"`
	Record       harness.LogRecord `json:"record"`
}

// New validates the entire authoritative managed-worker catalog before narrowing
// it to selected worker names and baselining their existing logs at EOF. Callers
// must pass the full inventory, even for worker- or repository-filtered viewers,
// because an excluded worker can have an ambiguous filename identity. A nil
// selection follows all workers; an empty non-nil selection follows none.
// emit runs synchronously; its errors stop the follower and close all files.
// The caller serializes Poll, Run and Close. No runtime locks or Ticket calls
// are needed while following this subscription snapshot.
func New(localDir string, catalog []Worker, selected []string, emit func(Observation) error) (*Follower, error) {
	workers, err := catalogWorkers(catalog)
	if err != nil {
		return nil, err
	}
	if selected != nil {
		subscription := make(map[string]Worker, len(selected))
		for _, name := range selected {
			worker, ok := workers[name]
			if !ok {
				return nil, fmt.Errorf("unknown worker %q", name)
			}
			subscription[name] = worker
		}
		workers = subscription
	}
	if emit == nil {
		return nil, fmt.Errorf("worker log observer is required")
	}
	watch := &Follower{logDir: filepath.Join(localDir, "logs"), workers: workers, latest: make(map[string]workerLogKey), readers: make(map[string]*workerLogReader), pending: make(map[string][]workerLogExecution), emit: emit}
	if err := watch.scan(true); err != nil {
		watch.Close()
		return nil, err
	}
	return watch, nil
}

// ValidateCatalog checks the entire managed inventory without file IO. A newly
// ambiguous identity must invalidate even a filtered live subscription.
func ValidateCatalog(catalog []Worker) error {
	_, err := catalogWorkers(catalog)
	return err
}

func catalogWorkers(catalog []Worker) (map[string]Worker, error) {
	if len(catalog) > workerWatchMaxWorkers {
		return nil, fmt.Errorf("worker watch supports at most %d workers", workerWatchMaxWorkers)
	}
	workers := make(map[string]Worker, len(catalog))
	prefixes := make(map[string]bool, len(catalog))
	for _, worker := range catalog {
		if !validComponent(worker.Name) || !validComponent(worker.Role) {
			return nil, fmt.Errorf("worker log catalog has invalid name or role: %q", worker.Name)
		}
		switch worker.Harness {
		case "codex", "claude", "pi":
		default:
			return nil, fmt.Errorf("worker %q has unsupported harness %q", worker.Name, worker.Harness)
		}
		if !ticketclient.ValidRepositoryID(worker.RepositoryID) {
			return nil, fmt.Errorf("worker %q requires authoritative repository identity", worker.Name)
		}
		if _, exists := workers[worker.Name]; exists {
			return nil, fmt.Errorf("duplicate worker %q in log catalog", worker.Name)
		}
		prefix := worker.logPrefix()
		if prefixes[prefix] {
			return nil, fmt.Errorf("ambiguous execution-log identity for worker %q", worker.Name)
		}
		prefixes[prefix] = true
		workers[worker.Name] = worker
	}
	return workers, nil
}

func (worker Worker) logPrefix() string {
	return worker.Role + "." + worker.Harness + "." + worker.Name + "."
}

func validComponent(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Close releases all readers, including after a failed poll. It is idempotent.
func (watch *Follower) Close() {
	watch.closed = true
	clear(watch.pending)
	for name, reader := range watch.readers {
		_ = reader.file.Close()
		delete(watch.readers, name)
	}
}

// Poll performs bounded reads and discovers executions when the directory changes
// or the fallback interval expires. Any failure permanently closes the follower.
func (watch *Follower) Poll(now time.Time) (err error) {
	if watch.closed {
		return fmt.Errorf("worker log follower is closed")
	}
	defer func() {
		if err != nil {
			watch.Close()
		}
	}()
	return watch.poll(now)
}

func (watch *Follower) poll(now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}
	for worker, reader := range watch.readers {
		if err := reader.readAvailable(); err != nil {
			return err
		}
		if len(watch.pending[worker]) > 0 {
			info, err := reader.file.Stat()
			if err != nil {
				return fmt.Errorf("inspect worker log %s: %w", filepath.Base(reader.path), err)
			}
			if reader.offset >= info.Size() {
				if err := watch.advance(worker); err != nil {
					return err
				}
			}
		}
	}
	info, err := os.Lstat(watch.logDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect worker log directory: %w", err)
	}
	if err := validateWorkerLogDirectory(info, watch.logDir); err != nil {
		return err
	}
	if info.ModTime().After(watch.lastMod) || now.Sub(watch.lastRescan) >= workerWatchRescanInterval {
		return watch.scan(false)
	}
	return nil
}

func (watch *Follower) scan(initial bool) error {
	info, err := os.Lstat(watch.logDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect worker log directory: %w", err)
	}
	if err := validateWorkerLogDirectory(info, watch.logDir); err != nil {
		return err
	}
	directory, err := os.Open(watch.logDir)
	if err != nil {
		return fmt.Errorf("open worker log directory: %w", err)
	}
	opened, openedErr := directory.Stat()
	current, currentErr := os.Lstat(watch.logDir)
	if openedErr != nil || currentErr != nil || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) || !os.SameFile(opened, current) {
		_ = directory.Close()
		return fmt.Errorf("worker log directory changed while opening")
	}
	watch.lastMod = info.ModTime()
	watch.lastRescan = time.Now()
	found := make(map[string][]workerLogExecution, len(watch.workers))
	newest := make(map[string]workerLogExecution, len(watch.workers))
	pendingCount := 0
	for _, queue := range watch.pending {
		pendingCount += len(queue)
	}
	for {
		entries, readErr := directory.ReadDir(workerWatchDirBatch)
		for _, entry := range entries {
			name := entry.Name()
			worker, key, ok := watch.workerForLog(name)
			if !ok || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, ".jsonl") {
				continue
			}
			path := filepath.Join(watch.logDir, name)
			fileInfo, statErr := os.Lstat(path)
			if statErr != nil {
				_ = directory.Close()
				return fmt.Errorf("inspect worker log %s: %w", name, statErr)
			}
			if fileInfo.Mode()&os.ModeSymlink != 0 || !fileInfo.Mode().IsRegular() {
				continue
			}
			execution := workerLogExecution{name: name, key: key}
			if initial {
				previous, ok := newest[worker]
				if !ok || previous.key.less(key) {
					newest[worker] = execution
				}
				continue
			}
			previous, ok := watch.latest[worker]
			if ok && !previous.less(key) {
				continue
			}
			pendingCount++
			if pendingCount > workerWatchPendingLimit {
				_ = directory.Close()
				return fmt.Errorf("worker watch found more than %d pending executions", workerWatchPendingLimit)
			}
			found[worker] = append(found[worker], execution)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = directory.Close()
			return fmt.Errorf("read worker log directory: %w", readErr)
		}
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close worker log directory: %w", err)
	}
	if initial {
		for worker, latest := range newest {
			reader, openErr := openWorkerLog(filepath.Join(watch.logDir, latest.name), watch.workers[worker], false)
			if openErr != nil {
				return openErr
			}
			reader.emit = watch.emit
			watch.readers[worker] = reader
			watch.latest[worker] = latest.key
		}
		return nil
	}
	for worker, executions := range found {
		sort.Slice(executions, func(i, j int) bool { return executions[i].key.less(executions[j].key) })
		for _, execution := range executions {
			watch.pending[worker] = append(watch.pending[worker], execution)
			watch.latest[worker] = execution.key
		}
		sort.Slice(watch.pending[worker], func(i, j int) bool { return watch.pending[worker][i].key.less(watch.pending[worker][j].key) })
		if watch.readers[worker] == nil && len(watch.pending[worker]) > 0 {
			if err := watch.advance(worker); err != nil {
				return err
			}
		}
	}
	return nil
}

func (key workerLogKey) less(other workerLogKey) bool {
	if key.started.Equal(other.started) {
		return key.collision < other.collision
	}
	return key.started.Before(other.started)
}

func (watch *Follower) advance(worker string) error {
	old := watch.readers[worker]
	if old != nil {
		if !old.discardPartial {
			if err := old.finishAtRollover(); err != nil {
				return err
			}
		}
		_ = old.file.Close()
		delete(watch.readers, worker)
	}
	queue := watch.pending[worker]
	if len(queue) == 0 {
		return nil
	}
	execution := queue[0]
	watch.pending[worker] = queue[1:]
	reader, err := openWorkerLog(filepath.Join(watch.logDir, execution.name), watch.workers[worker], true)
	if err != nil {
		return err
	}
	reader.emit = watch.emit
	watch.readers[worker] = reader
	return nil
}

func (reader *workerLogReader) finishAtRollover() error {
	err := reader.framer.Finish(reader.interpret)
	if errors.Is(err, harness.ErrEventTooLarge) {
		reader.framer = harness.LineFramer{}
		return reader.reportOversized()
	}
	if err != nil {
		return err
	}
	reader.framer = harness.LineFramer{}
	return nil
}

func (watch *Follower) workerForLog(name string) (string, workerLogKey, bool) {
	dot := strings.IndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 || !strings.HasSuffix(name, ".jsonl") {
		return "", workerLogKey{}, false
	}
	tail := name[dot+1:]
	if ticketclient.ValidateFullID(name[:dot]) != nil {
		return "", workerLogKey{}, false
	}
	for name, worker := range watch.workers {
		prefix := worker.logPrefix()
		if !strings.HasPrefix(tail, prefix) {
			continue
		}
		key, ok := workerLogKeyFromSuffix(tail[len(prefix):])
		if ok {
			return name, key, true
		}
	}
	return "", workerLogKey{}, false
}

func workerLogKeyFromSuffix(suffix string) (workerLogKey, bool) {
	base := strings.TrimSuffix(suffix, ".jsonl")
	const timestampLength = len("20060102T150405.000000000Z")
	if len(base) < timestampLength {
		return workerLogKey{}, false
	}
	stamp := base[:timestampLength]
	if len(base) == timestampLength {
		started, err := time.Parse("20060102T150405.000000000Z", stamp)
		if err == nil {
			return workerLogKey{started: started}, true
		}
		return workerLogKey{}, false
	}
	if base[timestampLength] != '-' {
		return workerLogKey{}, false
	}
	collisionSuffix := base[timestampLength+1:]
	collision, err := strconv.Atoi(collisionSuffix)
	if err != nil || collision < 1 || strconv.Itoa(collision) != collisionSuffix {
		return workerLogKey{}, false
	}
	started, err := time.Parse("20060102T150405.000000000Z", stamp)
	if err != nil {
		return workerLogKey{}, false
	}
	return workerLogKey{started: started, collision: collision}, true
}

func openWorkerLog(path string, worker Worker, fromStart bool) (*workerLogReader, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect worker log %s: %w", filepath.Base(path), err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("worker log is not a regular file: %s", filepath.Base(path))
	}
	if err := validatePrivateFile(before); err != nil {
		return nil, fmt.Errorf("worker log is not private: %s", filepath.Base(path))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open worker log %s: %w", filepath.Base(path), err)
	}
	after, statErr := file.Stat()
	current, pathErr := os.Lstat(path)
	if statErr != nil || pathErr != nil || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, after) || !os.SameFile(after, current) || !after.Mode().IsRegular() || validatePrivateFile(after) != nil {
		_ = file.Close()
		return nil, fmt.Errorf("worker log changed while opening: %s", filepath.Base(path))
	}
	reader := &workerLogReader{file: file, path: path, worker: worker, now: time.Now, ticket: strings.SplitN(filepath.Base(path), ".", 2)[0], execution: filepath.Base(path)}
	if fromStart {
		return reader, nil
	}
	size := after.Size()
	reader.offset = size
	if size > 0 {
		var last [1]byte
		if _, err := file.ReadAt(last[:], size-1); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("inspect end of worker log %s: %w", filepath.Base(path), err)
		}
		reader.discardPartial = last[0] != '\n'
	}
	return reader, nil
}

func (reader *workerLogReader) readAvailable() error {
	buffer := make([]byte, 32*1024)
	remaining := workerWatchReadLimit
	for remaining > 0 {
		chunk := buffer
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		n, err := reader.file.ReadAt(chunk, reader.offset)
		if n > 0 {
			reader.offset += int64(n)
			remaining -= n
			data := buffer[:n]
			if reader.discardPartial {
				newline := bytes.IndexByte(data, '\n')
				if newline < 0 {
					continue
				}
				reader.discardPartial = false
				data = data[newline+1:]
				reader.framer = harness.LineFramer{}
			}
			if len(data) != 0 {
				if err := reader.consume(data); err != nil {
					return err
				}
			}
		}
		if errors.Is(err, io.EOF) || n == 0 {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read worker log %s: %w", filepath.Base(reader.path), err)
		}
	}
	return nil
}

func (reader *workerLogReader) consume(data []byte) error {
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			err := reader.framer.Write(data, reader.interpret)
			if errors.Is(err, harness.ErrEventTooLarge) {
				reader.framer = harness.LineFramer{}
				reader.discardPartial = true
				return reader.reportOversized()
			}
			return err
		}
		frame := data[:newline+1]
		err := reader.framer.Write(frame, reader.interpret)
		if errors.Is(err, harness.ErrEventTooLarge) {
			reader.framer = harness.LineFramer{}
			if reportErr := reader.reportOversized(); reportErr != nil {
				return reportErr
			}
		} else if err != nil {
			return err
		}
		data = data[newline+1:]
	}
	return nil
}

func (reader *workerLogReader) reportOversized() error {
	return reader.observe(harness.LogRecord{Kind: "diagnostic", Level: "warning", Code: "event_too_large", Text: fmt.Sprintf("event exceeds %d bytes", harness.MaxEventBytes)}, reader.now())
}

func (reader *workerLogReader) observe(record harness.LogRecord, at time.Time) error {
	return reader.emit(Observation{Worker: reader.worker.Name, RepositoryID: reader.worker.RepositoryID, Harness: reader.worker.Harness, TicketID: reader.ticket, ExecutionID: reader.execution, ObservedAt: at.UTC(), Record: record})
}

func (reader *workerLogReader) interpret(line []byte) error {
	var records []harness.LogRecord
	var err error
	switch reader.worker.Harness {
	case "codex":
		records, err = codex.InterpretEvent(line)
	case "claude":
		records, err = claude.InterpretEvent(line)
	case "pi":
		records, err = pi.InterpretEvent(line)
	default:
		return fmt.Errorf("unsupported harness %q", reader.worker.Harness)
	}
	if err != nil {
		records = []harness.LogRecord{{Kind: "diagnostic", Level: "warning", Code: "invalid_event", Text: err.Error()}}
	}
	observed := reader.now()
	for _, record := range records {
		if err := reader.observe(record, observed); err != nil {
			return err
		}
	}
	return nil
}

func validateWorkerLogDirectory(info os.FileInfo, path string) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !privateModeMatches(info, 0o700) {
		return fmt.Errorf("worker log directory is not private: %s", path)
	}
	return nil
}

// Run polls until cancellation or failure, closing all readers before returning.
func (watch *Follower) Run(ctx context.Context) error {
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()
	defer watch.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if err := watch.Poll(now); err != nil {
				return err
			}
		}
	}
}

func validatePrivateFile(info os.FileInfo) error {
	if !info.Mode().IsRegular() || !privateModeMatches(info, 0o600) {
		return fmt.Errorf("worker log is not private")
	}
	return nil
}
