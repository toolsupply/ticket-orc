package ticketclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
)

// ChangeWatch is a Ticket watch stream that is ready against an authoritative
// initial baseline. It coalesces events because callers re-observe Ticket state
// instead of applying event payloads.
type ChangeWatcher interface {
	Check() error
	WaitForChange(context.Context) (bool, error)
	Close() error
}

type ChangeWatch struct {
	ctx      context.Context
	cancel   context.CancelFunc
	changed  chan bool
	ready    chan error
	done     chan struct{}
	finished error
	mu       sync.Mutex
	close    sync.Once
}

// StartChangeWatch starts an unfiltered repository watch and returns only
// after Ticket confirms the repository baseline is established. Work-log and
// handoff events are reported as non-material because pending acceptance
// explanations must not rearm an unchanged signoff sweep.
func (c *Client) StartChangeWatch(ctx context.Context, repositoryID string) (ChangeWatcher, error) {
	if ctx == nil {
		return nil, errors.New("ticket watch context must not be nil")
	}
	if !ValidRepositoryID(repositoryID) {
		return nil, errors.New("ticket watch repository ID is invalid")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	args := append([]string(nil), c.target.args()...)
	args = append(args, "watch", "-j", "--ready")
	command := exec.CommandContext(watchCtx, c.executable, args...)
	command.Dir = c.workingDir
	command.Env = append([]string(nil), c.command.Env...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open Ticket watch output: %w", err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start Ticket watch: %w", err)
	}
	watch := &ChangeWatch{ctx: watchCtx, cancel: cancel, changed: make(chan bool, 1), ready: make(chan error, 1), done: make(chan struct{})}
	go watch.read(command, stdout, repositoryID)
	select {
	case err := <-watch.ready:
		if err != nil {
			_ = watch.Close()
			return nil, err
		}
		select {
		case <-watch.done:
			err := watchStartupError(ctx, watch)
			_ = watch.Close()
			return nil, err
		default:
		}
		if err := ctx.Err(); err != nil {
			_ = watch.Close()
			return nil, err
		}
		return watch, nil
	case <-ctx.Done():
		_ = watch.Close()
		return nil, ctx.Err()
	case <-watch.done:
		err := watchStartupError(ctx, watch)
		_ = watch.Close()
		return nil, err
	}
}

func watchStartupError(ctx context.Context, watch *ChangeWatch) error {
	if err := watch.err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("Ticket watch exited before READY")
}

// WaitForChange waits for a repository event. The result is true when the
// event can justify re-observing acceptance work; the caller remains the
// authority for deciding whether the current frontier has materially changed.
func (watch *ChangeWatch) WaitForChange(ctx context.Context) (bool, error) {
	if watch == nil {
		return false, errors.New("ticket watch is nil")
	}
	if ctx == nil {
		return false, errors.New("ticket watch wait context must not be nil")
	}
	select {
	case material := <-watch.changed:
		return material, nil
	case <-watch.done:
		return false, watch.err()
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Check reports an unexpected watch termination without waiting.
func (watch *ChangeWatch) Check() error {
	if watch == nil {
		return errors.New("ticket watch is nil")
	}
	select {
	case <-watch.done:
		return watch.err()
	default:
		return nil
	}
}

// Close cancels and reaps the Ticket watch process.
func (watch *ChangeWatch) Close() error {
	if watch == nil {
		return nil
	}
	watch.close.Do(watch.cancel)
	<-watch.done
	return watch.err()
}

func (watch *ChangeWatch) read(command *exec.Cmd, stdout io.ReadCloser, repositoryID string) {
	ready := false
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var readErr error
	protocolFailure := false
	for scanner.Scan() {
		isReady, material, err := validateWatchRecord(scanner.Bytes(), repositoryID, ready)
		if err != nil {
			readErr = err
			protocolFailure = true
			break
		}
		if isReady {
			ready = true
			watch.ready <- nil
			continue
		}
		watch.signalChange(material)
	}
	if err := scanner.Err(); err != nil {
		readErr = fmt.Errorf("%w: read watch output: %v", ErrProtocol, err)
		protocolFailure = watch.ctx.Err() == nil
	} else if readErr == nil && !ready {
		if watch.ctx.Err() == nil {
			readErr = fmt.Errorf("%w: Ticket watch exited before READY", ErrProtocol)
			protocolFailure = true
		}
	} else if readErr == nil {
		if watch.ctx.Err() == nil {
			readErr = fmt.Errorf("Ticket watch exited unexpectedly")
			protocolFailure = true
		}
	}
	if readErr != nil {
		watch.cancel()
	}
	waitErr := command.Wait()
	_ = stdout.Close()
	if watch.ctx.Err() != nil && !protocolFailure {
		readErr = nil
		waitErr = nil
	}
	if readErr == nil && waitErr != nil {
		readErr = fmt.Errorf("Ticket watch process failed: %w", waitErr)
	}
	if !ready {
		watch.ready <- readErr
	}
	watch.mu.Lock()
	watch.finished = readErr
	watch.mu.Unlock()
	close(watch.done)
}

func (watch *ChangeWatch) signalChange(material bool) {
	select {
	case pending := <-watch.changed:
		material = material || pending
	default:
	}
	select {
	case watch.changed <- material:
	default:
	}
}

func (watch *ChangeWatch) err() error {
	watch.mu.Lock()
	defer watch.mu.Unlock()
	return watch.finished
}

func validateWatchRecord(line []byte, repositoryID string, ready bool) (bool, bool, error) {
	if err := jsonx.Validate(line); err != nil {
		return false, false, fmt.Errorf("%w: invalid Ticket watch record: %v", ErrProtocol, err)
	}
	var record struct {
		Type         json.RawMessage `json:"type"`
		RepositoryID string          `json:"repository_id"`
		Ticket       string          `json:"ticket"`
		Event        string          `json:"event"`
	}
	if err := json.Unmarshal(line, &record); err != nil {
		return false, false, fmt.Errorf("%w: decode Ticket watch record: %v", ErrProtocol, err)
	}
	if len(record.Type) != 0 {
		var recordType string
		if bytes.Equal(bytes.TrimSpace(record.Type), []byte("null")) || json.Unmarshal(record.Type, &recordType) != nil || recordType != "ready" {
			return false, false, fmt.Errorf("%w: Ticket watch record type is invalid", ErrProtocol)
		}
		if ready {
			return false, false, fmt.Errorf("%w: duplicate Ticket watch READY record", ErrProtocol)
		}
		if record.RepositoryID != repositoryID {
			return false, false, fmt.Errorf("%w: Ticket watch READY repository does not match", ErrProtocol)
		}
		return true, false, nil
	}
	if !ready {
		return false, false, fmt.Errorf("%w: Ticket watch event arrived before READY", ErrProtocol)
	}
	if err := validateFullID(record.Ticket); err != nil || record.Event == "" {
		return false, false, fmt.Errorf("%w: Ticket watch event is missing a valid ticket or event", ErrProtocol)
	}
	material := record.Event != "work-log-updated" && record.Event != "handoff-updated"
	return false, material, nil
}
