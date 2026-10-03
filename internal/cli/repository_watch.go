package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	repositoryWatchExecutable = "ticket"
	repositoryWatchActor      = "ticket-orc"
	repositoryWatchDebounce   = supervisor.RepositoryWatchDebounce
)

type ticketWatchProcess struct {
	cmd         *exec.Cmd
	containment childContainment
	ready       chan error
	done        chan error
	finished    chan struct{}
	stopOnce    sync.Once
}

func startRepositoryWatchProcess(ctx context.Context, repository supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
	if ctx == nil {
		return nil, errors.New("repository watch context must not be nil")
	}
	target := ticketTargetFromConfiguredRepository(repository)
	args := make([]string, 0, 7)
	if target.Config != "" {
		args = append(args, "--config", target.Config, "--scope", target.Scope)
	}
	args = append(args, "watch", "-j", "--ready")
	cmd := exec.CommandContext(ctx, repositoryWatchExecutable, args...)
	configureChildProcess(cmd)
	cmd.Env = ticketclient.ApplyTargetEnvironment(os.Environ(), repositoryWatchActor, ticketTargetToClientTarget(repository.Target))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open Ticket watch output: %w", err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start Ticket watch: %w", err)
	}
	containment, err := attachChildContainment(cmd.Process)
	if err != nil {
		_ = forceChildStop(cmd.Process)
		_ = cmd.Wait()
		return nil, fmt.Errorf("contain Ticket watch: %w", err)
	}
	process := &ticketWatchProcess{cmd: cmd, containment: containment, ready: make(chan error, 1), done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		ready := false
		readErr := scanTicketWatchOutput(stdout, repository.ID, notify, func() {
			ready = true
			process.ready <- nil
		})
		if readErr != nil {
			_ = process.containment.force(cmd.Process)
		}
		waitErr := cmd.Wait()
		_ = process.containment.close()
		result := errors.Join(readErr, waitErr)
		if !ready {
			process.ready <- result
		}
		process.done <- result
		close(process.finished)
	}()
	select {
	case err := <-process.ready:
		if err != nil {
			<-process.finished
			return nil, err
		}
		return process, nil
	case <-ctx.Done():
		_ = process.stop()
		<-process.finished
		return nil, ctx.Err()
	}
}

type ticketWatchRecord struct {
	Type         json.RawMessage `json:"type"`
	RepositoryID string          `json:"repository_id"`
	supervisor.RepositoryWatchEvent
}

func decodeTicketWatchRecord(line []byte, expectedRepositoryID string, ready bool) (supervisor.RepositoryWatchEvent, bool, error) {
	if err := jsonx.Validate(line); err != nil {
		return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: fmt.Errorf("validate record: %w", err)}
	}
	var record ticketWatchRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: fmt.Errorf("decode record: %w", err)}
	}
	if len(record.Type) != 0 {
		var recordType string
		if bytes.Equal(bytes.TrimSpace(record.Type), []byte("null")) || json.Unmarshal(record.Type, &recordType) != nil || recordType == "" {
			return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: errors.New("record type must be a non-empty string")}
		}
		if recordType != "ready" {
			return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: fmt.Errorf("unexpected record type %q", recordType)}
		}
		if ready {
			return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: errors.New("duplicate READY record")}
		}
		if !ticketclient.ValidRepositoryID(expectedRepositoryID) || record.RepositoryID != expectedRepositoryID {
			return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: errors.New("READY record has an unexpected repository ID")}
		}
		return supervisor.RepositoryWatchEvent{}, true, nil
	}
	if !ready {
		return supervisor.RepositoryWatchEvent{}, false, &supervisor.WatchProtocolError{Cause: errors.New("repository event received before READY")}
	}
	return record.RepositoryWatchEvent, false, nil
}

func scanTicketWatchOutput(reader io.Reader, expectedRepositoryID string, notify func(supervisor.RepositoryWatchEvent), onReady func()) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	ready := false
	for scanner.Scan() {
		event, isReady, err := decodeTicketWatchRecord(scanner.Bytes(), expectedRepositoryID, ready)
		if err != nil {
			return err
		}
		if isReady {
			ready = true
			if onReady != nil {
				onReady()
			}
			continue
		}
		if notify != nil {
			notify(event)
		}
	}
	if err := scanner.Err(); err != nil {
		return &supervisor.WatchProtocolError{Cause: fmt.Errorf("read output: %w", err)}
	}
	if !ready {
		return &supervisor.WatchProtocolError{Cause: errors.New("Ticket watch exited before READY")}
	}
	return nil
}

func (p *ticketWatchProcess) wait() error {
	if p == nil {
		return os.ErrProcessDone
	}
	return <-p.done
}

func (p *ticketWatchProcess) stop() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return os.ErrProcessDone
	}
	var requestErr error
	p.stopOnce.Do(func() {
		requestErr = p.containment.request(p.cmd.Process)
		timer := time.NewTimer(runChildStopTimeout)
		defer timer.Stop()
		select {
		case <-p.finished:
		case <-timer.C:
			requestErr = errors.Join(requestErr, p.containment.force(p.cmd.Process))
		}
	})
	return requestErr
}

func (p *ticketWatchProcess) Wait() error { return p.wait() }
func (p *ticketWatchProcess) Stop() error { return p.stop() }

func ticketTargetToClientTarget(target supervisor.TicketTarget) ticketclient.Target {
	return ticketclient.Target{Repository: target.Repository, Config: target.Config, Scope: target.Scope}
}

func newRepositoryWatchManager(ctx context.Context, runtime *supervisor.RuntimeState[supervisor.RunWorker], repositories supervisor.RepositoryRegistry, probe repositoryProbe) *supervisor.RepositoryWatchManager {
	return newRepositoryWatchManagerWithStarter(ctx, runtime, repositories, probe, startRepositoryWatchProcess)
}

func newRepositoryWatchManagerWithStarter(ctx context.Context, runtime *supervisor.RuntimeState[supervisor.RunWorker], repositories supervisor.RepositoryRegistry, probe repositoryProbe, start supervisor.RepositoryWatchStarter) *supervisor.RepositoryWatchManager {
	return supervisor.NewRepositoryWatchManager(ctx, runtime, repositories, supervisor.RepositoryWatchProbe(probe), start)
}

func repositoryWatchFailureCode(err error) string { return supervisor.RepositoryWatchFailureCode(err) }
