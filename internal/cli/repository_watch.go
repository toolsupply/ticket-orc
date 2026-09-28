package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

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
	done        chan error
	finished    chan struct{}
	stopOnce    sync.Once
}

func startRepositoryWatchProcess(ctx context.Context, repository supervisor.ConfiguredRepository, notify func(supervisor.RepositoryWatchEvent)) (supervisor.RepositoryWatchProcess, error) {
	if ctx == nil {
		return nil, errors.New("repository watch context must not be nil")
	}
	target := ticketTargetFromConfiguredRepository(repository)
	args := make([]string, 0, 6)
	if target.Config != "" {
		args = append(args, "--config", target.Config, "--scope", target.Scope)
	}
	args = append(args, "watch", "-j")
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
	process := &ticketWatchProcess{cmd: cmd, containment: containment, done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		var readErr error
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			var event supervisor.RepositoryWatchEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				readErr = &supervisor.WatchProtocolError{Cause: fmt.Errorf("decode event: %w", err)}
				_ = process.containment.force(cmd.Process)
				break
			}
			if notify != nil {
				notify(event)
			}
		}
		if readErr == nil && scanner.Err() != nil {
			readErr = &supervisor.WatchProtocolError{Cause: fmt.Errorf("read output: %w", scanner.Err())}
		}
		waitErr := cmd.Wait()
		_ = process.containment.close()
		process.done <- errors.Join(readErr, waitErr)
		close(process.finished)
	}()
	return process, nil
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
