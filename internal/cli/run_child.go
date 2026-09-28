package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type runChild struct {
	worker         supervisor.RunWorker
	cmd            *exec.Cmd
	containment    childContainment
	done           chan error
	stdoutWriter   *attributedWriter
	stderrWriter   *attributedWriter
	stderrEvents   *workerEventWriter
	waited         chan struct{}
	ready          chan struct{}
	readyOnce      sync.Once
	startupAttempt *supervisor.StartupAttempt
	waitCalls      *int32
	failureMu      sync.Mutex
	failure        *orc.Failure
	exitMu         sync.Mutex
	exitErr        error
}

type runChildStarter func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error)

const supervisedRoleEnv = "TICKET_ORC_SUPERVISED_ROLE"

func startRunChild(ctx context.Context, configPath, executable string, worker supervisor.RunWorker, stdout, stderr io.Writer, outputMu, errorMu *sync.Mutex) (*runChild, error) {
	args := []string{string(worker.Config.Role), "--config", configPath, "--worker", worker.Name}
	cmd := exec.Command(executable, args...)
	configureChildProcess(cmd)
	cmd.Dir = effectiveWorkingDirectory(worker.Config)
	if target := ticketTarget(worker.Config); target.Config != "" && (worker.TicketInfo == nil || strings.TrimSpace(worker.TicketInfo.Path) == "") {
		return nil, fmt.Errorf("resolve scoped Ticket target for worker %q before starting its harness", worker.Name)
	}
	cmd.Env = runChildEnvironment(os.Environ(), configPath, worker.Config, worker.TicketInfo)
	snapshot, err := encodeLaunchSnapshot(worker)
	if err != nil {
		return nil, err
	}
	cmd.Env = setChildEnvironment(cmd.Env, launchSnapshotEnv, snapshot)
	if worker.EventSink != nil {
		cmd.Env = setChildEnvironment(cmd.Env, "TICKET_ORC_EVENT_STREAM", "1")
	} else {
		// Do not let an operator's inherited setting make a direct child emit
		// structured records that have no supervisor sink to consume them.
		cmd.Env = setChildEnvironment(cmd.Env, "TICKET_ORC_EVENT_STREAM", "0")
	}
	waitCalls := int32(0)
	child := &runChild{worker: worker, cmd: cmd, done: make(chan error, 1), waited: make(chan struct{}), ready: make(chan struct{}), waitCalls: &waitCalls}
	if worker.Config.Output == OutputJSON {
		cmd.Stdout = &lockedWriter{dst: stdout, mu: outputMu}
	} else {
		child.stdoutWriter = &attributedWriter{dst: stdout, prefix: "[" + worker.Name + "] ", start: true, mu: outputMu}
		cmd.Stdout = child.stdoutWriter
	}
	child.stderrWriter = &attributedWriter{dst: stderr, prefix: "[" + worker.Name + "] ", start: true, mu: errorMu}
	if worker.EventSink != nil {
		child.stderrEvents = &workerEventWriter{dst: child.stderrWriter, sink: func(event orc.Event) {
			if child.captureFailure(event) {
				return
			}
			child.captureReady(event)
			worker.EventSink(event)
		}}
		cmd.Stderr = child.stderrEvents
	} else {
		cmd.Stderr = child.stderrWriter
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start worker %q: %w", worker.Name, err)
	}
	containment, err := attachChildContainment(cmd.Process)
	if err != nil {
		_ = forceChildStop(cmd.Process)
		_ = cmd.Wait()
		return nil, fmt.Errorf("contain worker %q: %w", worker.Name, err)
	}
	child.containment = containment
	go func() {
		atomic.AddInt32(child.waitCalls, 1)
		// On Linux and Darwin, observe leader exit without reaping it before
		// forcing its process group. The unreaped leader keeps the group identity
		// alive, so a descendant that outlives the leader is still cleaned up
		// without polling descendants or signaling a reusable numeric PGID.
		if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
			if waitForChildExitBeforeReap(cmd.Process) {
				_ = child.containment.forceAfterLeaderExit(cmd.Process)
			} else if runtime.GOOS == "darwin" {
				// A failed Darwin exit observation must not fall through to
				// post-reap cleanup, which cannot safely identify the old group.
				// Stop the verified group while the leader is still owned instead.
				if processAlive(cmd.Process) {
					_ = child.containment.force(cmd.Process)
				} else {
					_ = child.containment.forceAfterLeaderExit(cmd.Process)
				}
			}
		}
		err := cmd.Wait()
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
			_ = child.containment.force(cmd.Process)
		}
		if child.stdoutWriter != nil {
			_ = child.stdoutWriter.Flush()
		}
		if child.stderrEvents != nil {
			_ = child.stderrEvents.Flush()
		}
		_ = child.stderrWriter.Flush()
		_ = child.containment.close()
		child.setExitResult(err)
		child.done <- err
		close(child.waited)
	}()
	return child, nil
}

func (child *runChild) setExitResult(err error) {
	if child == nil {
		return
	}
	child.exitMu.Lock()
	child.exitErr = err
	child.exitMu.Unlock()
}

func (child *runChild) exitResult() error {
	if child == nil {
		return nil
	}
	child.exitMu.Lock()
	defer child.exitMu.Unlock()
	return child.exitErr
}

// captureReady records the one-way worker startup boundary. Readiness is
// advisory metadata from the child process; it never grants Ticket authority.
func (child *runChild) captureReady(event orc.Event) bool {
	if child == nil || event.Type != orc.WorkerReadyEventType {
		return false
	}
	if child.ready != nil {
		child.readyOnce.Do(func() { close(child.ready) })
	}
	return true
}

func (child *runChild) readyObserved() bool {
	if child == nil || child.ready == nil {
		return false
	}
	select {
	case <-child.ready:
		return true
	default:
		return false
	}
}

// captureFailure retains one child failure envelope for classification after
// the child exits. Failure events are consumed here and emitted by the
// supervisor once the wait result has been reconciled, preventing duplicate
// public failure notifications.
func (child *runChild) captureFailure(event orc.Event) bool {
	if child == nil || event.Type != "worker.failure" || event.Failure == nil {
		return false
	}
	copy := *event.Failure
	child.failureMu.Lock()
	child.failure = &copy
	child.failureMu.Unlock()
	return true
}

func (child *runChild) failureEnvelope() *orc.Failure {
	if child == nil {
		return nil
	}
	child.failureMu.Lock()
	defer child.failureMu.Unlock()
	if child.failure == nil {
		return nil
	}
	copy := *child.failure
	return &copy
}

func (child *runChild) forceStop() error {
	if child == nil || child.cmd == nil {
		return os.ErrProcessDone
	}
	return child.containment.force(child.cmd.Process)
}

func (child *runChild) requestStop() error {
	if child == nil || child.cmd == nil || child.cmd.Process == nil {
		return os.ErrProcessDone
	}
	return child.containment.request(child.cmd.Process)
}

func setChildEnvironment(base []string, name, value string) []string {
	result := make([]string, 0, len(base)+1)
	found := false
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if key == name {
			if !found {
				result = append(result, name+"="+value)
				found = true
			}
			continue
		}
		result = append(result, entry)
	}
	if !found {
		result = append(result, name+"="+value)
	}
	return result
}

func runChildEnvironment(base []string, configPath string, config supervisor.RoleConfig, ticketInfo *ticketclient.RepositoryInfo) []string {
	values := map[string]string{
		"TICKET_ORC":        filepath.Dir(configPath),
		"TICKET_ORC_WORKER": config.WorkerName,
		"TICKET_ORC_ACTOR":  config.Actor,
		supervisedRoleEnv:   "1",
	}
	result := make([]string, 0, len(base)+len(values))
	seen := make(map[string]bool, len(values))
	forbidden := map[string]struct{}{
		"TICKET_ORC_CONFIG":         {},
		"TICKET_ORC_STATE_DIR":      {},
		"TICKET_ORC_DAEMON_TOKEN":   {},
		"TICKET_ORC_ENDPOINT_TOKEN": {},
		"TICKET_ORC_BEARER_TOKEN":   {},
	}
	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		if _, blocked := forbidden[name]; blocked {
			continue
		}
		if value, replace := values[name]; replace {
			result = append(result, name+"="+value)
			seen[name] = true
			continue
		}
		result = append(result, entry)
	}
	for name, value := range values {
		if !seen[name] {
			result = append(result, name+"="+value)
		}
	}
	target := ticketTarget(config)
	if target.Config != "" && ticketInfo != nil {
		// Ticket accepts repository selection through TICKET_REPOSITORY, but
		// does not accept TICKET_CONFIG. Resolve the configured scope in Orc
		// before starting the harness, then pass the selected repository using
		// Ticket's supported environment contract.
		target = ticketclient.Target{Repository: ticketInfo.Path}
	}
	return ticketclient.ApplyTargetEnvironment(result, config.Actor, target)
}

func stopRunChildren(children []*runChild) {
	pending := append([]*runChild(nil), children...)
	for _, child := range pending {
		if child == nil || child.cmd == nil || child.cmd.Process == nil {
			continue
		}
		_ = child.requestStop()
	}
	deadline := time.NewTimer(runChildStopTimeout)
	defer deadline.Stop()
	for len(pending) > 0 {
		select {
		case <-pending[0].done:
			pending = pending[1:]
		case <-deadline.C:
			for _, child := range pending {
				if child.cmd != nil && child.cmd.Process != nil {
					_ = child.forceStop()
				}
			}
			for len(pending) > 0 {
				<-pending[0].done
				pending = pending[1:]
			}
			return
		}
	}
}

func stopActiveRunChildren(children []*runChild, results <-chan struct {
	child *runChild
	err   error
}) {
	for _, child := range children {
		if child != nil && child.cmd != nil && child.cmd.Process != nil {
			_ = child.requestStop()
		}
	}
	deadline := time.NewTimer(runChildStopTimeout)
	defer deadline.Stop()
	for len(children) > 0 {
		select {
		case result := <-results:
			for i, child := range children {
				if child == result.child {
					children = append(children[:i], children[i+1:]...)
					break
				}
			}
		case <-deadline.C:
			for _, child := range children {
				if child.cmd != nil && child.cmd.Process != nil {
					_ = child.forceStop()
				}
			}
			for len(children) > 0 {
				result := <-results
				for i, child := range children {
					if child == result.child {
						children = append(children[:i], children[i+1:]...)
						break
					}
				}
			}
			return
		}
	}
}

type attributedWriter struct {
	mu      *sync.Mutex
	dst     io.Writer
	prefix  string
	start   bool
	pending []byte
}

type workerEventWriter struct {
	dst     io.Writer
	sink    orc.EventSink
	pending []byte
}

func (w *workerEventWriter) Write(data []byte) (int, error) {
	originalLen := len(data)
	w.pending = append(w.pending, data...)
	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			return originalLen, nil
		}
		line := append([]byte(nil), w.pending[:i]...)
		w.pending = w.pending[i+1:]
		if !w.consumeEvent(line) {
			line = append(line, '\n')
			if _, err := w.dst.Write(line); err != nil {
				return 0, err
			}
		}
	}
}

func (w *workerEventWriter) consumeEvent(line []byte) bool {
	if !bytes.HasPrefix(line, []byte(orc.EventStreamPrefix)) {
		return false
	}
	var event orc.Event
	if err := json.Unmarshal(bytes.TrimSpace(bytes.TrimPrefix(line, []byte(orc.EventStreamPrefix))), &event); err != nil || strings.TrimSpace(event.Type) == "" {
		return false
	}
	w.sink(event)
	return true
}

func (w *workerEventWriter) Flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	line := append([]byte(nil), w.pending...)
	w.pending = nil
	if w.consumeEvent(line) {
		return nil
	}
	line = append(line, '\n')
	_, err := w.dst.Write(line)
	return err
}

func (w *attributedWriter) Write(data []byte) (int, error) {
	if w.mu == nil {
		w.mu = &sync.Mutex{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	originalLen := len(data)
	w.pending = append(w.pending, data...)
	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			return originalLen, nil
		}
		if w.start {
			if _, err := io.WriteString(w.dst, w.prefix); err != nil {
				return 0, err
			}
			w.start = false
		}
		if _, err := w.dst.Write(w.pending[:i+1]); err != nil {
			return 0, err
		}
		w.pending = w.pending[i+1:]
		w.start = true
	}
}

func (w *attributedWriter) Flush() error {
	if w == nil {
		return nil
	}
	if w.mu == nil {
		w.mu = &sync.Mutex{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return nil
	}
	if w.start {
		if _, err := io.WriteString(w.dst, w.prefix); err != nil {
			return err
		}
	}
	if _, err := w.dst.Write(w.pending); err != nil {
		return err
	}
	w.pending = nil
	w.start = true
	return nil
}

type lockedWriter struct {
	dst io.Writer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(data []byte) (int, error) {
	if w.mu == nil {
		w.mu = &sync.Mutex{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dst.Write(data)
}
