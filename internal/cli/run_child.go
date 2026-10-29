package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
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
	stderrCapture  boundedWorkerStderr
	waited         chan struct{}
	ready          chan struct{}
	readyOnce      sync.Once
	startupAttempt *supervisor.StartupAttempt
	waitCalls      *int32
	failureMu      sync.Mutex
	failure        *orc.Failure
	exitMu         sync.Mutex
	exitErr        error
	lostChild      bool
	leaderOnly     bool
	boundary       processBoundaryResult
}

// processBoundaryResult separates a successful stop request from positive
// completion evidence. Its zero value is uncertain. Unix process groups do not
// provide a wait for all descendants. Windows starts the worker before job
// assignment, so pre-assignment children can escape that job. Neither platform
// may authorize post-failure Ticket mutation from a stop request, leader wait
// or output EOF, even when every operation succeeds.
// Err retains actual boundary failures independently of the worker exit status.
type processBoundaryResult struct {
	Confirmed bool
	Err       error
}

type runChildStarter func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error)

const supervisedRoleEnv = "TICKET_ORC_SUPERVISED_ROLE"

// Keep the final reap boundary controllable in tests: cancellation must still
// reach a live owned child after observation fails and before this wait ends.
var waitChildProcess = (*exec.Cmd).Wait

func startRunChild(ctx context.Context, configPath, executable string, worker supervisor.RunWorker, stdout, stderr io.Writer, outputMu, errorMu *sync.Mutex) (*runChild, error) {
	args := []string{string(worker.Config.Role), "--config", configPath, "--worker", worker.Name}
	cmd := exec.Command(executable, args...)
	configureChildProcess(cmd)
	if runtime.GOOS == "windows" {
		// A writer created before job assignment may escape containment. Bound
		// the post-exit pipe drain without treating EOF as completion evidence.
		cmd.WaitDelay = runChildStopTimeout
	}
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
	child.stderrCapture.dst = stderr
	child.stderrWriter = &attributedWriter{dst: &child.stderrCapture, prefix: "[" + worker.Name + "] ", start: true, mu: errorMu}
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
		stopErr := forceChildStop(cmd.Process)
		cmd.WaitDelay = runChildStopTimeout
		waitErr := cmd.Wait()
		return nil, fmt.Errorf("contain worker %q: %w", worker.Name, errors.Join(err, stopErr, waitErr))
	}
	child.containment = containment
	go func() {
		atomic.AddInt32(child.waitCalls, 1)
		var observationErr error
		// Observe the exact leader without reaping. Stop controls and the final
		// group signal share exitMu; retire controls before cmd.Wait can release
		// the identity. Unix group signals require successful exit observation;
		// Windows job ownership is independent of the leader's numeric PID.
		if runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
			observationErr = waitForChildExitBeforeReap(cmd.Process)
			child.exitMu.Lock()
			if observationErr == nil || runtime.GOOS == "windows" {
				// The private Windows job remains ours even if observing the
				// leader fails; its termination does not require a PID lookup.
				child.boundary.Err = errors.Join(child.boundary.Err, child.containment.forceAfterLeaderExit(cmd.Process))
			}
			child.boundary.Err = errors.Join(child.boundary.Err, observationErr)
			// A failed observer is not evidence of exit or lost ownership. While
			// cmd.Wait blocks, retain controls through the exact os.Process handle
			// only; group signaling would race the eventual reap and PID reuse.
			child.leaderOnly = runtime.GOOS != "windows" && observationErr != nil &&
				!errors.Is(observationErr, syscall.ECHILD) && !errors.Is(observationErr, os.ErrProcessDone)
			child.lostChild = !child.leaderOnly
			child.exitMu.Unlock()
			if observationErr != nil || child.boundaryResult().Err != nil {
				// Failure-only drain bound. A failed stop/observation cannot leave
				// the worker wait blocked on output retained by an unowned writer.
				cmd.WaitDelay = runChildStopTimeout
			}
		}
		err := waitChildProcess(cmd)
		child.exitMu.Lock()
		child.lostChild = true
		child.exitMu.Unlock()
		if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "windows" {
			child.exitMu.Lock()
			child.boundary.Err = errors.Join(child.boundary.Err, child.containment.force(cmd.Process))
			child.lostChild = true
			child.exitMu.Unlock()
		}
		if child.stdoutWriter != nil {
			_ = child.stdoutWriter.Flush()
		}
		if child.stderrEvents != nil {
			_ = child.stderrEvents.Flush()
		}
		_ = child.stderrWriter.Flush()
		child.exitMu.Lock()
		child.boundary.Err = errors.Join(child.boundary.Err, child.containment.close())
		err = errors.Join(err, child.boundary.Err)
		child.exitMu.Unlock()
		child.setExitResult(err)
		close(child.waited)
		child.done <- err
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

func (child *runChild) boundaryResult() processBoundaryResult {
	if child == nil {
		return processBoundaryResult{}
	}
	child.exitMu.Lock()
	defer child.exitMu.Unlock()
	return child.boundary
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
	child.exitMu.Lock()
	defer child.exitMu.Unlock()
	if child.lostChild {
		return os.ErrProcessDone
	}
	var err error
	if child.leaderOnly {
		err = child.cmd.Process.Kill()
	} else {
		err = child.containment.force(child.cmd.Process)
	}
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		child.boundary.Err = errors.Join(child.boundary.Err, err)
	}
	return err
}

func (child *runChild) requestStop() error {
	if child == nil || child.cmd == nil || child.cmd.Process == nil {
		return os.ErrProcessDone
	}
	child.exitMu.Lock()
	defer child.exitMu.Unlock()
	if child.lostChild {
		return os.ErrProcessDone
	}
	var err error
	if child.leaderOnly {
		err = child.cmd.Process.Signal(os.Interrupt)
	} else {
		err = child.containment.request(child.cmd.Process)
	}
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		child.boundary.Err = errors.Join(child.boundary.Err, err)
	}
	return err
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
	dst      io.Writer
	sink     orc.EventSink
	pending  []byte
	overflow bool
}

// maxWorkerLineBytes bounds framing memory, including lines with no newline.
// An oversized line is ordinary output throughout; its suffix cannot become
// a structured event just because it crossed a buffer boundary.
const maxWorkerLineBytes = 16 << 10

func (w *workerEventWriter) Write(data []byte) (int, error) {
	originalLen := len(data)
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		complete := n >= 0
		if complete {
			n++
		} else {
			n = len(data)
		}
		n = min(n, maxWorkerLineBytes-len(w.pending))
		w.pending = append(w.pending, data[:n]...)
		data = data[n:]
		complete = n > 0 && w.pending[len(w.pending)-1] == '\n'
		if complete || len(w.pending) == maxWorkerLineBytes {
			line := w.pending
			if !complete || w.overflow || !w.consumeEvent(bytes.TrimSuffix(line, []byte("\n"))) {
				if _, err := w.dst.Write(line); err != nil {
					return 0, err
				}
			}
			w.pending = w.pending[:0]
			w.overflow = !complete
		}
	}
	return originalLen, nil
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
	if !w.overflow && w.consumeEvent(line) {
		return nil
	}
	w.overflow = false
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
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		if n >= 0 {
			n++
		} else {
			n = len(data)
		}
		n = min(n, maxWorkerLineBytes-len(w.pending))
		w.pending = append(w.pending, data[:n]...)
		data = data[n:]
		complete := w.pending[len(w.pending)-1] == '\n'
		if !complete && len(w.pending) < maxWorkerLineBytes {
			continue
		}
		if w.start {
			if _, err := io.WriteString(w.dst, w.prefix); err != nil {
				return 0, err
			}
		}
		if _, err := w.dst.Write(w.pending); err != nil {
			return 0, err
		}
		w.pending = w.pending[:0]
		w.start = complete
	}
	return originalLen, nil
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

// Capture is independent of terminal rendering and remains bounded in
// interactive mode. It is evidence only; it never enters event parsing.
type boundedWorkerStderr struct {
	mu        sync.Mutex
	dst       io.Writer
	data      []byte
	truncated bool
}

func (w *boundedWorkerStderr) Write(data []byte) (int, error) {
	w.mu.Lock()
	n := min(len(data), terminaltext.DiagnosticDetailBytes-len(w.data))
	w.data = append(w.data, data[:n]...)
	w.truncated = w.truncated || n < len(data)
	w.mu.Unlock()
	if w.dst == nil {
		return len(data), nil
	}
	return w.dst.Write(data)
}
func (child *runChild) stderrDetail() string {
	child.stderrCapture.mu.Lock()
	defer child.stderrCapture.mu.Unlock()
	if child.stderrCapture.truncated {
		return "diagnostic detail redacted (truncated: stderr capture exceeded 2048 bytes)"
	}
	return string(child.stderrCapture.data)
}
func (child *runChild) observedProcess(err error) *harness.ProcessOutcome {
	var process *os.ProcessState
	if child.cmd != nil {
		process = child.cmd.ProcessState
	}
	var exit *exec.ExitError
	if process == nil && errors.As(err, &exit) {
		process = exit.ProcessState
	}
	outcome := harness.ObservedProcess(process)
	return &outcome
}
