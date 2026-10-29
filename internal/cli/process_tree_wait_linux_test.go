//go:build linux

package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestLinuxChildExitWaitBlocksUntilExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		previous := waitidChildExit
		defer func() { waitidChildExit = previous }()
		exit := make(chan struct{})
		calls := 0
		released := false
		waitidChildExit = func(pid int) error {
			calls++
			if pid != 123 {
				t.Errorf("waitid PID=%d, want 123", pid)
			}
			<-exit
			return nil
		}
		done := make(chan error, 1)
		t.Cleanup(func() {
			if !released {
				close(exit)
			}
			synctest.Wait()
		})
		go func() { done <- waitForChildExitBeforeReap(&os.Process{Pid: 123}) }()
		// All observer work is now blocked at the one syscall boundary, with
		// neither a timer nor a retry needed to observe the eventual exit.
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("idle observation made %d calls, want one", calls)
		}
		select {
		case <-done:
			t.Fatal("observer completed before exit")
		default:
		}
		close(exit)
		released = true
		if err := <-done; err != nil {
			t.Fatalf("exit did not complete observation: %v", err)
		}
	})
}

func TestLinuxConversationLostChildDoesNotSignalOrWaitForDescendantOutput(t *testing.T) {
	previousWait, previousSignal := waitidChildExit, signalProcessGroupID
	var group atomic.Int64
	var signals atomic.Int32
	waitidChildExit = func(pid int) error {
		group.Store(int64(pid))
		return reapLinuxChildBeforeObservation(pid)
	}
	signalProcessGroupID = func(int, syscall.Signal) error {
		signals.Add(1)
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var runErr error
	t.Cleanup(func() {
		cancel()
		// The controlled descendant pins this group until cleanup. Bypass
		// the signal recorder to release it even when the assertion fails.
		if pid := group.Load(); pid > 0 {
			_ = syscall.Kill(-int(pid), syscall.SIGKILL)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("lost-child conversation did not stop during cleanup")
		}
		waitidChildExit, signalProcessGroupID = previousWait, previousSignal
	})
	go func() {
		defer close(done)
		_, runErr = runConversationProcess(ctx, harness.ProcessRequest{
			Executable: "sh", Args: []string{"-c", "sleep 30 & exit 0"},
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lost ownership left conversation waiting for descendant output")
	}
	if !errors.Is(runErr, syscall.ECHILD) || signals.Load() != 0 {
		t.Fatalf("lost-child result=%v group signals=%d", runErr, signals.Load())
	}
}

func TestLinuxRunChildReportsPermanentObservationError(t *testing.T) {
	previous, previousSignal := waitidChildExit, signalProcessGroupID
	var signals atomic.Int32
	waitidChildExit = func(int) error { return syscall.ENOSYS }
	signalProcessGroupID = func(int, syscall.Signal) error { signals.Add(1); return nil }
	t.Cleanup(func() { waitidChildExit, signalProcessGroupID = previous, previousSignal })
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", t.TempDir())
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "unobserved")
	child, err := startRunChild(t.Context(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("unobserved", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatal(err)
	}
	stopRunChildAtTestCleanup(t, child)
	select {
	case <-child.waited:
	case <-time.After(5 * time.Second):
		t.Fatal("permanent observation error blocked child completion")
	}
	if child.boundaryResult().Confirmed || !errors.Is(child.boundaryResult().Err, syscall.ENOSYS) || !errors.Is(child.exitResult(), syscall.ENOSYS) || atomic.LoadInt32(child.waitCalls) != 1 || signals.Load() != 0 {
		t.Fatalf("child result=%v wait calls=%d", child.exitResult(), atomic.LoadInt32(child.waitCalls))
	}
}

func TestLinuxRunChildLostOwnershipRejectsStop(t *testing.T) {
	previousWait, previousSignal := waitidChildExit, signalProcessGroupID
	waitidChildExit = reapLinuxChildBeforeObservation
	t.Cleanup(func() { waitidChildExit, signalProcessGroupID = previousWait, previousSignal })
	signalProcessGroupID = func(int, syscall.Signal) error {
		t.Fatal("stop signaled a group after ownership was lost")
		return nil
	}
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", t.TempDir())
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "unobserved")
	child, err := startRunChild(t.Context(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("unobserved", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatal(err)
	}
	stopRunChildAtTestCleanup(t, child)
	select {
	case <-child.waited:
	case <-time.After(5 * time.Second):
		t.Fatal("lost-child worker did not finish")
	}
	if !errors.Is(child.exitResult(), syscall.ECHILD) {
		t.Fatalf("lost-child result=%v", child.exitResult())
	}
	// Model reuse by replacing the released handle with a live PID. Stopping
	// must reject the lost ownership before checking numeric liveness.
	child.cmd.Process = &os.Process{Pid: os.Getpid()}
	if err := child.requestStop(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("unowned stop request=%v", err)
	}
	if err := child.forceStop(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("unowned force stop=%v", err)
	}
}

// Model an external reaper with authoritative OS evidence, rather than just
// manufacturing ECHILD while the child is still waitable.
func reapLinuxChildBeforeObservation(pid int) error {
	for {
		_, err := syscall.Wait4(pid, nil, 0, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		return syscall.ECHILD
	}
}

func TestLinuxChildExitWaitRetriesOnlyInterruption(t *testing.T) {
	for _, test := range []struct {
		name   string
		errors []syscall.Errno
		want   bool
	}{
		{"exit", []syscall.Errno{0}, true},
		{"interrupted", []syscall.Errno{syscall.EINTR, syscall.EINTR, 0}, true},
		{"ownership_lost", []syscall.Errno{syscall.ECHILD}, false},
		{"unsupported", []syscall.Errno{syscall.ENOSYS}, false},
		{"invalid", []syscall.Errno{syscall.EINVAL}, false},
		{"denied", []syscall.Errno{syscall.EPERM}, false},
		{"interrupted_then_lost", []syscall.Errno{syscall.EINTR, syscall.ECHILD}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := waitidChildExit
			t.Cleanup(func() { waitidChildExit = previous })
			calls := 0
			waitidChildExit = func(int) error {
				if calls >= len(test.errors) {
					t.Fatal("retried a permanent waitid result")
				}
				err := test.errors[calls]
				calls++
				if err != 0 {
					return err
				}
				return nil
			}
			if got := waitForChildExitBeforeReap(&os.Process{Pid: 123}); (got == nil) != test.want || calls != len(test.errors) {
				t.Fatalf("observed=%v calls=%d, want %v/%d", got, calls, test.want, len(test.errors))
			} else if !test.want && !errors.Is(got, test.errors[len(test.errors)-1]) {
				t.Fatalf("observation lost structured errno: %v", got)
			}
		})
	}
	for _, process := range []*os.Process{nil, {}, {Pid: -1}} {
		if !errors.Is(waitForChildExitBeforeReap(process), os.ErrProcessDone) {
			t.Fatal("invalid process reported an owned exit")
		}
	}
}

// Run the single-P kernel wait in a subprocess so a regression to RawSyscall6
// cannot prevent the test's own failure guard from running. The parent kills
// the entire test process group on timeout, including the stdin-gated shell.
func TestLinuxChildExitWaitReleasesSchedulerAndPreservesReap(t *testing.T) {
	if os.Getenv("ORC_TEST_BLOCKING_WAIT_CHILD") == "1" {
		runtime.GOMAXPROCS(1)
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		defer writer.Close()
		cmd := exec.Command("sh", "-c", "read -r done; exit 7")
		cmd.Stdin = reader
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var observed error
		entered := make(chan struct{})
		previous := waitidChildExit
		waitidChildExit = func(pid int) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			return previous(pid)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("observer did not exit after child stop")
			}
			waitidChildExit = previous
			if cmd.ProcessState == nil {
				_ = cmd.Wait()
			}
		})
		go func() {
			observed = waitForChildExitBeforeReap(cmd.Process)
			close(done)
		}()
		<-entered
		// Entry acknowledges the call attempt, not kernel entry. Keep the real
		// child gated while exercising unrelated scheduler and reap work.
		progress := make(chan struct{})
		go func() { close(progress) }()
		<-progress
		select {
		case <-done:
			t.Fatal("stdin-gated child exited before release")
		default:
		}
		// An unrelated child can be reaped independently; the observer must
		// not consume a global SIGCHLD notification or another child's status.
		if err := exec.Command("sh", "-c", "exit 0").Run(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
			t.Fatal("unrelated child exit completed the observer")
		default:
		}
		writer.Close()
		<-done
		if observed != nil {
			t.Fatalf("blocking wait did not observe child exit: %v", observed)
		}
		// A second observation of an already-exited child must also succeed.
		// Only cmd.Wait consumes its status, and only once.
		if err := waitForChildExitBeforeReap(cmd.Process); err != nil {
			t.Fatalf("first observation reaped the child: %v", err)
		}
		if err := cmd.Wait(); err == nil || cmd.ProcessState.ExitCode() != 7 {
			t.Fatalf("final reap lost child exit code: %v / %v", err, cmd.ProcessState)
		}
		if err := waitForChildExitBeforeReap(cmd.Process); !errors.Is(err, syscall.ECHILD) {
			t.Fatalf("reaped child observation=%v, want ECHILD", err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLinuxChildExitWaitReleasesSchedulerAndPreservesReap$")
	cmd.Env = append(os.Environ(), "ORC_TEST_BLOCKING_WAIT_CHILD=1")
	configureChildProcess(cmd)
	cmd.Cancel = func() error { return forceChildStop(cmd.Process) }
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "PASS") {
		t.Fatalf("single-P wait subprocess: %v\n%s", err, output)
	}
}

func TestLinuxRunChildBoundaryErrorsRemainUncertain(t *testing.T) {
	for _, operation := range []string{"request", "force", "exit_cleanup"} {
		t.Run(operation, func(t *testing.T) {
			previous := signalProcessGroupID
			t.Cleanup(func() { signalProcessGroupID = previous })
			t.Setenv("TICKET_ORC_TEST_CHILD", "1")
			dir := t.TempDir()
			t.Setenv("TICKET_ORC_TEST_CHILD_DIR", dir)
			if operation == "exit_cleanup" {
				t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "boundary")
			}
			signalProcessGroupID = func(int, syscall.Signal) error { return syscall.EPERM }
			child, err := startRunChild(t.Context(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("boundary", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
			if err != nil {
				t.Fatal(err)
			}
			// Failure cleanup uses the real kernel operation, then joins the one
			// production wait before restoring the process-wide test seam.
			t.Cleanup(func() {
				child.exitMu.Lock()
				signalProcessGroupID = previous
				child.exitMu.Unlock()
				_ = child.forceStop()
				select {
				case <-child.waited:
				case <-time.After(5 * time.Second):
					t.Error("boundary fixture did not finish")
				}
			})
			if operation != "exit_cleanup" {
				waitForSupervisorTestFile(t, filepath.Join(dir, "boundary.ready"))
				var stopErr error
				if operation == "request" {
					stopErr = child.requestStop()
				} else {
					stopErr = child.forceStop()
				}
				if !errors.Is(stopErr, syscall.EPERM) {
					t.Fatalf("lost stop errno: %v", stopErr)
				}
				child.exitMu.Lock()
				signalProcessGroupID = previous
				child.exitMu.Unlock()
				if err := child.forceStop(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-child.waited:
			case <-time.After(5 * time.Second):
				t.Fatal("boundary failure blocked worker exit")
			}
			result := child.boundaryResult()
			var exit *exec.ExitError
			if result.Confirmed || !errors.Is(result.Err, syscall.EPERM) || !errors.Is(child.exitResult(), syscall.EPERM) || !errors.As(child.exitResult(), &exit) {
				t.Fatalf("lost stop/exit evidence: %+v, %v", result, child.exitResult())
			}
			if operation == "exit_cleanup" && exit.ExitCode() != 17 {
				t.Fatalf("cleanup overwrote known worker exit: %v", exit)
			}
		})
	}
}

func TestLinuxConversationPropagatesBoundaryFailureAndExit(t *testing.T) {
	previous := signalProcessGroupID
	signalProcessGroupID = func(int, syscall.Signal) error { return syscall.EPERM }
	t.Cleanup(func() { signalProcessGroupID = previous })
	result, err := runConversationProcess(t.Context(), harness.ProcessRequest{
		Executable: "sh", Args: []string{"-c", "exit 17"},
	})
	if result.ExitCode != 17 || !errors.Is(err, syscall.EPERM) {
		t.Fatalf("conversation cleanup lost errno or original exit: %+v, %v", result, err)
	}
}

func TestLinuxRunChildObservationFailureKeepsLiveChildStoppable(t *testing.T) {
	for _, fault := range []syscall.Errno{syscall.ENOSYS, syscall.EPERM, syscall.EINVAL} {
		t.Run(fault.Error(), func(t *testing.T) {
			previousObserver, previousWait, previousSignal := waitidChildExit, waitChildProcess, signalProcessGroupID
			t.Cleanup(func() {
				waitidChildExit, waitChildProcess, signalProcessGroupID = previousObserver, previousWait, previousSignal
			})
			failObserver, allowWait := make(chan struct{}), make(chan struct{})
			var failOnce, waitOnce sync.Once
			releaseObserver := func() { failOnce.Do(func() { close(failObserver) }) }
			releaseWait := func() { waitOnce.Do(func() { close(allowWait) }) }
			waiting := make(chan struct{})
			var observations, groupSignals atomic.Int32
			waitidChildExit = func(int) error { observations.Add(1); <-failObserver; return fault }
			waitChildProcess = func(cmd *exec.Cmd) error {
				// This milestone follows the production observer-failure transition,
				// rather than merely acknowledging the injected waitid handoff.
				close(waiting)
				<-allowWait
				return previousWait(cmd)
			}
			signalProcessGroupID = func(int, syscall.Signal) error { groupSignals.Add(1); return syscall.EIO }
			dir := t.TempDir()
			t.Setenv("TICKET_ORC_TEST_CHILD", "1")
			t.Setenv("TICKET_ORC_TEST_CHILD_DIR", dir)
			t.Setenv("TICKET_ORC_TEST_CHILD_IGNORE", "unobserved-live")
			child, err := startRunChild(t.Context(), supervisorTestConfigPath(t), os.Args[0], supervisorTestWorker("unobserved-live", t.TempDir()), io.Discard, io.Discard, &sync.Mutex{}, &sync.Mutex{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				// The exact process handle remains safe even if the assertion
				// detects broken control retirement. Never use a remembered PGID.
				_ = child.cmd.Process.Kill()
				releaseObserver()
				releaseWait()
				select {
				case <-child.waited:
				case <-time.After(5 * time.Second):
					t.Error("live observation-failure fixture did not join")
				}
			})
			waitForSupervisorTestFile(t, filepath.Join(dir, "unobserved-live.armed"))
			releaseObserver()
			select {
			case <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("observer failure did not reach final wait")
			}
			if err := child.cmd.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("fixture stopped before the control assertion: %v", err)
			}
			if err := child.requestStop(); err != nil {
				t.Fatalf("live owned child rejected graceful stop after observation failure: %v", err)
			}
			if err := child.forceStop(); err != nil {
				t.Fatalf("live owned child rejected force stop after observation failure: %v", err)
			}
			releaseWait()
			select {
			case <-child.waited:
			case <-time.After(5 * time.Second):
				t.Fatal("stopped child did not complete its final wait")
			}
			result := child.boundaryResult()
			var exit *exec.ExitError
			if result.Confirmed || !errors.Is(result.Err, fault) || !errors.Is(child.exitResult(), fault) || !errors.As(child.exitResult(), &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("lost uncertain boundary/exit evidence: %+v, %v", result, child.exitResult())
			}
			if observations.Load() != 1 || groupSignals.Load() != 0 {
				t.Fatalf("permanent observer failure retried or signaled a group: observations=%d signals=%d", observations.Load(), groupSignals.Load())
			}
			if err := child.forceStop(); !errors.Is(err, os.ErrProcessDone) {
				t.Fatalf("reaped child still accepted stop: %v", err)
			}
		})
	}
}

func TestLinuxConversationObservationFailurePreservesCancellation(t *testing.T) {
	for _, fault := range []syscall.Errno{syscall.ENOSYS, syscall.EPERM, syscall.EINVAL} {
		t.Run(fault.Error(), func(t *testing.T) {
			previousObserver, previousWait, previousSignal := waitidChildExit, waitChildProcess, signalProcessGroupID
			t.Cleanup(func() {
				waitidChildExit, waitChildProcess, signalProcessGroupID = previousObserver, previousWait, previousSignal
			})
			failObserver, allowWait := make(chan struct{}), make(chan struct{})
			var failOnce, waitOnce, readyOnce sync.Once
			releaseObserver := func() { failOnce.Do(func() { close(failObserver) }) }
			releaseWait := func() { waitOnce.Do(func() { close(allowWait) }) }
			waiting := make(chan *exec.Cmd, 1)
			var observations, groupSignals atomic.Int32
			waitidChildExit = func(int) error { observations.Add(1); <-failObserver; return fault }
			waitChildProcess = func(cmd *exec.Cmd) error {
				waiting <- cmd // Observer failure has been processed; controls must remain usable.
				<-allowWait
				return previousWait(cmd)
			}
			signalProcessGroupID = func(int, syscall.Signal) error { groupSignals.Add(1); return syscall.EIO }
			ctx, cancel := context.WithCancel(t.Context())
			ready, done := make(chan struct{}), make(chan struct{})
			var command *exec.Cmd
			var result harness.ProcessResult
			var runErr error
			t.Cleanup(func() {
				cancel()
				releaseObserver()
				if command == nil {
					select {
					case command = <-waiting:
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("conversation did not reach final wait during cleanup")
					}
				}
				if command != nil {
					_ = command.Process.Kill()
				}
				releaseWait()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("conversation observation-failure fixture did not join")
				}
			})
			go func() {
				defer close(done)
				result, runErr = runConversationProcess(ctx, harness.ProcessRequest{
					Executable: "sh", Args: []string{"-c", "printf ready; exec sleep 30"},
					Stdout: conversationReadyWriter{ready: func() { readyOnce.Do(func() { close(ready) }) }},
				})
			}()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("conversation did not become a live controlled fixture")
			}
			releaseObserver()
			select {
			case command = <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("conversation observer failure did not reach final wait")
			}
			if err := command.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("conversation stopped before cancellation: %v", err)
			}
			cancel()
			releaseWait()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("observation failure disabled live conversation cancellation")
			}
			var exit *exec.ExitError
			if !errors.Is(runErr, fault) || !errors.As(runErr, &exit) || result.ExitCode != exit.ExitCode() || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("lost observation error or canceled exit: %+v, %v", result, runErr)
			}
			if observations.Load() != 1 || groupSignals.Load() != 0 {
				t.Fatalf("unsafe observer failure: observations=%d group signals=%d", observations.Load(), groupSignals.Load())
			}
			if err := command.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
				t.Fatalf("conversation fixture was not reaped: %v", err)
			}
		})
	}
}
