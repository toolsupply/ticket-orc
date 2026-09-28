package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestParseDoctorConfig(t *testing.T) {
	emptyEnv := func(string) (string, bool) { return "", false }
	got, help, err := parseDoctorConfig([]string{"--config", "workers.json"}, emptyEnv)
	if err != nil || help || got.configPath != "workers.json" {
		t.Fatalf("doctor config=%#v help=%v err=%v", got, help, err)
	}
	if _, _, err := parseDoctorConfig([]string{"--state-dir=/tmp/orc"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "unknown option") {
		t.Fatalf("doctor state-dir override error = %v", err)
	}
	if _, _, err := parseDoctorConfig([]string{"--config", "a", "--config", "b"}, emptyEnv); err == nil {
		t.Fatal("duplicate doctor config accepted")
	}
	if _, help, err := parseDoctorConfig([]string{"--help"}, emptyEnv); err != nil || !help {
		t.Fatalf("doctor help=%v err=%v", help, err)
	}
}

func TestOrdinaryRunDoesNotEnableDoctorRecovery(t *testing.T) {
	root := t.TempDir()
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	config, help, err := parseRunConfig([]string{"--all", "--config", configPath}, emptyEnv)
	if err != nil || help {
		t.Fatalf("ordinary run config=%#v help=%t err=%v", config, help, err)
	}
	if config.Doctor {
		t.Fatal("ordinary run unexpectedly enabled doctor recovery")
	}
}

func TestDoctorResultCompleteRequiresEveryWorkerRecovered(t *testing.T) {
	if !doctorResultComplete(supervisor.DoctorResult{Workers: []supervisor.DoctorWorkerResult{{Worker: "one", Outcome: "recovered"}}}) {
		t.Fatal("all recovered workers should complete doctor")
	}
	if doctorResultComplete(supervisor.DoctorResult{Workers: []supervisor.DoctorWorkerResult{{Worker: "one", Outcome: "recovered"}, {Worker: "two", Outcome: "failed"}}}) {
		t.Fatal("a failed worker must make standalone doctor fail")
	}
}

func TestDoctorFailureReasonBoundsRecoveryActions(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "configuration", err: &supervisor.LifecycleError{Code: "invalid_config"}, want: "configuration change required"},
		{name: "external", err: &supervisor.LifecycleError{Code: "worker_unavailable"}, want: "worker configuration or availability requires correction"},
		{name: "opaque fallback", err: errors.New("opaque harness details"), want: "manual operator decision required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := doctorFailureReason(test.err)
			if got != test.want || strings.Contains(got, "opaque") {
				t.Fatalf("doctor failure reason=%q, want %q", got, test.want)
			}
		})
	}
}

func TestWorkerManagerDoctorRejectsInvalidReload(t *testing.T) {
	root := t.TempDir()
	configPath := root + "/config.json"
	if err := os.WriteFile(configPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := supervisorTestWorker("one", root)
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, Workers: []supervisor.RunWorker{worker}}, "ticket-orc", io.Discard, io.Discard, nil)
	_, err := manager.doctor(context.Background())
	var controlErr *supervisor.LifecycleError
	if !errors.As(err, &controlErr) || controlErr.Code != "invalid_config" {
		t.Fatalf("doctor invalid reload error=%v, want invalid_config", err)
	}
}

func TestWorkerManagerDoctorReportsPartialRecovery(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"},"two":{"role":"coder","actor":"actor-two"}}}`)
	workers := []supervisor.RunWorker{supervisorTestWorker("one", localDir), supervisorTestWorker("two", localDir)}
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: workers, Runtime: NewRuntimeState(workers), startChild: func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		if worker.Name == "two" {
			return nil, errors.New("simulated start failure")
		}
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}}, "ticket-orc", io.Discard, io.Discard, nil)
	result, err := manager.doctor(context.Background())
	if err != nil || len(result.Workers) != 2 {
		t.Fatalf("doctor result=%#v err=%v", result, err)
	}
	if result.Workers[0].Worker != "one" || result.Workers[0].Outcome != "recovered" || result.Workers[1].Worker != "two" || result.Workers[1].Outcome != "failed" {
		t.Fatalf("doctor workers=%#v", result.Workers)
	}
	if doctorResultComplete(result) {
		t.Fatal("partial recovery must produce a non-success exit result")
	}
}

func TestWorkerManagerDoctorRejectsChildThatExitsDuringVerification(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	worker := supervisorTestWorker("one", localDir)
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	starts := 0
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		starts++
		child := &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{}), ready: make(chan struct{})}
		child.captureFailure(orc.Event{Type: "worker.failure", Worker: candidate.Name, Role: "reviewer", Failure: &orc.Failure{Classification: "ticket_command_failed", Phase: "worker operation", ExitCode: 1, TicketCode: "ticket_not_ready"}})
		child.done <- errors.New("child exited")
		close(child.waited)
		return child, nil
	}}, "ticket-orc", io.Discard, io.Discard, nil)
	result, err := manager.doctor(context.Background())
	if err != nil || len(result.Workers) != 1 || result.Workers[0].Outcome != "failed" {
		t.Fatalf("doctor result=%#v err=%v", result, err)
	}
	if result.Workers[0].Reason != "worker exited during startup verification" {
		t.Fatalf("doctor failure reason=%q", result.Workers[0].Reason)
	}
	if result.Workers[0].Failure == nil || result.Workers[0].Failure.Classification != "ticket_command_failed" || result.Workers[0].Failure.TicketCode != "ticket_not_ready" {
		t.Fatalf("doctor failure metadata=%#v", result.Workers[0].Failure)
	}
	if starts != 1 || runtimeState.Snapshot()[worker.Name].State != WorkerFailed {
		t.Fatalf("starts=%d runtime=%#v, want one failed child", starts, runtimeState.Snapshot())
	}
	_, running := manager.lifecycle.Child(worker.Name)
	if running {
		t.Fatal("doctor retained a child that exited during verification")
	}
	second, err := manager.doctor(context.Background())
	if err != nil || len(second.Workers) != 1 || second.Workers[0].Outcome != "failed" {
		t.Fatalf("repeat doctor result=%#v err=%v", second, err)
	}
	if starts != 2 {
		t.Fatalf("definitive startup failure launches=%d, want one retry", starts)
	}
}

func TestWorkerManagerDoctorDoesNotDuplicateHealthyWorkerLaunch(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	worker := supervisorTestWorker("one", localDir)
	runtimeState := NewRuntimeState([]supervisor.RunWorker{worker})
	starts := 0
	var child *runChild
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtimeState, startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		starts++
		child = &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}
		return child, nil
	}}, "ticket-orc", io.Discard, io.Discard, nil)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := manager.doctor(context.Background())
		if err != nil || len(result.Workers) != 1 || result.Workers[0].Outcome != "recovered" {
			t.Fatalf("doctor attempt %d result=%#v err=%v", attempt, result, err)
		}
	}
	if starts != 1 {
		t.Fatalf("healthy worker launches=%d, want one", starts)
	}
	child.done <- nil
	close(child.waited)
}

func TestWorkerManagerDoctorReconcilesAnotherChildExitDuringVerification(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"},"two":{"role":"coder","actor":"actor-two"}}}`)
	workers := []supervisor.RunWorker{supervisorTestWorker("one", localDir), supervisorTestWorker("two", localDir)}
	children := make(map[string]*runChild)
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: workers, Runtime: NewRuntimeState(workers), startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		child := &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{}), ready: make(chan struct{})}
		children[candidate.Name] = child
		if candidate.Name == "one" {
			close(child.ready)
		}
		return child, nil
	}}, "ticket-orc", io.Discard, io.Discard, nil)
	for _, worker := range workers {
		if _, err := manager.start(context.Background(), worker.Name); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(children["two"].waited)
	}()
	result, err := manager.doctor(context.Background())
	if err != nil || len(result.Workers) != 2 {
		t.Fatalf("doctor result=%#v err=%v", result, err)
	}
	byWorker := make(map[string]supervisor.DoctorWorkerResult, len(result.Workers))
	for _, outcome := range result.Workers {
		byWorker[outcome.Worker] = outcome
	}
	if byWorker["one"].Outcome != "recovered" || byWorker["two"].Outcome != "failed" {
		t.Fatalf("doctor outcomes=%#v", byWorker)
	}
	children["one"].done <- nil
	close(children["one"].waited)
	children["two"].done <- nil
}

func TestStandaloneDoctorKeepsRecoveredWorkersUnderSupervisor(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := root + "/config.json"
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	worker := supervisorTestWorker("one", localDir)
	started := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runSupervisor(ctx, RunConfig{
			ConfigPath: configPath, StateDir: localDir, Doctor: true,
			Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}),
			startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
				started <- struct{}{}
				return &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
			},
		}, "ticket-orc", io.Discard, io.Discard)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("doctor did not start the recovered worker")
	}
	select {
	case err := <-done:
		t.Fatalf("doctor supervisor exited before cancellation: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("doctor supervisor shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("doctor supervisor did not stop after cancellation")
	}
}
