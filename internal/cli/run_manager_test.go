package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestWorkerManagerReloadCommitsCompleteSteerPolicyAndDirtiesRepositories(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "runtime")
	configPath := filepath.Join(root, "config.json")
	configData := func(prompt, tags, exclusions, completion string) string {
		return fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"roles":{"coder":{"ticket_queue":"open","nudge_prompt":%q,"ticket_tags":[%q],"ticket_without_tags":["generic-exclusion"]},"quality":{"ticket_queue":"review","nudge_prompt":%q,"ticket_tags":[%q],"ticket_without_tags":["generic-exclusion"],"review_completion":%q}},"review":{"skip_tags":[%q]}}`,
			stateDir, prompt, tags, prompt, tags, completion, exclusions)
	}
	writeConfigFixture(t, configPath, configData("old prompt", "old-tag", "old-exclusion", ReviewCompletionSignoff))
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := resolveSteerRolePolicies(loaded.Config, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	policies := newSteerPolicyStore(initial)
	initialGeneration := policies.Snapshot().generation
	dirty := newSteerDirtySet()
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: stateDir, InstanceID: loaded.Config.ID,
		steerPolicies: policies, steerDirty: dirty,
	}, os.Args[0], io.Discard, io.Discard, nil)
	defer manager.repositoryWatch.StopObservers()
	dirty.Drain()

	writeConfigFixture(t, configPath, configData("new prompt", "new-tag", "new-exclusion", ReviewCompletionClose))
	result, err := manager.reload(context.Background())
	if err != nil || !result.Applied {
		t.Fatalf("reload result=%#v err=%v", result, err)
	}
	snapshot := policies.Snapshot()
	if snapshot.generation != initialGeneration+1 {
		t.Fatalf("real policy replacement generation=%d, want %d", snapshot.generation, initialGeneration+1)
	}
	if snapshot.roles["coder"].NudgePrompt != "new prompt" || !reflect.DeepEqual(snapshot.roles["coder"].QueueFilters, ticketclient.QueueFilters{Tags: []string{"new-tag"}, WithoutTags: []string{"generic-exclusion"}}) {
		t.Fatalf("reloaded coder policy=%#v", snapshot.roles["coder"])
	}
	wantReview := ticketclient.QueueFilters{Tags: []string{"new-tag"}, WithoutTags: []string{"generic-exclusion", "new-exclusion"}}
	if snapshot.roles["quality"].TicketQueue != "review" || snapshot.roles["quality"].ReviewCompletion != ReviewCompletionClose || !reflect.DeepEqual(snapshot.roles["quality"].QueueFilters, wantReview) {
		t.Fatalf("reloaded review policy=%#v", snapshot.roles["quality"])
	}
	if !reflect.DeepEqual(snapshot.roles["quality"].TicketWithoutTags, []string{"generic-exclusion"}) || !reflect.DeepEqual(snapshot.roles["quality"].ReviewSkipTags, []string{"new-exclusion"}) {
		t.Fatalf("review exclusions were not kept separate in policy: %#v", snapshot.roles["quality"])
	}
	if !dirty.Drain().all {
		t.Fatal("committed policy reload did not dirty all steer repositories")
	}

	committedGeneration := snapshot.generation
	dirty.Drain()
	unrelatedConfig := strings.Replace(configData("new prompt", "new-tag", "new-exclusion", ReviewCompletionClose), `"local_dir":`, `"default_role":"quality","local_dir":`, 1)
	writeConfigFixture(t, configPath, unrelatedConfig)
	unrelatedReload, err := manager.reload(context.Background())
	if err != nil || !unrelatedReload.Applied {
		t.Fatalf("unrelated reload result=%#v err=%v", unrelatedReload, err)
	}
	if after := policies.Snapshot(); after.generation != committedGeneration || !reflect.DeepEqual(after.roles, snapshot.roles) {
		t.Fatalf("unrelated reload changed steering policy: before=%#v after=%#v", snapshot, after)
	}
	if dirtied := dirty.Drain(); !dirtied.empty() {
		t.Fatalf("unrelated reload dirtied steering repositories: %#v", dirtied)
	}

	writeConfigFixture(t, configPath, configData("committed despite later error", "committed-tag", "committed-exclusion", ReviewCompletionSignoff))
	postCommit, err := manager.reload(&errorAfterErrChecks{Context: context.Background(), failAt: 4})
	if err == nil || !postCommit.Applied {
		t.Fatalf("post-commit reload result=%#v err=%v, want applied state with reconciliation error", postCommit, err)
	}
	committed := policies.Snapshot()
	if committed.generation != snapshot.generation+1 {
		t.Fatalf("post-commit policy generation=%d, want %d", committed.generation, snapshot.generation+1)
	}
	if committed.roles["coder"].NudgePrompt != "committed despite later error" || !reflect.DeepEqual(committed.roles["coder"].QueueFilters, ticketclient.QueueFilters{Tags: []string{"committed-tag"}, WithoutTags: []string{"generic-exclusion"}}) {
		t.Fatalf("post-commit error rolled back steer policy: %#v", committed)
	}

	generation := committed.generation
	writeConfigFixture(t, configPath, configData("bad prompt", "invalid:tag", "new-exclusion", ReviewCompletionClose))
	if _, err := manager.reload(context.Background()); err == nil {
		t.Fatal("invalid policy reload succeeded")
	}
	afterFailure := policies.Snapshot()
	if afterFailure.generation != generation || afterFailure.roles["coder"].NudgePrompt != "committed despite later error" {
		t.Fatalf("pre-commit failure changed active policy: %#v", afterFailure)
	}
}

func TestDynamicSteerUsesResolvedRoleBounceLimit(t *testing.T) {
	configured := 6
	config := FileConfig{ID: "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa", Roles: map[string]RoleFileConfig{
		"coder": {TicketQueue: "open", MaxBounces: &configured},
	}}
	policies, err := resolveSteerRolePolicies(config, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	if got := policies["coder"].MaxBounces; got != 6 {
		t.Fatalf("resolved role max_bounces = %d, want 6", got)
	}
	if got, want := policies["coder"].ContainmentActor, "ticket-orc."+config.ID; got != want {
		t.Fatalf("containment actor = %q, want %q", got, want)
	}
	policies, err = resolveSteerRolePolicies(config, mapEnv(map[string]string{"TICKET_ORC_MAX_BOUNCES": "3"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := policies["coder"].MaxBounces; got != 3 {
		t.Fatalf("environment-resolved role max_bounces = %d, want 3", got)
	}
}

func TestWorkerManagerSerializesConcurrentStarts(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var calls atomic.Int32
	starter := func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
		calls.Add(1)
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(ctx, RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	first := make(chan error, 1)
	firstFinished := make(chan struct{})
	go func() {
		defer close(firstFinished)
		_, err := m.start(ctx, "one")
		close(started)
		first <- err
	}()
	t.Cleanup(func() {
		select {
		case <-firstFinished:
		case <-time.After(2 * time.Second):
			t.Errorf("worker start goroutine did not stop during test cleanup")
		}
		m.lifecycle.ForgetChild("one")
	})
	<-started
	_, secondErr := m.start(ctx, "one")
	if secondErr == nil {
		t.Fatal("concurrent start was accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("starter calls = %d, want one", calls.Load())
	}
	// The fake child has no process, so remove it directly for test cleanup.
	m.lifecycle.ForgetChild("one")
	<-first
}

func TestWorkerManagerPreflightsExplicitStartAndRetainsMetadata(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	var probes atomic.Int32
	var started supervisor.RunWorker
	manager := newWorkerManager(context.Background(), RunConfig{
		Workers: []supervisor.RunWorker{worker},
		ticketProbe: func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{Path: "/repo", ID: joinTestRepositoryID, FormatVersion: 1, StorageVersion: 1}, nil
		},
		startChild: func(_ context.Context, _ string, _ string, candidate supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
			started = candidate
			return &runChild{worker: candidate, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
		},
	}, os.Args[0], io.Discard, io.Discard, nil)
	result, err := manager.start(context.Background(), worker.Name)
	if err != nil || !result.Applied {
		t.Fatalf("start = %#v, %v", result, err)
	}
	if probes.Load() != 1 || started.TicketInfo == nil || started.TicketInfo.Path != "/repo" {
		t.Fatalf("probe count/metadata = %d/%#v", probes.Load(), started.TicketInfo)
	}
}

func TestWorkerManagerReloadRejectsChangedTicketTargetTransactionally(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","repository":"old"}}}`)
	localDir := defaultTestLocalDir(root)
	initial := supervisorTestWorker("one", localDir)
	initial.Config.Actor = "one"
	initial.Config.Repository = filepath.Join(root, "old")
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: localDir,
		Workers: []supervisor.RunWorker{initial},
		ticketProbe: func(_ context.Context, candidate supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
			if candidate.Config.Repository == filepath.Join(root, "new") {
				return ticketclient.RepositoryInfo{}, &ticketclient.ProbeError{Code: "missing_config"}
			}
			return ticketclient.RepositoryInfo{Path: candidate.Config.Repository, ID: joinTestRepositoryID}, nil
		},
		Runtime: NewRuntimeState([]supervisor.RunWorker{initial}),
	}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","repository":"new"}}}`)
	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "ticket_target_unavailable") {
		t.Fatalf("changed target reload error = %v", err)
	}
	current, _ := manager.lifecycle.Worker("one")
	if current.Config.Repository != filepath.Join(root, "old") {
		t.Fatalf("active target after rejected reload = %q", current.Config.Repository)
	}
}

func TestWorkerManagerReloadRejectsLocalRootChangeBeforeProbingOrApplying(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	const firstID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	const secondID = "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q,"workers":{"one":{"role":"coder","actor":"one"}}}`, firstID))
	localDir := filepath.Join(root, ".local", firstID)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "one"
	var probes atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker},
		repositoryProbe: func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{}, nil
		},
		Runtime: NewRuntimeState([]supervisor.RunWorker{worker}),
	}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q,"workers":{"one":{"role":"coder","actor":"one"}}}`, secondID))

	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "local_root_changed") {
		t.Fatalf("local-root reload error = %v, want local_root_changed", err)
	}
	if probes.Load() != 0 {
		t.Fatalf("candidate repositories were probed before root rejection: %d", probes.Load())
	}
	current, ok := manager.lifecycle.Worker("one")
	if !ok || current.Config.StateDir != localDir || len(manager.snapshotWorkers()) != 1 {
		t.Fatalf("active worker changed after root rejection: %#v", current)
	}
	if _, err := os.Stat(filepath.Join(root, ".local", secondID)); !os.IsNotExist(err) {
		t.Fatalf("reload created the candidate local root: %v", err)
	}
}

func TestWorkerManagerReloadRejectsInstanceIDChangeBeforeProbingOrApplying(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	const firstID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	const secondID = "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	const workerConfig = `,"workers":{"one":{"role":"coder","actor":"one"}}`
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+firstID+`"`+workerConfig+`}`)
	localDir := filepath.Join(root, ".local")
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "one"
	var probes atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{
		ConfigPath: configPath, StateDir: localDir, InstanceID: firstID, Workers: []supervisor.RunWorker{worker},
		repositoryProbe: func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			return ticketclient.RepositoryInfo{}, nil
		},
		Runtime: NewRuntimeState([]supervisor.RunWorker{worker}),
	}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+secondID+`"`+workerConfig+`}`)

	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "instance_id_changed") {
		t.Fatalf("instance-ID reload error = %v, want instance_id_changed", err)
	}
	if probes.Load() != 0 {
		t.Fatalf("candidate repositories were probed before identity rejection: %d", probes.Load())
	}
	if manager.instanceID != firstID {
		t.Fatalf("active instance ID changed to %q, want %q", manager.instanceID, firstID)
	}
	current, ok := manager.lifecycle.Worker("one")
	if !ok || current.Config.Actor != "one" || len(manager.snapshotWorkers()) != 1 {
		t.Fatalf("active worker config changed after identity rejection: %#v", current)
	}
}

func TestWorkerManagerPauseBlocksStartUntilResume(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	var starts atomic.Int32
	starter := func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
		starts.Add(1)
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: runtime, startChild: starter}, os.Args[0], io.Discard, io.Discard, func(name string, state supervisor.WorkerState, err error) {
		runtime.Set(supervisor.WorkerTransition{Worker: name, State: state})
	})
	paused, err := m.pause(context.Background(), worker.Name)
	if err != nil || !paused.Applied || paused.State != string(WorkerPaused) {
		t.Fatalf("pause = %#v, %v", paused, err)
	}
	if got := runtime.Snapshot()[worker.Name].State; got != WorkerPaused {
		t.Fatalf("runtime state after pause = %q", got)
	}
	repeatedPause, err := m.pause(context.Background(), worker.Name)
	if err != nil || repeatedPause.Applied || repeatedPause.State != string(WorkerPaused) {
		t.Fatalf("repeated pause = %#v, %v", repeatedPause, err)
	}
	if _, err := m.start(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "worker_paused") {
		t.Fatalf("start while paused error = %v", err)
	}
	if starts.Load() != 0 {
		t.Fatalf("start calls while paused = %d", starts.Load())
	}
	resumed, err := m.resume(context.Background(), worker.Name)
	if err != nil || !resumed.Applied || resumed.State != string(WorkerRunning) {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	if starts.Load() != 1 {
		t.Fatalf("start calls after resume = %d", starts.Load())
	}
	repeatedResume, err := m.resume(context.Background(), worker.Name)
	if err != nil || repeatedResume.Applied || repeatedResume.State != string(WorkerRunning) {
		t.Fatalf("repeated resume = %#v, %v", repeatedResume, err)
	}
	if starts.Load() != 1 {
		t.Fatalf("repeated resume start calls = %d, want one", starts.Load())
	}
	m.lifecycle.ForgetChild(worker.Name)
}

// A pause cannot be overwritten by a concurrent explicit start.
func TestWorkerManagerPauseSerializesWithStartup(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	childDir := t.TempDir()
	starterCalled := make(chan struct{})
	releaseStarter := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStarter) }) }
	var childMu sync.Mutex
	var startedChild *runChild
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		close(starterCalled)
		<-releaseStarter
		cmd := exec.Command(os.Args[0], "-test.run=TestMain")
		cmd.Env = append(os.Environ(), "TICKET_ORC_TEST_CHILD=1", "TICKET_ORC_TEST_CHILD_DIR="+childDir, "TICKET_ORC_WORKER="+worker.Name)
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		child := &runChild{worker: worker, cmd: cmd, done: make(chan error, 1), waited: make(chan struct{})}
		go func() {
			err := cmd.Wait()
			child.done <- err
			close(child.waited)
		}()
		childMu.Lock()
		startedChild = child
		childMu.Unlock()
		return child, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker}), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	started := make(chan error, 1)
	startedFinished := make(chan struct{})
	go func() {
		defer close(startedFinished)
		_, err := m.start(context.Background(), worker.Name)
		started <- err
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-startedFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("worker start goroutine did not stop during test cleanup")
		}
		childMu.Lock()
		child := startedChild
		childMu.Unlock()
		stopRunChildForTest(t, child)
	})
	select {
	case <-starterCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("worker starter was not called")
	}
	paused := make(chan error, 1)
	pausedFinished := make(chan struct{})
	go func() {
		defer close(pausedFinished)
		_, err := m.pause(context.Background(), worker.Name)
		paused <- err
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-pausedFinished:
		case <-time.After(5 * time.Second):
			t.Errorf("worker pause goroutine did not stop during test cleanup")
		}
	})
	select {
	case err := <-paused:
		t.Fatalf("pause completed before startup released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatalf("pause after startup: %v", err)
	}
	_, running := m.lifecycle.Child(worker.Name)
	pausedState := m.lifecycle.Paused(worker.Name)
	if running || !pausedState {
		t.Fatalf("startup/pause state running=%v paused=%v", running, pausedState)
	}
}

func TestWorkerManagerRejectsResumeAfterShutdownBegins(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.pause(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	m.lifecycle.SetStopping(true)
	if _, err := m.resume(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "daemon_stopping") {
		t.Fatalf("resume during shutdown error = %v", err)
	}
}

// A reloaded desired config survives pausing and restarting the active child.
func TestWorkerManagerPausesActiveChildAndResumesDesiredConfig(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", root)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "old-actor"
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"new-actor"}}}`)
	runtime := NewRuntimeState([]supervisor.RunWorker{worker})
	var failureTransitions atomic.Int32
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, func(name string, state supervisor.WorkerState, err error) {
		if state == WorkerFailed {
			failureTransitions.Add(1)
		}
		runtime.Set(supervisor.WorkerTransition{Worker: name, State: state})
	})
	started, err := manager.start(context.Background(), worker.Name)
	if err != nil || !started.Applied {
		t.Fatalf("start = %#v, %v", started, err)
	}
	waitForSupervisorTestFile(t, filepath.Join(root, "one.ready"))
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"new-actor"}}}`)
	if result, err := manager.reload(context.Background()); err != nil || !result.Applied {
		t.Fatalf("reload = %#v, %v", result, err)
	}
	paused, err := manager.pause(context.Background(), worker.Name)
	if err != nil || paused.State != string(WorkerPaused) || !paused.Applied {
		t.Fatalf("pause active child = %#v, %v", paused, err)
	}
	effective := runtime.EffectiveWorkers()
	if len(effective) != 1 || effective[0].Config.Actor != "new-actor" {
		t.Fatalf("effective worker after pause = %#v", effective)
	}
	if got := runtime.Snapshot()[worker.Name].State; got != WorkerPaused {
		t.Fatalf("runtime state after active pause = %q", got)
	}
	resumed, err := manager.resume(context.Background(), worker.Name)
	if err != nil || !resumed.Applied || resumed.State != string(WorkerRunning) {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	child, _ := manager.lifecycle.Child(worker.Name)
	if child == nil || child.worker.Config.Actor != "new-actor" {
		t.Fatalf("resumed child = %#v", child)
	}
	if _, err := manager.stop(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	if got := failureTransitions.Load(); got != 0 {
		t.Fatalf("deliberate pause/restart/stop classified %d child failures", got)
	}
}

func TestWorkerManagerRejectsRemovingPausedWorker(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	worker := supervisorTestWorker("one", localDir)
	worker.Config.Actor = "actor-one"
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker})}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := manager.pause(context.Background(), worker.Name); err != nil {
		t.Fatal(err)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{}}`)
	if _, err := manager.reload(context.Background()); err == nil || !strings.Contains(err.Error(), "candidate removes all configured workers") {
		t.Fatalf("paused worker removal error = %v", err)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	if _, err := manager.reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.start(context.Background(), worker.Name); err == nil || !strings.Contains(err.Error(), "worker_paused") {
		t.Fatalf("re-added paused worker start error = %v", err)
	}
}

func TestWorkerManagerRejectsActiveIdentityConflicts(t *testing.T) {
	first := supervisorTestWorker("coder", t.TempDir())
	second := supervisorTestWorker("reviewer", t.TempDir())
	first.Config.Actor, second.Config.Actor = "shared", "shared"
	workers := []supervisor.RunWorker{first, second}
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("duplicate actor start error = %v", err)
	}
	m.lifecycle.ForgetChild("coder")

	first.Config.Actor, second.Config.Actor = "coder", "reviewer"
	second.Config.Role = RoleReviewer
	first.Config.Harness, second.Config.Harness = "codex", "codex"
	for name := range m.lifecycle.Children() {
		m.lifecycle.ForgetChild(name)
	}
	m.lifecycle.SetWorker("coder", first)
	m.lifecycle.SetWorker("reviewer", second)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
}

func TestRoleRejectsSurvivingWorkerLease(t *testing.T) {
	config := supervisorTestWorker("one", t.TempDir()).Config
	lease, err := state.AcquireLock(context.Background(), workerLeasePath(config), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	err = executeRole(config)
	if !errors.Is(err, ErrWorkerLeaseConflict) {
		t.Fatalf("role lease conflict error=%v, want ErrWorkerLeaseConflict", err)
	}
}

func TestWorkerLeaseIdentityDoesNotDependOnHarnessOrModel(t *testing.T) {
	config := supervisorTestWorker("", t.TempDir()).Config
	config.Actor = "project-coder"
	config.RoleName = "backend"
	config.TicketQueue = "open"
	config.WorkingDir = "/work/project"
	basePath := workerLeasePath(config)
	baseName := filepath.Base(basePath)
	if filepath.Dir(basePath) != filepath.Join(config.StateDir, "workers") || len(strings.TrimSuffix(baseName, ".lease")) != 64 || strings.Contains(baseName, "project-coder") {
		t.Fatalf("worker lease path is not a collision-safe key beneath the local root: %q", basePath)
	}

	changedExecution := config
	changedExecution.Harness = "claude"
	changedExecution.Model = "changed-model"
	changedExecution.Pi.Provider = "different-provider"
	if got := workerLeasePath(changedExecution); got != basePath {
		t.Fatalf("harness/model changed worker lease identity: %q != %q", got, basePath)
	}

	renamedRole := config
	renamedRole.RoleName = "backend-v2"
	if got := workerLeasePath(renamedRole); got != basePath {
		t.Fatalf("configured role rename changed worker lease identity: %q != %q", got, basePath)
	}

	changedRole := config
	changedRole.Role = RoleReviewer
	if got := workerLeasePath(changedRole); got == basePath {
		t.Fatal("role change did not change the unnamed worker lease identity")
	}
}

func TestWorkerLeaseReleaseAllowsExplicitReplacement(t *testing.T) {
	config := supervisorTestWorker("replace", t.TempDir()).Config
	path := workerLeasePath(config)
	old, err := state.AcquireLock(context.Background(), path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if replacement, acquireErr := state.AcquireLock(context.Background(), path, 25*time.Millisecond); acquireErr == nil {
		replacement.Release()
		t.Fatal("replacement acquired a lease held by the older worker")
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	replacement, err := state.AcquireLock(context.Background(), path, time.Second)
	if err != nil {
		t.Fatalf("replacement after release: %v", err)
	}
	defer replacement.Release()
}

func TestWorkerManagerScopesIdentityConflictsByRepository(t *testing.T) {
	first := supervisorTestWorker("coder", t.TempDir())
	second := supervisorTestWorker("reviewer", t.TempDir())
	first.Config.Actor, second.Config.Actor = "shared", "shared"
	first.Config.Repository = filepath.Join(t.TempDir(), "repo-one")
	second.Config.Repository = filepath.Join(t.TempDir(), "repo-two")
	workers := []supervisor.RunWorker{first, second}
	starter := func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _ io.Writer, _ io.Writer, _ *sync.Mutex, _ *sync.Mutex) (*runChild, error) {
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m := newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err != nil {
		t.Fatalf("distinct repository actor start failed: %v", err)
	}

	first.Config.Repository = filepath.Join(t.TempDir(), "same-repo")
	second.Config.Repository = first.Config.Repository
	workers = []supervisor.RunWorker{first, second}
	m = newWorkerManager(context.Background(), RunConfig{Workers: workers, Runtime: NewRuntimeState(workers), startChild: starter}, os.Args[0], io.Discard, io.Discard, nil)
	if _, err := m.start(context.Background(), "coder"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.start(context.Background(), "reviewer"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("same repository actor start error = %v", err)
	}
}

func TestWorkerManagerRetainsChildWhenStopSignalFails(t *testing.T) {
	worker := supervisorTestWorker("one", t.TempDir())
	m := newWorkerManager(context.Background(), RunConfig{Workers: []supervisor.RunWorker{worker}, Runtime: NewRuntimeState([]supervisor.RunWorker{worker})}, os.Args[0], io.Discard, io.Discard, nil)
	command := exec.Command(os.Args[0])
	command.Env = setChildEnvironment(os.Environ(), "TICKET_ORC_TEST_EXIT_CODE", "0")
	if err := command.Start(); err != nil {
		t.Fatalf("start stop-failure helper process: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for stop-failure helper process: %v", err)
	}
	child := &runChild{worker: worker, cmd: command}
	m.lifecycle.TrackChild(worker.Name, child)
	_, err := m.stop(context.Background(), worker.Name)
	if err == nil {
		t.Fatal("stop unexpectedly succeeded for an already-exited process")
	}
	var lifecycleErr *supervisor.LifecycleError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Code != "worker_unavailable" {
		t.Fatalf("stop error = %v, want worker_unavailable lifecycle error", err)
	}
	_, retained := m.lifecycle.Child(worker.Name)
	if !retained {
		t.Fatal("failed stop lost the managed child")
	}
}

func TestWorkerManagerReloadAtomicVisibilityAndRollback(t *testing.T) {
	t.Setenv("TICKET_ORC_ACTOR", "reload-test")
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"roles":{"coder":{"groups":["role-old"]}},"workers":{"one":{"role":"coder","groups":["old"]}}}`)
	initial := supervisorTestWorker("one", localDir)
	runtime := NewRuntimeState([]supervisor.RunWorker{initial})
	m := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{initial}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, nil)
	writeConfigFixture(t, configPath, `{"version":1,"roles":{"coder":{"groups":["role-new"]}},"workers":{"one":{"role":"coder","groups":["new","role-new"]},"two":{"role":"reviewer","groups":["new"]}}}`)
	if result, err := m.reload(context.Background()); err != nil || !result.Applied {
		t.Fatalf("reload add/change = %#v, %v", result, err)
	}
	snapshot := m.snapshotWorkers()
	if len(snapshot) != 2 || snapshot[0].Name != "one" || !reflect.DeepEqual(snapshot[0].Groups, []string{"role-new", "new"}) || snapshot[1].Name != "two" {
		t.Fatalf("workers after add/change = %#v", snapshot)
	}
	if got := runtime.Snapshot()["two"].State; got != WorkerStopped {
		t.Fatalf("added worker state = %q, want stopped", got)
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"two":{"role":"reviewer","groups":["new"]}}}`)
	if _, err := m.reload(context.Background()); err != nil {
		t.Fatalf("reload remove stopped worker: %v", err)
	}
	if got := len(m.snapshotWorkers()); got != 1 || m.snapshotWorkers()[0].Name != "two" {
		t.Fatalf("workers after removal = %#v", m.snapshotWorkers())
	}
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"two":{"role":"reviewer","future":true}}}`)
	if _, err := m.reload(context.Background()); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if got := len(m.snapshotWorkers()); got != 1 || m.snapshotWorkers()[0].Name != "two" {
		t.Fatalf("invalid reload changed workers = %#v", m.snapshotWorkers())
	}
	worker, _ := m.lifecycle.Worker("two")
	m.lifecycle.TrackChild("two", &runChild{worker: worker})
	writeConfigFixture(t, configPath, `{"version":1,"workers":{}}`)
	if _, err := m.reload(context.Background()); err == nil {
		t.Fatal("running worker removal succeeded")
	}
}

func TestWorkerManagerReloadKeepsRunningEffectiveConfigAndUpdatesDesired(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"defaults":{"required_skills":["ticket"]},"workers":{"one":{"role":"coder","actor":"new-actor","groups":["old"]},"two":{"role":"reviewer","actor":"old-actor"}}}`)
	initial := supervisorTestWorker("one", localDir)
	initial.Config.Actor = "old-actor"
	initial.RequiredSkills = []string{"ticket"}
	runtime := NewRuntimeState([]supervisor.RunWorker{initial})
	m := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Workers: []supervisor.RunWorker{initial}, Runtime: runtime}, os.Args[0], io.Discard, io.Discard, nil)
	child := &runChild{worker: initial, cmd: &exec.Cmd{}}
	m.lifecycle.TrackChild("one", child)
	writeConfigFixture(t, configPath, `{"version":1,"defaults":{"required_skills":["ticket"]},"workers":{"one":{"role":"coder","actor":"new-actor","groups":["old"]},"two":{"role":"reviewer","actor":"old-actor"}}}`)
	reloadResult, err := m.reload(context.Background())
	if err != nil {
		t.Fatalf("reload changed running worker: %v", err)
	}
	if len(reloadResult.Warnings) == 0 || reloadResult.Warnings[0].Code != "reload.restart_required" {
		t.Fatalf("reload warnings = %#v, want restart_required", reloadResult.Warnings)
	}
	desired, _ := m.lifecycle.Worker("one")
	effectiveChild, _ := m.lifecycle.Child("one")
	effective := effectiveChild.worker
	if desired.Config.Actor != "new-actor" {
		t.Fatalf("desired actor = %q, want new-actor", desired.Config.Actor)
	}
	if effective.Config.Actor != "old-actor" {
		t.Fatalf("effective actor changed to %q while running", effective.Config.Actor)
	}
	if !reflect.DeepEqual(desired.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("desired required skills = %#v", desired.RequiredSkills)
	}
	if !reflect.DeepEqual(effective.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("effective required skills changed to %#v while running", effective.RequiredSkills)
	}
	if got := reloadResult.Warnings[0].Remediation; !strings.Contains(got, "restart this worker") {
		t.Fatalf("Skill reload remediation = %q", got)
	}
	effectiveWorkers := runtime.EffectiveWorkers()
	if len(effectiveWorkers) != 2 || effectiveWorkers[0].Config.Actor != "old-actor" {
		t.Fatalf("runtime effective workers = %#v; status must use the running configuration", effectiveWorkers)
	}
	if _, err := m.start(context.Background(), "two"); err == nil || !strings.Contains(err.Error(), "worker_identity_conflict") {
		t.Fatalf("start while old effective worker is running error = %v", err)
	}
	var started supervisor.RunWorker
	m.starter = func(_ context.Context, _ string, _ string, worker supervisor.RunWorker, _, _ io.Writer, _, _ *sync.Mutex) (*runChild, error) {
		started = worker
		return &runChild{worker: worker, cmd: &exec.Cmd{}, done: make(chan error, 1), waited: make(chan struct{})}, nil
	}
	m.lifecycle.ForgetChild("one")
	if result, err := m.start(context.Background(), "one"); err != nil || !result.Applied {
		t.Fatalf("restart after reload = %#v, %v", result, err)
	}
	if !reflect.DeepEqual(started.RequiredSkills, []string{"ticket"}) {
		t.Fatalf("restarted worker required skills = %#v", started.RequiredSkills)
	}
}
