package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Exercise the same synchronous composition steps as runSupervisor. Completing
// initial startup is the barrier for asserting that dormant workers never launch.
func TestRunConfiguredInventoryAndStartupSelection(t *testing.T) {
	// Reload reads process overrides; keep its resolution identical to the
	// initial parser's empty environment without inheriting this worker's actor.
	t.Setenv("TICKET_ORC_ACTOR", "")
	if err := os.Unsetenv("TICKET_ORC_ACTOR"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", t.TempDir())
	for _, test := range []struct {
		name, workers, supervisor string
		args                      []string
		wantStarted               []string
	}{
		{name: "bare one", workers: `{"orc-coder":{"role":"coder","actor":"orc-coder","groups":["coding"]}}`, wantStarted: []string{"orc-coder"}},
		{name: "interactive one", workers: `{"orc-coder":{"role":"coder","actor":"orc-coder","groups":["coding"]}}`, args: []string{"-i"}, wantStarted: []string{"orc-coder"}},
		{name: "bare multiple", wantStarted: []string{"orc-coder", "reviewer"}},
		{name: "interactive multiple", args: []string{"-i"}, wantStarted: []string{"orc-coder", "reviewer"}},
		{name: "bare zero", workers: "{}"},
		{name: "interactive zero", workers: "{}", args: []string{"-i"}},
		{name: "empty groups one", workers: `{"orc-coder":{"role":"coder","actor":"orc-coder","groups":["coding"]}}`, supervisor: `,"supervisor":{"startup_groups":[]}`, wantStarted: []string{"orc-coder"}},
		{name: "interactive empty groups one", workers: `{"orc-coder":{"role":"coder","actor":"orc-coder","groups":["coding"]}}`, supervisor: `,"supervisor":{"startup_groups":[]}`, args: []string{"-i"}, wantStarted: []string{"orc-coder"}},
		{name: "empty groups multiple", supervisor: `,"supervisor":{"startup_groups":[]}`, wantStarted: []string{"orc-coder", "reviewer"}},
		{name: "interactive empty groups multiple", supervisor: `,"supervisor":{"startup_groups":[]}`, args: []string{"-i"}, wantStarted: []string{"orc-coder", "reviewer"}},
		{name: "empty groups zero", workers: "{}", supervisor: `,"supervisor":{"startup_groups":[]}`},
		{name: "interactive empty groups zero", workers: "{}", supervisor: `,"supervisor":{"startup_groups":[]}`, args: []string{"-i"}},
		{name: "worker subset", args: []string{"--worker", "orc-coder"}, wantStarted: []string{"orc-coder"}},
		{name: "group subset", args: []string{"--group", "coding"}, wantStarted: []string{"orc-coder"}},
		{name: "startup groups", supervisor: `,"supervisor":{"startup_groups":["coding"]}`, wantStarted: []string{"orc-coder"}},
		{name: "explicit overrides startup groups", supervisor: `,"supervisor":{"startup_groups":["coding"]}`, args: []string{"--worker", "reviewer"}, wantStarted: []string{"reviewer"}},
		{name: "all", args: []string{"--all"}, wantStarted: []string{"orc-coder", "reviewer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.json")
			workers := test.workers
			if workers == "" {
				workers = `{"orc-coder":{"role":"coder","actor":"orc-coder","groups":["coding"]},"reviewer":{"role":"reviewer","actor":"reviewer","groups":["reviewing"]}}`
			}
			writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"repositories":{"main":{"repository":"tickets"}},"defaults":{"repository":"main"},"workers":%s%s}`, workers, test.supervisor))
			config, help, err := parseRunConfig(append([]string{"--config", path}, test.args...), emptyEnv)
			if err != nil || help {
				t.Fatalf("parse config: help=%t err=%v", help, err)
			}
			if !reflect.DeepEqual(config.StartupWorkers, append([]string{}, test.wantStarted...)) {
				t.Fatalf("startup selection=%v, want %v", config.StartupWorkers, test.wantStarted)
			}
			config.Runtime = NewRuntimeState(config.Workers)
			config.control = &daemon.Control{}
			config.dispatchGate = newDispatchGate(state.NewDaemonControlStore(config.StateDir), state.DaemonRunning)
			config.repositoryProbe = func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
				return ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: filepath.Join(root, "tickets")}, nil
			}
			config.ticketProbe = func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
				return ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: filepath.Join(root, "tickets")}, nil
			}
			preflighted, failures, err := preflightSupervisorWorkers(context.Background(), &config, io.Discard)
			if err != nil || len(failures) != 0 {
				t.Fatalf("preflight: failures=%v err=%v", failures, err)
			}
			var started []string
			var children []*runChild
			config.startChild = func(ctx context.Context, configPath, executable string, worker supervisor.RunWorker, out, errOut io.Writer, outMu, errMu *sync.Mutex) (*runChild, error) {
				started = append(started, worker.Name)
				child, err := startRunChild(ctx, configPath, executable, worker, out, errOut, outMu, errMu)
				if err == nil {
					children = append(children, child)
				}
				return child, err
			}
			transition := newSupervisorWorkerTransition(config, func(supervisor.RuntimeEvent) {})
			manager := newWorkerManager(context.Background(), config, os.Args[0], io.Discard, io.Discard, transition)
			t.Cleanup(func() {
				manager.repositoryWatch.StopObservers()
				manager.stopAll(context.Background())
				for range children {
					select {
					case <-manager.results:
					case <-time.After(3 * time.Second):
						t.Error("child result forwarding did not finish")
					}
				}
			})
			wireSupervisorRuntimeEvents(config, manager, func(supervisor.RuntimeEvent) {})
			// An unrelated external registration and its repository remain
			// independently owned while managed inventory starts and reloads.
			registrations := state.NewRegistrationStore(config.StateDir)
			registration := dynamicTestRegistration(t, "ui-session", "/ticket-ui")
			if _, _, _, err := registrations.Join(context.Background(), registration); err != nil {
				t.Fatal(err)
			}
			registered, err := registrations.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			applyDynamicRepositoryObservation(context.Background(), manager,
				func(_ context.Context, registration state.SteerRegistration) (ticketclient.RepositoryInfo, error) {
					return ticketclient.RepositoryInfo{ID: registration.RepositoryID, Path: registration.RepositoryPath}, nil
				}, registrationObservation{Registrations: registered.Registrations})

			manager.preflighted = preflighted
			wireSupervisorControlCallbacks(&config, manager, func(supervisor.RuntimeEvent) {}, nil, nil)
			if err := startInitialManagedWorkers(context.Background(), config, manager, failures, transition, io.Discard); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(started, test.wantStarted) {
				t.Fatalf("initial launches=%v, want %v", started, test.wantStarted)
			}
			before := runtimeDaemonStatus(config.Runtime, nil).Workers
			wantCount := 2
			if test.workers != "" {
				wantCount = 1
			}
			if test.workers == "{}" {
				wantCount = 0
			}
			if len(before) != wantCount || len(manager.snapshotWorkers()) != wantCount {
				t.Fatalf("initial runtime/lifecycle inventory=%v/%v, want %d workers", before, manager.snapshotWorkers(), wantCount)
			}
			for _, worker := range before {
				wantState := string(WorkerStopped)
				for _, name := range test.wantStarted {
					if worker.Name == name {
						wantState = string(WorkerRunning)
					}
				}
				if worker.State != wantState || worker.RepositoryID != joinTestRepositoryID || worker.RepositoryKey != "main" || worker.TicketActor != worker.Name || worker.Role == "" {
					t.Fatalf("initial worker=%#v, want state=%s with resolved identity", worker, wantState)
				}
			}
			groupsBefore := config.control.Groups()
			logsBefore := runtimeLogSource(config.StateDir, config.Runtime).Inventory()
			for _, worker := range logsBefore.Workers {
				if worker.RepositoryID != joinTestRepositoryID || worker.Role == "" {
					t.Fatalf("log subscription identity=%#v", worker)
				}
			}
			if len(logsBefore.Workers) != len(before) {
				t.Fatalf("log inventory=%v, want all workers", logsBefore.Workers)
			}
			// A dormant worker must be startable before any reload, through the
			// same typed callback exposed by the daemon control transport.
			for _, worker := range before {
				if worker.State == string(WorkerStopped) {
					result, err := config.control.StartWorker(context.Background(), worker.Name)
					if err != nil || !result.Applied || result.Worker != worker.Name {
						t.Fatalf("start dormant worker=%#v err=%v", result, err)
					}
					if err := manager.verifyWorkerStartup(context.Background(), worker.Name); err != nil {
						t.Fatal(err)
					}
					if _, err := config.control.StopWorker(context.Background(), worker.Name); err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.name == "explicit overrides startup groups" {
				result, err := config.control.StartGroup(context.Background(), "coding")
				if err != nil || len(result.Results) != 1 || result.Results[0].Worker != "orc-coder" || !result.Results[0].Applied {
					t.Fatalf("start dormant group=%#v err=%v", result, err)
				}
				if err := manager.verifyWorkerStartup(context.Background(), "orc-coder"); err != nil {
					t.Fatal(err)
				}
				if _, err := config.control.StopGroup(context.Background(), "coding"); err != nil {
					t.Fatal(err)
				}
			}
			launchesBeforeReload := len(started)
			if result, err := config.control.Reload(context.Background()); err != nil || !result.Applied {
				t.Fatalf("reload=%#v err=%v", result, err)
			}
			if after := runtimeDaemonStatus(config.Runtime, nil).Workers; !reflect.DeepEqual(before, after) {
				t.Fatalf("startup/reload workers differ: before=%#v after=%#v", before, after)
			}
			if after := config.control.Groups(); !reflect.DeepEqual(groupsBefore, after) {
				t.Fatalf("startup/reload groups differ: before=%v after=%v", groupsBefore, after)
			}
			if after := runtimeLogSource(config.StateDir, config.Runtime).Inventory(); !reflect.DeepEqual(logsBefore, after) {
				t.Fatalf("startup/reload logs differ: before=%v after=%v", logsBefore, after)
			}
			if after, err := registrations.Snapshot(context.Background()); err != nil || !reflect.DeepEqual(registered, after) {
				t.Fatalf("managed startup/reload changed external registrations: before=%v after=%v err=%v", registered, after, err)
			}
			if _, ok := manager.dynamicReposByID[registration.RepositoryID]; !ok {
				t.Fatal("managed reload removed the unrelated dynamic repository")
			}

			if len(started) != launchesBeforeReload {
				t.Fatal("reload launched a stopped worker")
			}
		})
	}
}

func TestRunPreflightOnlyRejectsSelectedActorConflicts(t *testing.T) {
	for _, selection := range [][]string{nil, {"one"}, {"one", "two"}} {
		t.Run(fmt.Sprint(selection), func(t *testing.T) {
			one := supervisorTestWorker("one", t.TempDir())
			two := supervisorTestWorker("two", t.TempDir())
			two.Config.Actor = one.Config.Actor
			config := RunConfig{
				Workers: []supervisor.RunWorker{one, two}, StartupWorkers: selection,
				ticketProbe: func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
					return ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: "/repo"}, nil
				},
			}
			_, _, err := preflightSupervisorWorkers(context.Background(), &config, io.Discard)
			if (err != nil) != (len(selection) == 2) {
				t.Fatalf("preflight selection=%v err=%v", selection, err)
			}
		})
	}
}

func TestRunSupervisorJSONValidationUsesStartupSelection(t *testing.T) {
	for _, selection := range [][]string{nil, {"json"}, {"one", "two"}} {
		t.Run(fmt.Sprint(selection), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			workers := []supervisor.RunWorker{supervisorTestWorker("json", t.TempDir()), supervisorTestWorker("one", t.TempDir()), supervisorTestWorker("two", t.TempDir())}
			workers[0].Config.Output = OutputJSON
			config := RunConfig{
				StateDir: t.TempDir(), Workers: workers, StartupWorkers: selection,
				ticketProbe: func(context.Context, supervisor.RunWorker) (ticketclient.RepositoryInfo, error) {
					// Reaching preflight proves JSON validation allowed this
					// selection. Cancel before any workers or observers launch.
					cancel()
					return ticketclient.RepositoryInfo{ID: joinTestRepositoryID, Path: "/repo"}, nil
				},
			}
			if err := runSupervisor(ctx, config, os.Args[0], io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
				t.Fatalf("selection=%v stopped before Ticket preflight: %v", selection, err)
			}
		})
	}
}
