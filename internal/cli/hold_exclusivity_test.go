package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type competingHoldClient struct {
	*ticketclient.Client
	arrived  chan<- struct{}
	release  <-chan struct{}
	acquired chan<- struct{}
}

func (c *competingHoldClient) AcquireHold(ctx context.Context, id string, filters ticketclient.QueueFilters) (*ticketclient.Ticket, error) {
	c.arrived <- struct{}{}
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	item, err := c.Client.AcquireHold(ctx, id, filters)
	c.acquired <- struct{}{}
	return item, err
}

type exclusiveHoldHarness struct{ launched chan<- struct{} }

func (h exclusiveHoldHarness) Run(context.Context, harness.RunRequest) (harness.RunResult, error) {
	h.launched <- struct{}{}
	return harness.RunResult{StreamEndedNormally: true}, nil
}
func (h exclusiveHoldHarness) Resume(ctx context.Context, _ string, r harness.RunRequest) (harness.RunResult, error) {
	return h.Run(ctx, r)
}
func (exclusiveHoldHarness) Cleanup(context.Context, string, harness.CleanupPolicy) error { return nil }

// Both real dispatch paths observe the native repository before either is
// allowed to mutate it. Completion signals prove one total harness/wake and
// no losing execution, without scheduling sleeps or time-based assertions.
func TestNativeHoldDispatchExclusive(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "managed-managed", true: "managed-steered"}[mixed], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			root := t.TempDir()
			target := ticketclient.Target{Repository: root}
			cmd := exec.CommandContext(ctx, "ticket", "init", "-j")
			cmd.Dir, cmd.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "first", target)
			if out, err := cmd.CombinedOutput(); err != nil {
				cancel()
				t.Fatalf("init: %s %v", out, err)
			}
			arrived, acquired, launched := make(chan struct{}, 2), make(chan struct{}, 2), make(chan struct{}, 2)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			done := make(chan error, 2)
			var workers sync.WaitGroup
			var runner *dynamicSteerRunner
			clients := make([]*competingHoldClient, 2)
			t.Cleanup(func() {
				cancel()
				unblock()
				exited := make(chan struct{})
				go func() { workers.Wait(); close(exited) }()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("dispatch cleanup did not exit")
				}
				if runner != nil {
					runner.close()
				}
				for _, c := range clients {
					if c != nil {
						c.Close()
					}
				}
			})
			for i, actor := range []string{"first", "second"} {
				c, err := ticketclient.NewWithWorkingDirAndTarget(actor, root, target)
				if err != nil {
					t.Fatal(err)
				}
				clients[i] = &competingHoldClient{c, arrived, release, acquired}
			}
			hold := "hold"
			made, err := clients[0].CreateTicket(ctx, ticketclient.CreateInput{Title: "exclusive held task", State: &hold, Tags: []string{"api"}, Sections: map[string]string{"objective": "Only one assessment"}})
			if err != nil {
				t.Fatal(err)
			}
			info, err := ticketclient.ProbeInfo(ctx, "first", root, target)
			if err != nil {
				t.Fatal(err)
			}
			stateDirs := []string{t.TempDir(), t.TempDir()}
			runManaged := func(i int) {
				defer workers.Done()
				cfg := orc.AcceptanceConfig{WorkerName: []string{"first", "second"}[i], Actor: []string{"first", "second"}[i], Harness: "codex", SessionPolicy: orc.SessionPolicyFresh, SessionCleanup: "keep", RepositoryID: info.ID, WorkingDir: root, HoldQueue: true}
				done <- orc.RunAcceptance(ctx, cfg, clients[i], exclusiveHoldHarness{launched}, state.NewForRepository(stateDirs[i], info.ID), nil)
			}
			workers.Add(2)
			go runManaged(0)
			if mixed {
				dir := t.TempDir()
				joinDiagnosticSteer(t, dir, "second", info.ID)
				runner = newTestDynamicSteerRunner(t, ctx, dir, map[string]steerRolePolicy{"worker": holdSteerPolicy("signoff")}, nil, func(state.SteerRegistration) (steerClient, error) { return clients[1], nil }, testSteerRouter(t, func(context.Context, string, state.SteerRegistration, steertransport.Message) error {
					launched <- struct{}{}
					return nil
				}), &dynamicSteerStatus{}, nil, nil, nil, nil)
				go func() { defer workers.Done(); runner.bootstrap(ctx); done <- nil }()
			} else {
				go runManaged(1)
			}
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal("dispatch barrier: ", ctx.Err())
				}
			}
			wait(arrived)
			wait(arrived)
			unblock()
			wait(acquired)
			wait(acquired)
			wait(launched)
			cancel()
			for range 2 {
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("dispatch did not exit")
				}
			}
			if len(launched) != 0 {
				t.Fatal("loser launched another assessment")
			}
			checkCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			observer, err := ticketclient.NewWithWorkingDirAndTarget("observer", root, target)
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			item, err := observer.Show(checkCtx, made.ID)
			if err != nil || item.State != "hold" || (item.Assignee != "first" && item.Assignee != "second" && item.Assignee != "user") {
				t.Fatalf("ownership lost: %+v %v", item, err)
			}
		})
	}
}
