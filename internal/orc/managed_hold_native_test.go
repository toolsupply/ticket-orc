package orc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Cancel only after the real dispatch loop has exhausted both native queues.
// The real watch still supplies READY, Check, and Close; no elapsed-time idle
// inference or repository event timing is needed for the completion barrier.
type nativeHoldDrainClient struct {
	*ticketclient.Client
	cancel context.CancelFunc
}

type nativeHoldDrainWatch struct {
	ticketclient.ChangeWatcher
	cancel context.CancelFunc
}

func (c nativeHoldDrainClient) StartChangeWatch(ctx context.Context, repository string) (ticketclient.ChangeWatcher, error) {
	watch, err := c.Client.StartChangeWatch(ctx, repository)
	if err != nil {
		return nil, err
	}
	return nativeHoldDrainWatch{watch, c.cancel}, nil
}

func (w nativeHoldDrainWatch) WaitForChange(context.Context) (bool, error) {
	w.cancel()
	return false, context.Canceled
}

// Native exclusivity is covered by TestNativeHoldDispatchExclusive in cli;
// this sequence composes native acquisition/escalation guards with the real
// reviewer and architect loops, durable restart, and harness failure policy.
func TestManagedHoldNativeDrainRestartAndCrash(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			root := t.TempDir()
			target := ticketclient.Target{Repository: root}
			command := exec.CommandContext(ctx, "ticket", "init", "-j")
			command.Dir, command.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "assessor", target)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("init: %v: %s", err, output)
			}
			client, err := ticketclient.NewWithWorkingDirAndTarget("assessor", root, target)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			info, err := ticketclient.ProbeInfo(ctx, "assessor", root, target)
			if err != nil {
				t.Fatal(err)
			}
			makeTicket := func(lifecycle string) string {
				t.Helper()
				made, err := client.CreateTicket(ctx, ticketclient.CreateInput{Title: "controlled " + lifecycle, State: &lifecycle, Tags: []string{"api"}, Sections: map[string]string{"objective": "Validate bounded hold assessment", "handoff": "User must choose storage owner."}})
				if err != nil {
					t.Fatal(err)
				}
				return made.ID
			}
			var held, primary []string
			for range 3 {
				held = append(held, makeTicket("hold"))
				primary = append(primary, makeTicket(queue))
			}
			stateDir := t.TempDir()
			run := func(agent *fakeCoderHarness) error {
				turnCtx, stop := context.WithCancel(ctx)
				defer stop()
				tickets := nativeHoldDrainClient{client, stop}
				// Reopen the same durable store for every worker invocation.
				store := state.NewForRepository(stateDir, info.ID)
				if queue == "review" {
					cfg := reviewerConfig()
					cfg.WorkerName, cfg.Actor, cfg.WorkingDir = "stable-assessor", "assessor", root
					cfg.RepositoryID, cfg.HoldQueue = info.ID, true
					return RunReviewer(turnCtx, cfg, tickets, agent, store, nil)
				}
				cfg := acceptanceConfig()
				cfg.WorkerName, cfg.Actor, cfg.WorkingDir = "stable-assessor", "assessor", root
				cfg.RepositoryID, cfg.HoldQueue = info.ID, true
				return RunAcceptance(turnCtx, cfg, tickets, agent, store, nil)
			}
			newAgent := func() *fakeCoderHarness {
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				agent.onRun = func() {
					id := agent.runs[len(agent.runs)-1].TicketID
					item, err := client.Show(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if item.State == "hold" {
						if item.Assignee != "assessor" {
							t.Fatalf("assessment without exclusive ownership: %+v", item)
						}
						return // Clean unresolved completion must escalate natively.
					}
					if queue == "review" {
						_, err = client.ApproveTicket(ctx, id, ticketclient.MutationOptions{})
					} else {
						_, err = client.ReassignTicket(ctx, id, "user", ticketclient.MutationOptions{})
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				return agent
			}
			agent := newAgent()
			if err := run(agent); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			var got, want []string
			for i := range held {
				want = append(want, held[i], primary[i])
				item, err := client.Show(ctx, held[i])
				if err != nil || item.State != "hold" || item.Assignee != "user" {
					t.Fatalf("unresolved ownership: %+v %v", item, err)
				}
				detail, err := client.ShowFullBody(ctx, held[i])
				if err != nil || !strings.Contains(detail.Body, "User must choose storage owner") {
					t.Fatalf("unresolved handoff: %+v %v", detail, err)
				}
			}
			for _, request := range agent.runs {
				got = append(got, request.TicketID)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("bounded alternating progress: got %v want %v", got, want)
			}
			// Fresh primary work must run after restart without reassessing holds.
			fresh := makeTicket(queue)
			restarted := newAgent()
			if err := run(restarted); !errors.Is(err, context.Canceled) || len(restarted.runs) != 1 || restarted.runs[0].TicketID != fresh {
				t.Fatalf("restart: %v runs=%v", err, restarted.runs)
			}
			// A real failed harness turn retains ownership. Restart has neither
			// execution authority nor permission to release or launch diagnostics.
			crashed := makeTicket("hold")
			failure := &harness.ExecutionError{Harness: "codex", Cause: fmt.Errorf("controlled crash"), Result: harness.RunResult{ExitCode: 7}}
			failed := &fakeCoderHarness{runErr: failure, runRes: failure.Result}
			if err := run(failed); !errors.Is(err, failure) || len(failed.runs) != 1 {
				t.Fatalf("crash: %v runs=%v", err, failed.runs)
			}
			blocked := newAgent()
			if err := run(blocked); err == nil || !strings.Contains(err.Error(), "operator reconciliation") || len(blocked.runs) != 0 {
				t.Fatalf("uncertain restart: %v runs=%v", err, blocked.runs)
			}
			item, err := client.Show(ctx, crashed)
			if err != nil || item.State != "hold" || item.Assignee != "assessor" {
				t.Fatalf("crash ownership lost: %+v %v", item, err)
			}
		})
	}
}
