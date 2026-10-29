package orc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Stop at the next selection boundary, after the coder reconciles its hold.
// All ticket operations, including the initial blocking claim, use Ticket.
type holdIntegrationCoder struct {
	*ticketclient.Client
	cancel context.CancelFunc
	claims int
}

func (c *holdIntegrationCoder) WaitAndClaimImplementation(ctx context.Context, filters ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	c.claims++
	if c.claims > 1 {
		c.cancel()
		return ticketclient.Ticket{}, ctx.Err()
	}
	return c.Client.WaitAndClaimImplementation(ctx, filters)
}

func TestManagedHoldWorkflowWithTicket(t *testing.T) {
	for _, primary := range []string{"signoff", "review"} {
		t.Run(primary, func(t *testing.T) {
			testCtx, stop := context.WithTimeout(t.Context(), 15*time.Second)
			defer stop()
			root := t.TempDir()
			target := ticketclient.Target{Repository: filepath.Join(root, "tickets")}
			init := exec.CommandContext(testCtx, "ticket", "init", "-j")
			init.Dir, init.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "coder-1", target)
			if out, err := init.CombinedOutput(); err != nil {
				t.Fatalf("init: %v %s", err, out)
			}
			// A higher-priority hold in another repository must never enter this
			// consumer's frontier, even when its primary queue is empty.
			foreignTarget := ticketclient.Target{Repository: filepath.Join(root, "other-tickets")}
			foreignInit := exec.CommandContext(testCtx, "ticket", "init", "-j")
			foreignInit.Dir, foreignInit.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "foreign", foreignTarget)
			if out, err := foreignInit.CombinedOutput(); err != nil {
				t.Fatalf("foreign init: %v %s", err, out)
			}
			foreign, err := ticketclient.NewWithWorkingDirAndTarget("foreign", root, foreignTarget)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { foreign.Close() })
			holdState, highest := "hold", 0
			if _, err := foreign.CreateTicket(testCtx, ticketclient.CreateInput{Title: "Foreign hold", State: &holdState, Priority: &highest, Sections: map[string]string{"objective": "This hold belongs only to another repository."}}); err != nil {
				t.Fatal(err)
			}
			info, err := ticketclient.ProbeInfo(testCtx, "coder-1", root, target)
			if err != nil {
				t.Fatal(err)
			}
			coder, err := ticketclient.NewWithWorkingDirAndTarget("coder-1", root, target)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { coder.Close() })
			created, err := coder.CreateTicket(testCtx, ticketclient.CreateInput{Title: "Architecture decision", Sections: map[string]string{"objective": "Choose the approved storage boundary."}})
			if err != nil {
				t.Fatal(err)
			}
			const blocker = "Architecture blocker: approved scope does not identify the durable storage owner. Architect must choose the owning package before implementation resumes."
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			agent := &fakeCoderHarness{onRun: func() {
				result, err := coder.HoldTicket(testCtx, created.ID, ticketclient.MutationOptions{Handoff: blocker})
				if err != nil || result.ID != created.ID || result.State != "hold" {
					t.Fatalf("coder hold: %+v %v", result, err)
				}
			}}
			store := state.NewForRepository(filepath.Join(root, "state"), info.ID)
			config := coderConfig()
			config.WorkingDir, config.StateDir = root, filepath.Join(root, "state")
			if err := RunCoder(ctx, config, &holdIntegrationCoder{Client: coder, cancel: cancel}, agent, store, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("coder: %v", err)
			}
			held, err := coder.ShowFullBody(testCtx, created.ID)
			if err != nil || held.State != "hold" || held.Assignee != "" || !strings.Contains(held.Body, blocker) {
				t.Fatalf("held: %+v %v", held, err)
			}

			consumer, err := ticketclient.NewWithWorkingDirAndTarget("decision-worker", root, target)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { consumer.Close() })
			ctx, cancelConsumer := context.WithCancel(testCtx)
			defer cancelConsumer()
			var normal string
			if primary == "review" {
				queue := "review"
				item, err := coder.CreateTicket(testCtx, ticketclient.CreateInput{Title: "Ordinary review", State: &queue, Sections: map[string]string{"objective": "Review approved implementation."}})
				if err != nil {
					t.Fatal(err)
				}
				normal = item.ID
			}
			var decisions []string
			consumerAgent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
			consumerAgent.onRun = func() {
				req := consumerAgent.runs[len(consumerAgent.runs)-1]
				decisions = append(decisions, req.TicketID)
				if req.TicketID == normal {
					if _, err := consumer.ApproveTicket(testCtx, normal, ticketclient.MutationOptions{}); err != nil {
						t.Fatal(err)
					}
					return
				}
				if req.TicketID != created.ID || !strings.HasPrefix(req.Prompt, HoldTaskPrompt(created.ID, primary)) {
					t.Fatalf("hold task: %+v", req)
				}
				if primary == "signoff" {
					if _, err := consumer.OpenTicket(testCtx, created.ID, ticketclient.MutationOptions{Handoff: "Decision: state owns durable storage; implementation may resume."}); err != nil {
						t.Fatal(err)
					}
				} else {
					// The worker follows the shared prompt's atomic owner guard.
					cmd := exec.CommandContext(testCtx, "ticket", "query", "-j", "id:"+created.ID, "state:hold", "assignee:decision-worker", "::", "reassign", "--to", "user", "--handoff", "Storage ownership exceeds reviewer authority; user must approve the architecture.")
					cmd.Dir, cmd.Env = root, ticketclient.ApplyTargetEnvironment(os.Environ(), "decision-worker", target)
					out, err := cmd.CombinedOutput()
					var receipt ticketclient.MutationResult
					if err != nil || json.Unmarshal(out, &receipt) != nil || receipt.ID != created.ID || receipt.State != "hold" || receipt.Assignee != "user" {
						t.Fatalf("guarded handoff: %s %v", out, err)
					}
				}
			}
			var outcomes []string
			sink := func(event Event) {
				if event.Type == "ticket.hold" {
					outcomes = append(outcomes, event.State)
				}
				if primary == "signoff" && event.Type == "ticket.hold" && event.State == "open" || event.Type == "ticket.lifecycle" && event.Ticket == normal {
					cancelConsumer()
				}
			}
			if primary == "signoff" {
				cfg := acceptanceConfig()
				cfg.Actor, cfg.RepositoryID, cfg.WorkingDir = "decision-worker", info.ID, root
				cfg.QueueFilters, cfg.HoldQueue, cfg.EventSink = ticketclient.QueueFilters{}, true, sink
				err = RunAcceptance(ctx, cfg, consumer, consumerAgent, newFakeCoderState(), nil)
			} else {
				cfg := reviewerConfig()
				cfg.Actor, cfg.RepositoryID, cfg.WorkingDir = "decision-worker", info.ID, root
				cfg.HoldQueue, cfg.ReviewCompletion, cfg.EventSink = true, "signoff", sink
				err = RunReviewer(ctx, cfg, consumer, consumerAgent, newFakeCoderState(), nil)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("consumer: %v decisions=%v outcomes=%v", err, decisions, outcomes)
			}
			want := "open"
			if primary == "review" {
				want = "human_handoff"
			}
			if len(outcomes) != 2 || outcomes[0] != "selected" || outcomes[1] != want || decisions[0] != created.ID {
				t.Fatalf("outcomes=%v decisions=%v", outcomes, decisions)
			}
			if primary == "review" && (len(decisions) != 2 || decisions[1] != normal) {
				t.Fatalf("ordinary review did not resume: %v", decisions)
			}
			final, err := consumer.ShowFullBody(testCtx, created.ID)
			owner, ownerErr := consumer.Show(testCtx, created.ID)
			if err != nil || primary == "signoff" && (final.State != "open" || !strings.Contains(final.Body, "Decision: state owns")) || primary == "review" && (final.State != "hold" || ownerErr != nil || owner.Assignee != "user") {
				t.Fatalf("final: %+v %v", final, err)
			}
		})
	}
}
