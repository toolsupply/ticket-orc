package orc

import (
	"context"
	"errors"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestManagedTurnsCarryMutableCodexServiceTier(t *testing.T) {
	const ticketID = "20261022-28668"
	for _, role := range []string{"coder", "reviewer", AcceptanceSessionRole} {
		t.Run(role, func(t *testing.T) {
			store := newFakeCoderState()
			agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread-1"}, resumeRes: harness.RunResult{SessionID: "thread-1"}}
			for _, tier := range []string{"fast", "default"} {
				var err error
				switch role {
				case "coder":
					config := coderConfig()
					config.WorkerName = "worker"
					config.CodexServiceTier = tier
					_, err = runCoderTurn(context.Background(), config, ticketID, agent, store)
				case "reviewer":
					config := reviewerConfig()
					config.WorkerName = "worker"
					config.CodexServiceTier = tier
					_, err = runReviewerTurn(context.Background(), config, ticketID, agent, store)
				case AcceptanceSessionRole:
					config := acceptanceConfig()
					config.WorkerName = "worker"
					config.CodexServiceTier = tier
					err = runAcceptanceTurn(context.Background(), config, ticketclient.Ticket{ID: ticketID}, agent, store)
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := agent.runs[len(agent.runs)-1].CodexServiceTier; got != tier {
					t.Fatalf("request tier=%q, want %q", got, tier)
				}
			}
			if len(agent.resumes) != 1 || agent.resumes[0] != "thread-1" {
				t.Fatalf("tier change must reuse session: %v", agent.resumes)
			}
			if len(store.history) != 0 {
				t.Fatalf("tier change superseded session: %#v", store.history)
			}
			if got := store.sessions[coderSessionKey(ticketID, role, "codex", "worker")]; got.ID != "thread-1" {
				t.Fatalf("session identity changed: %#v", got)
			}
		})
	}
}

type tierPreflightHarness struct {
	fakeCoderHarness
	config harness.PreflightConfig
	err    error
}

func (f *tierPreflightHarness) Capabilities() harness.Capabilities { return harness.Capabilities{} }
func (f *tierPreflightHarness) Validate(config harness.PreflightConfig) error {
	f.config = config
	return f.err
}

func TestManagedPreflightCarriesCodexServiceTier(t *testing.T) {
	for _, role := range []string{"coder", "reviewer", AcceptanceSessionRole} {
		t.Run(role, func(t *testing.T) {
			sentinel := errors.New("stop at preflight before Ticket access")
			agent := &tierPreflightHarness{err: sentinel}
			var err error
			switch role {
			case "coder":
				config := coderConfig()
				config.CodexServiceTier = "fast"
				err = RunCoder(context.Background(), config, &fakeCoderTickets{}, agent, newFakeCoderState(), &fakeCoderCleanup{})
			case "reviewer":
				config := reviewerConfig()
				config.CodexServiceTier = "fast"
				err = RunReviewer(context.Background(), config, &fakeCoderTickets{}, agent, newFakeCoderState(), &fakeCoderCleanup{})
			case AcceptanceSessionRole:
				config := acceptanceConfig()
				config.CodexServiceTier = "fast"
				err = RunAcceptance(context.Background(), config, &fakeAcceptanceTickets{}, agent, newFakeCoderState(), &fakeCoderCleanup{})
			}
			if !errors.Is(err, sentinel) || agent.config.CodexServiceTier != "fast" {
				t.Fatalf("preflight=%#v err=%v", agent.config, err)
			}
		})
	}
}
