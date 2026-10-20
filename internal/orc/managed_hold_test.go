package orc

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// This fixture models the agent's owned hold, separately from Orc containment.
type managedHoldTickets struct {
	*fakeCoderTickets
	actor        string
	reason       string
	holdErr      error
	appliedErr   error
	readinessErr error
	holds        []string
}

func (f *managedHoldTickets) HoldTicket(_ context.Context, id string, options ticketclient.MutationOptions) (ticketclient.MutationResult, error) {
	f.holds = append(f.holds, id)
	if f.holdErr != nil {
		return ticketclient.MutationResult{}, f.holdErr
	}
	ticket := f.shows[id]
	if ticket.Assignee != f.actor {
		return ticketclient.MutationResult{}, errors.New("lost ownership")
	}
	from := ticket.State
	ticket.State, ticket.Assignee = "hold", ""
	f.shows[id] = ticket
	f.reason = options.Handoff
	if f.appliedErr != nil {
		return ticketclient.MutationResult{}, f.appliedErr
	}
	return ticketclient.MutationResult{ID: id, State: "hold", FromState: from, Changed: true}, nil
}

func (f *managedHoldTickets) ShowReadiness(ctx context.Context, id string) (ticketclient.TicketDetail, error) {
	if f.readinessErr != nil {
		return ticketclient.TicketDetail{}, f.readinessErr
	}
	return f.fakeCoderTickets.ShowReadiness(ctx, id)
}

func TestManagedBlockedTurn(t *testing.T) {
	for _, role := range []string{"coder", "reviewer"} {
		for _, outcome := range []string{"held", "harness failure after hold", "hold failed", "hold uncertain", "hold uncertain after application", "observation failed", "readiness failed", "unexpected unchanged", "lost ownership"} {
			t.Run(role+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const first, second = "20261017-10001", "20261017-10002"
				const reason = "Sibling repository is read-only; operator must move this work to its owning repository."
				actor, queue, completed := "coder-1", "open", "review"
				if role == "reviewer" {
					actor, queue, completed = "reviewer-1", "review", "signoff"
				}
				claims := []ticketclient.Ticket{{ID: first, State: queue, Assignee: actor}, {ID: second, State: queue, Assignee: actor}}
				tickets := &managedHoldTickets{fakeCoderTickets: &fakeCoderTickets{shows: map[string]ticketclient.Ticket{first: claims[0], second: claims[1]}}, actor: actor}
				if role == "coder" {
					tickets.claims = claims
				} else {
					tickets.reviewClaims = claims
				}
				failure := errors.New(outcome)
				if outcome == "hold failed" || outcome == "hold uncertain" {
					tickets.holdErr = failure
				}
				if outcome == "hold uncertain after application" {
					tickets.appliedErr = failure
				}
				store := state.NewForRepository(t.TempDir(), "/repo")
				cleanup := &fakeCoderCleanup{}
				agent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "thread", StreamEndedNormally: true}}
				if outcome == "harness failure after hold" {
					agent.runErr = failure
				}
				runs := 0
				agent.onRun = func() {
					runs++
					if runs == 2 {
						// Completing the second turn and returning from Run is the barrier for
						// assertions that the first ticket was never reclaimed or mutated again.
						ticket := tickets.shows[second]
						ticket.State, ticket.Assignee = completed, ""
						tickets.shows[second] = ticket
						cancel()
						return
					}
					if outcome == "lost ownership" {
						ticket := tickets.shows[first]
						ticket.Assignee = "other"
						tickets.shows[first] = ticket
					}
					if outcome != "unexpected unchanged" {
						result, err := tickets.HoldTicket(ctx, first, ticketclient.MutationOptions{Handoff: reason})
						if err != nil {
							return
						}
						if result.ID != first || result.State != "hold" {
							t.Fatal("invalid hold result")
						}
					}
					if outcome == "observation failed" {
						tickets.showErr = failure
					}
					if outcome == "readiness failed" {
						tickets.readinessErr = failure
					}
				}
				// The harness reports failed/uncertain hold operations as turn errors.
				if tickets.holdErr != nil || tickets.appliedErr != nil || outcome == "lost ownership" {
					agent.runErr = failure
				}
				var err error
				if role == "coder" {
					config := coderConfig()
					err = RunCoder(ctx, config, tickets, agent, store, cleanup)
				} else {
					config := reviewerConfig()
					config.ReviewCompletion = "signoff"
					config.MaxBounces = 6
					err = RunReviewer(ctx, config, tickets, agent, store, cleanup)
				}
				if outcome == "held" {
					if !errors.Is(err, context.Canceled) || runs != 2 || tickets.waitCalls != 2 {
						t.Fatalf("err=%v runs=%d waits=%d", err, runs, tickets.waitCalls)
					}
					if got := []string{agent.runs[0].TicketID, agent.runs[1].TicketID}; !reflect.DeepEqual(got, []string{first, second}) {
						t.Fatalf("runs=%v", got)
					}
					loop, found, readErr := store.TicketLoop(context.Background(), first)
					if readErr != nil || !found || loop.Phase != state.TicketLoopHeld || loop.ClaimActor != "" || loop.BounceCount != 0 {
						t.Fatalf("loop=%#v found=%t err=%v", loop, found, readErr)
					}
				} else {
					if err == nil || runs != 1 || tickets.waitCalls != 1 {
						t.Fatalf("failure hidden: err=%v runs=%d waits=%d", err, runs, tickets.waitCalls)
					}
					if outcome != "unexpected unchanged" && outcome != "lost ownership" && !errors.Is(err, failure) {
						t.Fatalf("err=%v missing %v", err, failure)
					}
					if outcome == "unexpected unchanged" && !strings.Contains(err.Error(), "exited successfully but ticket") {
						t.Fatalf("unexpected lifecycle error=%v", err)
					}
				}
				if outcome == "held" || outcome == "hold uncertain after application" || outcome == "harness failure after hold" || outcome == "observation failed" || outcome == "readiness failed" {
					if tickets.shows[first].State != "hold" || tickets.reason != reason {
						t.Fatalf("held ticket=%#v reason=%q", tickets.shows[first], tickets.reason)
					}
				}
				if outcome == "lost ownership" && tickets.shows[first].Assignee != "other" {
					t.Fatal("changed another actor's claim")
				}
				if len(tickets.releases)+len(tickets.closes)+len(tickets.approves)+len(cleanup.tickets) != 0 {
					t.Fatal("blocked turn triggered release, completion, or cleanup")
				}
				if outcome != "unexpected unchanged" && !reflect.DeepEqual(tickets.holds, []string{first}) {
					t.Fatalf("hold attempts=%v", tickets.holds)
				}
			})
		}
	}
}
