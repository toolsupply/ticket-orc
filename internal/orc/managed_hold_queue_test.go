package orc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Both loops use the existing watch fixture. Its wait hook is a synchronous
// idle-dispatch barrier, so arrivals/cancellation need no elapsed-time waits.
type holdQueueFixture struct {
	*fakeAcceptanceTickets
	actor          string
	primaryQueue   string
	onFrontier     func()
	holdPages      int
	claims         int
	blockingClaims int
	mutations      int
	watched        bool
	activeErr      error
	frontierErr    error
	escalateErr    error
	acquireHook    func(string) bool
}

func (f *holdQueueFixture) StartChangeWatch(ctx context.Context, repository string) (ticketclient.ChangeWatcher, error) {
	watch, err := f.fakeAcceptanceTickets.StartChangeWatch(ctx, repository)
	f.watched = err == nil
	return watch, err
}

func (f *holdQueueFixture) HasActiveClaim(_ context.Context, queue string) (bool, error) {
	if !f.watched {
		return false, errors.New("ownership observed before watch READY")
	}
	if f.activeErr != nil {
		return false, f.activeErr
	}
	for _, item := range f.items {
		if item.State == queue && item.Assignee == f.actor {
			return true, nil
		}
	}
	return false, nil
}

func (f *holdQueueFixture) HoldFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	if !f.watched {
		return ticketclient.ListResult{}, errors.New("frontier observed before watch READY")
	}
	f.holdPages++
	if f.frontierErr != nil {
		return ticketclient.ListResult{}, f.frontierErr
	}
	var items []ticketclient.Ticket
	for _, item := range f.items {
		if item.State == "hold" && item.Assignee == "" && matchesQueueFilters(item, filters) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if f.onFrontier != nil {
		f.onFrontier()
	}
	if offset >= len(items) {
		return ticketclient.ListResult{}, nil
	}
	end := min(offset+limit, len(items))
	return ticketclient.ListResult{Items: items[offset:end], More: end < len(items)}, nil
}

func (f *holdQueueFixture) NextAndClaimReview(_ context.Context, filters ticketclient.QueueFilters) (*ticketclient.Ticket, error) {
	f.claims++
	for _, item := range f.items {
		if item.Assignee == f.actor && (item.State == "open" || item.State == "review") {
			if item.State != "review" || !matchesQueueFilters(item, filters) {
				return nil, errors.New("existing claim conflicts with review selection")
			}
			return &item, nil
		}
	}
	var ids []string
	for _, item := range f.items {
		if item.State == "review" && item.Assignee == "" && matchesQueueFilters(item, filters) {
			ids = append(ids, item.ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	sort.Strings(ids)
	item := f.items[ids[0]]
	item.Assignee = f.actor
	f.set(item.ID, item)
	return &item, nil
}

func (f *holdQueueFixture) WaitAndClaimReview(context.Context, ticketclient.QueueFilters) (ticketclient.Ticket, error) {
	f.blockingClaims++
	return ticketclient.Ticket{}, errors.New("blocking review wait must not be used with holds")
}

func (f *holdQueueFixture) Release(context.Context, string) (ticketclient.Transition, error) {
	f.mutations++
	return ticketclient.Transition{}, errors.New("hold assessment must not release claims")
}

func (f *holdQueueFixture) CloseTicket(context.Context, string) (ticketclient.Transition, error) {
	f.mutations++
	return ticketclient.Transition{}, errors.New("hold assessment must not close tickets")
}

func newHoldQueueFixture(queue string) *holdQueueFixture {
	return &holdQueueFixture{fakeAcceptanceTickets: &fakeAcceptanceTickets{items: make(map[string]ticketclient.Ticket), watch: newFakeAcceptanceWatch()}, actor: "worker-actor", primaryQueue: queue}
}

func (f *holdQueueFixture) put(id, lifecycle, owner string) {
	f.set(id, ticketclient.Ticket{ID: id, State: lifecycle, Assignee: owner, Tags: []string{"api"}})
}

func (f *holdQueueFixture) run(ctx context.Context, agent *fakeCoderHarness, store TicketState, cleanup TicketCleanup, sink EventSink, diagnostics *bytes.Buffer) error {
	filters := ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"excluded"}}
	if f.primaryQueue == "signoff" {
		config := acceptanceConfig()
		config.Actor, config.WorkerName = f.actor, "stable-worker"
		config.HoldQueue, config.HoldQueueFilters = true, filters
		config.EventSink, config.Diagnostics = sink, diagnostics
		return RunAcceptance(ctx, config, f, agent, store, cleanup)
	}
	config := reviewerConfig()
	config.Actor, config.WorkerName = f.actor, "stable-worker"
	config.RoleName = "custom-quality"
	config.HoldQueue, config.HoldQueueFilters, config.RepositoryID = true, filters, acceptanceTestRepositoryID
	config.QueueFilters = ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"skip-review"}}
	config.EventSink, config.Diagnostics = sink, diagnostics
	return RunReviewer(ctx, config, f, agent, store, cleanup)
}

func cancelAtHoldIdle(f *holdQueueFixture, cancel context.CancelFunc) {
	f.watch.onWait = func(int) error { cancel(); return context.Canceled }
}

func TestManagedHoldQueueDispatchOrderAndSuccessiveOutcomes(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, primary := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/primary=%t", queue, primary), func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				const first, second, normal = "20261025-10001", "20261025-10002", "20261025-10003"
				f.put(first, "hold", "")
				f.put(second, "hold", "")
				// Review skip tags must not disqualify holds.
				item := f.items[first]
				item.Tags = append(item.Tags, "skip-review")
				f.set(first, item)
				if primary {
					f.put(normal, queue, "")
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancelAtHoldIdle(f, cancel)
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				agent.onRun = func() {
					request := agent.runs[len(agent.runs)-1]
					switch request.TicketID {
					case first:
						if primary {
							f.put(first, "hold", "user")
						} else {
							f.put(first, "open", "")
						}
					case second:
						f.put(second, "open", "")
					case normal:
						f.put(normal, "signoff", "user")
					}
				}
				var events []Event
				err := f.run(ctx, agent, newFakeCoderState(), &fakeCoderCleanup{}, func(e Event) { events = append(events, e) }, &bytes.Buffer{})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("run: %v", err)
				}
				want := []string{first, second}
				if primary {
					want = []string{first, normal, second}
				}
				var got []string
				for _, request := range agent.runs {
					got = append(got, request.TicketID)
					if request.TicketID != normal {
						if !strings.HasPrefix(request.Prompt, HoldTurnPrompt(request.TicketID)) || strings.Contains(request.Prompt, "review_completion=") || strings.Contains(request.Prompt, "Make its acceptance decision") {
							t.Fatalf("hold prompt leaked primary instruction: %s", request.Prompt)
						}
						if !strings.Contains(request.Prompt, map[string]string{"review": "exercise reviewer authority", "signoff": "exercise architectural planning authority"}[queue]) {
							t.Fatal("missing role authority")
						}
					}
					if request.WorkerName != "stable-worker" || request.Actor != f.actor {
						t.Fatalf("worker identity changed: %#v", request)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("runs=%v want %v", got, want)
				}
				if !f.watch.closed || f.blockingClaims != 0 || f.mutations != 0 {
					t.Fatalf("watch closed=%v blocking=%d mutations=%d", f.watch.closed, f.blockingClaims, f.mutations)
				}
				if primary && f.items[first].Assignee != "user" {
					t.Fatal("human handoff was consumed again")
				}
			})
		}
	}
}

func TestManagedHoldQueueArrivalDuringIdle(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			f := newHoldQueueFixture(queue)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.watch.onWait = func(wait int) error {
				if wait == 1 {
					f.put("20261025-10001", "hold", "")
					f.watch.signal(true)
					return nil
				}
				cancel()
				return context.Canceled
			}
			agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}, onRun: func() { f.put("20261025-10001", "open", "") }}
			err := f.run(ctx, agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
			if !errors.Is(err, context.Canceled) || len(agent.runs) != 1 || f.watch.waits != 2 || !f.watch.closed {
				t.Fatalf("err=%v runs=%d waits=%d closed=%v", err, len(agent.runs), f.watch.waits, f.watch.closed)
			}
		})
	}
}

func TestManagedHoldQueueRechecksBeforeDispatch(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, change := range []string{"open", "queued", "closed", "assigned coder", "assigned user", "missing tag", "excluded tag"} {
			t.Run(queue+"/"+change, func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				const id = "20261025-10001"
				f.put(id, "hold", "")
				f.onFrontier = func() {
					item := f.items[id]
					switch change {
					case "assigned coder":
						item.Assignee = "coder"
					case "assigned user":
						item.Assignee = "user"
					case "missing tag":
						item.Tags = nil
					case "excluded tag":
						item.Tags = append(item.Tags, "excluded")
					default:
						item.State = change
					}
					f.set(id, item)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancelAtHoldIdle(f, cancel)
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				cleanup := &fakeCoderCleanup{}
				err := f.run(ctx, agent, newFakeCoderState(), cleanup, nil, &bytes.Buffer{})
				if !errors.Is(err, context.Canceled) || len(agent.runs) != 0 {
					t.Fatalf("err=%v unexpected runs=%d", err, len(agent.runs))
				}
				if (len(cleanup.tickets) == 1) != (change == "closed") {
					t.Fatalf("cleanup=%#v", cleanup.tickets)
				}
			})
		}
	}
}

func TestManagedHoldQueueFailedOutcomesStop(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, outcome := range []string{"incomplete turn", "nonzero exit", "harness failed", "harness failed after reopen", "harness failed after handoff", "ambiguous handoff", "post observation failed"} {
			t.Run(queue+"/"+outcome, func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				f.put("20261025-10001", "hold", "")
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				failure := errors.New("harness execution failed")
				if outcome == "nonzero exit" {
					agent.runRes.ExitCode = 7
				}
				if outcome == "incomplete turn" {
					agent.runRes.StreamEndedNormally = false
				}
				if strings.HasPrefix(outcome, "harness failed") {
					agent.runErr = failure
				}
				if outcome == "harness failed after reopen" {
					agent.onRun = func() { f.put("20261025-10001", "open", "") }
				}
				if outcome == "harness failed after handoff" {
					agent.onRun = func() { f.put("20261025-10001", "hold", "user") }
				}
				if outcome == "ambiguous handoff" {
					f.escalateErr = errors.New("mutation applied but persistence uncertain")
				}
				if outcome == "post observation failed" {
					f.showErr = 2
				}
				var events []Event
				diagnostics := &bytes.Buffer{}
				err := f.run(context.Background(), agent, newFakeCoderState(), nil, func(e Event) { events = append(events, e) }, diagnostics)
				wantMutations := 0
				if outcome == "ambiguous handoff" {
					wantMutations = 1
				}
				if err == nil || len(agent.runs) != 1 || f.watch.waits != 0 || !f.watch.closed || f.mutations != wantMutations {
					t.Fatalf("err=%v runs=%d waits=%d closed=%v mutations=%d", err, len(agent.runs), f.watch.waits, f.watch.closed, f.mutations)
				}
				if strings.HasPrefix(outcome, "harness failed") && !errors.Is(err, failure) {
					t.Fatalf("lost harness error: %v", err)
				}
			})
		}
	}
}

func TestManagedHoldQueueTerminalAndOtherOwnerOutcomes(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, outcome := range []string{"closed", "rejected", "open", "review", "assigned elsewhere"} {
			t.Run(queue+"/"+outcome, func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				const id = "20261025-10001"
				f.put(id, "hold", "")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancelAtHoldIdle(f, cancel)
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}, onRun: func() {
					if outcome == "assigned elsewhere" {
						f.put(id, "hold", "coder")
					} else {
						f.put(id, outcome, "elsewhere")
					}
				}}
				cleanup := &fakeCoderCleanup{}
				err := f.run(ctx, agent, newFakeCoderState(), cleanup, nil, &bytes.Buffer{})
				if !errors.Is(err, context.Canceled) || len(agent.runs) != 1 {
					t.Fatalf("err=%v runs=%d", err, len(agent.runs))
				}
				if (len(cleanup.tickets) == 1) != (outcome == "closed" || outcome == "rejected") {
					t.Fatalf("cleanup=%#v", cleanup.tickets)
				}
			})
		}
	}
}

func TestManagedHoldQueueRespectsActiveClaimsAndSessions(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, active := range []string{"open", "review"} {
			t.Run(queue+"/"+active, func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				f.put("20261025-10001", "hold", "")
				f.put("20261025-10002", active, f.actor)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancelAtHoldIdle(f, cancel)
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}, onRun: func() { f.put("20261025-10002", "signoff", "user") }}
				err := f.run(ctx, agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
				if queue == "review" && active == "review" {
					// Finish active review, then escalate the unresolved hold.
					if !errors.Is(err, context.Canceled) || len(agent.runs) != 2 || agent.runs[0].TicketID != "20261025-10002" {
						t.Fatalf("err=%v runs=%#v", err, agent.runs)
					}
				} else if len(agent.runs) != 0 || f.holdPages != 0 {
					t.Fatalf("unsafe dispatch: err=%v runs=%d frontiers=%d", err, len(agent.runs), f.holdPages)
				}
			})
		}
		t.Run(queue+"/stable role session", func(t *testing.T) {
			f := newHoldQueueFixture(queue)
			const id = "20261025-10001"
			f.put(id, "hold", "")
			store := newFakeCoderState()
			role := "reviewer"
			if queue == "signoff" {
				role = AcceptanceSessionRole
			}
			for _, r := range []string{role, "coder"} {
				store.sessions[coderSessionKey(id, r, "codex", "stable-worker")] = state.Session{Ticket: id, Role: r, Harness: "codex", Owner: "stable-worker", ID: r + "-session"}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancelAtHoldIdle(f, cancel)
			agent := &fakeCoderHarness{resumeRes: harness.RunResult{SessionID: role + "-session", StreamEndedNormally: true}}
			err := f.run(ctx, agent, store, nil, nil, &bytes.Buffer{})
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(agent.resumes, []string{role + "-session"}) || len(store.sets) != 1 || store.sets[0].Role != role {
				t.Fatalf("err=%v resumes=%v sessions=%v", err, agent.resumes, store.sets)
			}
		})
	}
}

func TestManagedHoldQueueWatchStartupAndObservationFailures(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, phase := range []string{"startup failure", "startup cancellation", "idle cancellation", "ownership failure", "frontier failure"} {
			t.Run(queue+"/"+phase, func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				f.put("20261025-10001", "hold", "")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				switch phase {
				case "startup failure":
					f.startErr = errors.New("watch startup failed")
				case "startup cancellation":
					f.start = func(context.Context) error { cancel(); return context.Canceled }
				case "idle cancellation":
					f.items = make(map[string]ticketclient.Ticket)
					cancelAtHoldIdle(f, cancel)
				case "ownership failure":
					f.activeErr = errors.New("ownership unavailable")
				case "frontier failure":
					f.frontierErr = errors.New("frontier unavailable")
				}
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				var events []Event
				err := f.run(ctx, agent, newFakeCoderState(), nil, func(e Event) { events = append(events, e) }, &bytes.Buffer{})
				if err == nil || len(agent.runs) != 0 {
					t.Fatalf("err=%v runs=%d", err, len(agent.runs))
				}
				if strings.HasPrefix(phase, "startup") {
					if len(events) != 0 || f.holdPages != 0 {
						t.Fatalf("work/readiness before READY: %v", events)
					}
				} else if !f.watch.closed {
					t.Fatal("watch not reaped")
				}
			})
		}
	}
}

func (f *holdQueueFixture) AcquireHold(_ context.Context, id string, filters ticketclient.QueueFilters) (*ticketclient.Ticket, error) {
	if f.acquireHook != nil && !f.acquireHook(id) {
		return nil, nil
	}
	item := f.items[id]
	if item.State != "hold" || item.Assignee != "" || !matchesQueueFilters(item, filters) {
		return nil, nil
	}
	item.Assignee = f.actor
	f.set(id, item)
	return &item, nil
}

func (f *holdQueueFixture) OwnedHold(context.Context) (*ticketclient.Ticket, error) {
	for _, item := range f.items {
		if item.State == "hold" && item.Assignee == f.actor {
			return &item, nil
		}
	}
	return nil, nil
}

func TestManagedHoldStaleOwnershipNeverStartsHarness(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			f := newHoldQueueFixture(queue)
			const id = "20261025-10001"
			f.put(id, "hold", f.actor)
			agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
			err := f.run(context.Background(), agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "operator reconciliation") || len(agent.runs) != 0 || f.items[id].Assignee != f.actor {
				t.Fatalf("stale ownership: runs=%d err=%v ticket=%+v", len(agent.runs), err, f.items[id])
			}
		})
	}
}

func (f *holdQueueFixture) EscalateHold(_ context.Context, id, owner, explanation string) (ticketclient.Ticket, error) {
	f.mutations++
	if f.escalateErr != nil {
		return ticketclient.Ticket{}, f.escalateErr
	}
	item := f.items[id]
	if item.State == "hold" && item.Assignee == owner {
		item.Assignee = "user"
		f.set(id, item)
	}
	return item, nil
}

func TestManagedHoldEscalatesOnceAndContinues(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		for _, unassigned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unassigned=%t", queue, unassigned), func(t *testing.T) {
				f := newHoldQueueFixture(queue)
				const held, normal = "20261025-10001", "20261025-10002"
				f.put(held, "hold", "")
				f.put(normal, queue, "")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				f.watch.onWait = func(wait int) error {
					if wait < 4 {
						f.watch.signal(true)
						return nil
					}
					cancel()
					return context.Canceled
				}
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				agent.onRun = func() {
					switch agent.runs[len(agent.runs)-1].TicketID {
					case held:
						if unassigned {
							f.put(held, "hold", "")
						}
					case normal:
						f.put(normal, "signoff", "user")
					}
				}
				var events []Event
				diagnostics := &bytes.Buffer{}
				err := f.run(ctx, agent, newFakeCoderState(), nil, func(e Event) { events = append(events, e) }, diagnostics)
				if !errors.Is(err, context.Canceled) || len(agent.runs) != 2 || f.mutations != 1 || f.items[held].State != "hold" || f.items[held].Assignee != "user" || !strings.Contains(diagnostics.String(), "hold_escalated") {
					t.Fatalf("err=%v runs=%v mutations=%d held=%+v events=%v", err, agent.runs, f.mutations, f.items[held], events)
				}
				// Fresh worker/state/watch simulate restart: exclusion lives in Ticket.
				f.watch = newFakeAcceptanceWatch()
				ctx, cancelRestart := context.WithCancel(t.Context())
				defer cancelRestart()
				cancelAtHoldIdle(f, cancelRestart)
				restarted := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				err = f.run(ctx, restarted, newFakeCoderState(), nil, nil, &bytes.Buffer{})
				if !errors.Is(err, context.Canceled) || len(restarted.runs) != 0 || f.mutations != 1 {
					t.Fatalf("reassessment after restart: err=%v runs=%v mutations=%d", err, restarted.runs, f.mutations)
				}
			})
		}
	}
}

// Keep both inputs nonempty at every boundary, including after the final turn.
func TestManagedHoldAlternatesPopulatedBacklogs(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			f := newHoldQueueFixture(queue)
			for i := 0; i < 5; i++ {
				f.put(fmt.Sprintf("20261025-11%03d", i), "hold", "")
				f.put(fmt.Sprintf("20261025-12%03d", i), queue, "")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
			agent.onRun = func() {
				request := agent.runs[len(agent.runs)-1]
				index := len(agent.runs) - 1
				want := fmt.Sprintf("20261025-%02d%03d", 11+index%2, index/2)
				if request.TicketID != want {
					t.Fatalf("turn %d: got %s want %s", index, request.TicketID, want)
				}
				// Leave holds unresolved: clean completion must escalate once, then yield.
				if index%2 == 1 {
					f.put(request.TicketID, "signoff", "user")
				}
				if len(agent.runs) == 8 {
					cancel()
				}
			}
			err := f.run(ctx, agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
			if !errors.Is(err, context.Canceled) || len(agent.runs) != 8 || f.mutations != 4 {
				t.Fatalf("err=%v turns=%d escalations=%d", err, len(agent.runs), f.mutations)
			}
		})
	}
}

func TestManagedLostHoldRaceYieldsToPrimary(t *testing.T) {
	for _, queue := range []string{"review", "signoff"} {
		t.Run(queue, func(t *testing.T) {
			f := newHoldQueueFixture(queue)
			f.put("20261025-10001", "hold", "")
			f.put("20261025-10002", queue, "")
			attempts := 0
			// Even a still-visible candidate must not be retried before primary work.
			f.acquireHook = func(string) bool { attempts++; return false }
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}, onRun: func() {
				f.put("20261025-10002", "signoff", "user")
				cancel()
			}}
			err := f.run(ctx, agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
			if !errors.Is(err, context.Canceled) || attempts != 1 || len(agent.runs) != 1 || agent.runs[0].TicketID != "20261025-10002" {
				t.Fatalf("err=%v attempts=%d runs=%v", err, attempts, agent.runs)
			}
		})
	}
}

func TestManagedArchitectActiveClaimBlocksBothInputs(t *testing.T) {
	for _, active := range []string{"open", "review"} {
		for _, primaryPreferred := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/primaryPreferred=%t", active, primaryPreferred), func(t *testing.T) {
				const warmup, held, owned, primary = "20261025-10000", "20261025-10001", "20261025-10002", "20261025-10003"
				f := newHoldQueueFixture("signoff")
				f.put(held, "hold", "")
				f.put(primary, "signoff", "")
				putClaim := func() {
					// Ownership is actor-wide, even outside this worker's tags.
					f.set(owned, ticketclient.Ticket{ID: owned, State: active, Assignee: f.actor, Tags: []string{"excluded"}})
				}
				initialTurns := 0
				if primaryPreferred {
					f.put(warmup, "hold", "")
					initialTurns = 1
				} else {
					putClaim()
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}}
				agent.onRun = func() {
					id := agent.runs[len(agent.runs)-1].TicketID
					if id == warmup {
						f.put(warmup, "hold", "user")
						putClaim()
						return
					}
					if f.items[owned].Assignee == f.actor {
						t.Fatalf("dispatched %s while %s claim is active", id, active)
					}
					f.put(id, "signoff", "user")
				}
				f.watch.onWait = func(wait int) error {
					if wait <= 2 {
						if len(agent.runs) != initialTurns || f.calls != 0 {
							t.Fatalf("observed/dispatched fresh work while claimed: runs=%v signoff queries=%d", agent.runs, f.calls)
						}
						if wait == 1 {
							f.watch.signal(false) // Nonmaterial events cannot bypass ownership.
						} else {
							f.put(owned, "signoff", "user") // Authoritative departure.
							f.watch.signal(true)
						}
						return nil
					}
					cancel()
					return context.Canceled
				}
				err := f.run(ctx, agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
				var got []string
				for _, request := range agent.runs {
					got = append(got, request.TicketID)
				}
				want := []string{held, primary}
				if primaryPreferred {
					want = []string{warmup, primary, held}
				}
				if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, want) || !f.watch.closed {
					t.Fatalf("err=%v runs=%v want=%v closed=%v", err, got, want, f.watch.closed)
				}
			})
		}
	}
}

func TestManagedArchitectPrimaryPreferenceFailsClosedOnClaimObservation(t *testing.T) {
	f := newHoldQueueFixture("signoff")
	const warmup, primary = "20261025-10000", "20261025-10003"
	f.put(warmup, "hold", "")
	f.put(primary, "signoff", "")
	failure := errors.New("active claim observation unavailable")
	agent := &fakeCoderHarness{runRes: harness.RunResult{StreamEndedNormally: true}, onRun: func() {
		f.put(warmup, "hold", "user")
		f.activeErr = failure
	}}
	err := f.run(t.Context(), agent, newFakeCoderState(), nil, nil, &bytes.Buffer{})
	if !errors.Is(err, failure) || len(agent.runs) != 1 || f.calls != 0 || !f.watch.closed {
		t.Fatalf("err=%v runs=%v signoff queries=%d closed=%v", err, agent.runs, f.calls, f.watch.closed)
	}
}
