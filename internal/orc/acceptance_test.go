package orc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const acceptanceTestRepositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"

type fakeAcceptanceTickets struct {
	mu       sync.Mutex
	items    map[string]ticketclient.Ticket
	first    []ticketclient.Ticket
	calls    int
	watch    *fakeAcceptanceWatch
	showIDs  []string
	showErr  int
	start    func(context.Context) error
	startErr error
}

func (f *fakeAcceptanceTickets) Show(_ context.Context, id string) (ticketclient.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.showIDs = append(f.showIDs, id)
	if len(f.showIDs) == f.showErr {
		return ticketclient.Ticket{}, errors.New("authoritative observation failed")
	}
	item, ok := f.items[id]
	if !ok {
		return ticketclient.Ticket{}, fmt.Errorf("ticket %s missing", id)
	}
	return item, nil
}

func (f *fakeAcceptanceTickets) set(id string, ticket ticketclient.Ticket) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[id] = ticket
}

func (f *fakeAcceptanceTickets) SignoffFrontier(_ context.Context, filters ticketclient.QueueFilters, limit, offset int) (ticketclient.ListResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 && f.first != nil {
		return ticketclient.ListResult{Items: append([]ticketclient.Ticket(nil), f.first...), More: true}, nil
	}
	var items []ticketclient.Ticket
	for _, item := range f.items {
		if eligibleAcceptanceEntry(item, filters) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if offset >= len(items) {
		return ticketclient.ListResult{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return ticketclient.ListResult{Items: append([]ticketclient.Ticket(nil), items[offset:end]...), More: end < len(items)}, nil
}

func (f *fakeAcceptanceTickets) StartChangeWatch(ctx context.Context, repositoryID string) (ticketclient.ChangeWatcher, error) {
	if repositoryID != acceptanceTestRepositoryID {
		return nil, errors.New("unexpected repository ID")
	}
	if f.start != nil {
		if err := f.start(ctx); err != nil {
			return nil, err
		}
	}
	if f.startErr != nil {
		return nil, f.startErr
	}
	return f.watch, nil
}

type fakeAcceptanceWatch struct {
	changed chan bool
	mu      sync.Mutex
	waits   int
	closed  bool
	onWait  func(int) error
}

func newFakeAcceptanceWatch() *fakeAcceptanceWatch {
	return &fakeAcceptanceWatch{changed: make(chan bool, 1)}
}

func (*fakeAcceptanceWatch) Check() error { return nil }

func (w *fakeAcceptanceWatch) WaitForChange(ctx context.Context) (bool, error) {
	w.mu.Lock()
	w.waits++
	wait := w.waits
	onWait := w.onWait
	w.mu.Unlock()
	if onWait != nil {
		if err := onWait(wait); err != nil {
			return false, err
		}
	}
	select {
	case material := <-w.changed:
		return material, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (w *fakeAcceptanceWatch) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

func (w *fakeAcceptanceWatch) signal(material bool) {
	select {
	case w.changed <- material:
	default:
	}
}

func TestRunAcceptanceSweepsBeyondFirstBoundedFrontierWithoutRepeat(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	tickets := &fakeAcceptanceTickets{items: make(map[string]ticketclient.Ticket), watch: watch}
	want := make([]string, 25)
	for i := range want {
		id := fmt.Sprintf("20260919-%05d", 10001+i)
		want[i] = id
		tickets.items[id] = ticketclient.Ticket{ID: id, State: "signoff", Tags: []string{"api"}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch.onWait = func(int) error {
		cancel()
		return context.Canceled
	}
	harness := &fakeCoderHarness{onRun: func() {}}
	var got []string
	harness.onRun = func() {
		harness.mu.Lock()
		got = append(got, harness.runs[len(harness.runs)-1].TicketID)
		harness.mu.Unlock()
	}
	err := RunAcceptance(ctx, acceptanceConfig(), tickets, harness, newFakeCoderState(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error = %v, want cancellation after complete sweep", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("acceptance turns = %v, want all 25 ordered entries once", got)
	}
	if watch.waits != 1 || !watch.closed {
		t.Fatalf("watch waits=%d closed=%v, want one wait after sweep and closed process", watch.waits, watch.closed)
	}
	for _, request := range harness.runs {
		if request.TicketID == "" || request.Actor != "architect" || request.Role != "architect-api" {
			t.Fatalf("acceptance request = %#v; want exact ticket and architect defaults", request)
		}
		assertAcceptanceHoldGuidance(t, request.Prompt)
		for _, wantText := range []string{"exception to claim-before-work", request.TicketID, "do not claim the ticket", "Do not select, claim, implement, review, or make an acceptance decision for a second ticket", "one atomic Ticket command", "reassign --to user", "mutation_applied=true"} {
			if !containsText(request.Prompt, wantText) {
				t.Fatalf("acceptance prompt %q missing %q", request.Prompt, wantText)
			}
		}
	}
}

func TestRunAcceptanceComposesConfiguredPromptWithRequiredBoundary(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	id := "20260919-10001"
	tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{id: {ID: id, State: "signoff", Tags: []string{"api"}}}, watch: watch}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := &fakeCoderHarness{}
	harness.onRun = cancel
	config := acceptanceConfig()
	config.TicketPrompt = "Check the API outcome for {{ticket}}."
	err := RunAcceptance(ctx, config, tickets, harness, newFakeCoderState(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error = %v, want cancellation after one turn", err)
	}
	if len(harness.runs) != 1 {
		t.Fatalf("harness runs = %d, want one", len(harness.runs))
	}
	prompt := harness.runs[0].Prompt
	if strings.Contains(prompt, "outstanding holds") {
		t.Fatal("default guidance replaced the configured acceptance prompt")
	}
	for _, want := range []string{
		"Check the API outcome for " + id,
		"exception to claim-before-work",
		"do not claim the ticket",
		"The dispatched signoff ticket is " + id,
		"Reread it immediately before lifecycle actions",
		"Ticket-valid lifecycle outcome",
		"Do not treat leaving it unassigned in signoff as a successful resolution",
		"Do not select, claim, implement, review, or make an acceptance decision for a second ticket",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("composed prompt %q missing %q", prompt, want)
		}
	}
}

func TestRunAcceptanceEmitsReadyOnlyAfterWatchStartup(t *testing.T) {
	for _, failStartup := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready after watch baseline", true: "no ready on watch failure"}[failStartup], func(t *testing.T) {
			watch := newFakeAcceptanceWatch()
			tickets := &fakeAcceptanceTickets{watch: watch, startErr: nil}
			started := make(chan struct{})
			release := make(chan struct{})
			tickets.start = func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					if failStartup {
						return errors.New("watch baseline failed")
					}
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events := make(chan Event, 2)
			done := make(chan error, 1)
			config := acceptanceConfig()
			config.EventSink = func(event Event) { events <- event }
			go func() { done <- RunAcceptance(ctx, config, tickets, &fakeCoderHarness{}, newFakeCoderState(), nil) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("watch startup did not reach its gate")
			}
			select {
			case event := <-events:
				t.Fatalf("event before Ticket watch READY: %#v", event)
			default:
			}
			close(release)
			if failStartup {
				select {
				case err := <-done:
					if err == nil || !strings.Contains(err.Error(), "watch baseline failed") {
						t.Fatalf("RunAcceptance error = %v, want watch startup failure", err)
					}
				case <-time.After(time.Second):
					t.Fatal("RunAcceptance did not return after watch startup failure")
				}
				select {
				case event := <-events:
					t.Fatalf("ready event on failed startup: %#v", event)
				default:
				}
				return
			}
			select {
			case event := <-events:
				if event.Type != WorkerReadyEventType {
					t.Fatalf("first startup event = %#v, want readiness", event)
				}
			case <-time.After(time.Second):
				t.Fatal("successful watch startup did not emit readiness")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("RunAcceptance did not stop after cancellation")
			}
			if !watch.closed {
				t.Fatal("Ticket watch was not closed after cancellation")
			}
		})
	}
}

func TestRunAcceptanceSuppressesUnchangedEntryAfterChangeDuringHarnessTurn(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	// A READY watch may buffer an event during the turn. It triggers an
	// authoritative stale-target check but cannot rearm an unchanged entry.
	id := "20260919-10001"
	tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{id: {ID: id, State: "signoff", Tags: []string{"api"}}}, watch: watch}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := &fakeCoderHarness{onRun: func() { watch.signal(true) }}
	watch.onWait = func(wait int) error {
		if wait == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	err := RunAcceptance(ctx, acceptanceConfig(), tickets, harness, newFakeCoderState(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error = %v, want cancellation", err)
	}
	if len(harness.runs) != 1 || watch.waits != 2 || !watch.closed {
		t.Fatalf("runs=%d waits=%d closed=%v; want one stale check and a reaped watch", len(harness.runs), watch.waits, watch.closed)
	}
}

func TestRunAcceptanceDoesNotRearmUnchangedEntryForRepositoryChanges(t *testing.T) {
	for _, test := range []struct {
		name          string
		materialFirst bool
		wantRuns      int
	}{
		{name: "unchanged result does not immediately repeat", wantRuns: 1},
		{name: "material repository change does not rearm stale entry", materialFirst: true, wantRuns: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			watch := newFakeAcceptanceWatch()
			id := "20260919-10001"
			tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{id: {ID: id, State: "signoff", Tags: []string{"api"}}}, watch: watch}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch.onWait = func(wait int) error {
				if wait == 2 {
					cancel()
					return context.Canceled
				}
				return nil
			}
			harness := &fakeCoderHarness{}
			harness.onRun = func() {
				harness.mu.Lock()
				runs := len(harness.runs)
				harness.mu.Unlock()
				if runs == 1 {
					watch.signal(test.materialFirst)
				} else {
					watch.signal(false)
				}
			}
			err := RunAcceptance(ctx, acceptanceConfig(), tickets, harness, newFakeCoderState(), nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunAcceptance error = %v, want cancellation after wait barriers", err)
			}
			if len(harness.runs) != test.wantRuns || !watch.closed {
				t.Fatalf("runs=%d watch closed=%v; want %d runs and a reaped watch", len(harness.runs), watch.closed, test.wantRuns)
			}
		})
	}
}

func TestRunAcceptanceSuppressesStaleEntriesAndContinuesFilteredFrontier(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	first := "20260919-10001"
	second := "20260919-10002"
	tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{
		first:  {ID: first, State: "signoff", Tags: []string{"api"}},
		second: {ID: second, State: "signoff", Tags: []string{"api"}},
	}, watch: watch}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch.onWait = func(wait int) error {
		if wait <= 3 {
			watch.signal(true)
			return nil
		}
		cancel()
		return context.Canceled
	}
	harness := &fakeCoderHarness{}
	events := make([]Event, 0)
	config := acceptanceConfig()
	config.EventSink = func(event Event) { events = append(events, event) }
	err := RunAcceptance(ctx, config, tickets, harness, newFakeCoderState(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error = %v, want cancellation after repeated material changes", err)
	}
	want := []string{first, second}
	var got []string
	for _, request := range harness.runs {
		got = append(got, request.TicketID)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("acceptance turns = %v, want both eligible entries considered once", got)
	}
	if watch.waits != 4 || !watch.closed {
		t.Fatalf("watch waits=%d closed=%v, want four change waits and a reaped watch", watch.waits, watch.closed)
	}
	if len(events) != 5 || events[2].State != "acceptance_stale" || events[4].State != "acceptance_stale" {
		t.Fatalf("acceptance events=%#v; want one stale result for each selected ticket", events)
	}
	for _, id := range []string{first, second} {
		count := 0
		for _, shown := range tickets.showIDs {
			if shown == id {
				count++
			}
		}
		if count != 5 {
			t.Errorf("Ticket showed stale ID %s %d times, want pre/post turn plus three material-change checks", id, count)
		}
	}
}

func TestRunAcceptanceRearmsWhenStaleTicketLifecycleOrOwnershipChanges(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*ticketclient.Ticket, bool)
	}{
		{name: "lifecycle", edit: func(ticket *ticketclient.Ticket, changed bool) {
			if changed {
				ticket.State = "signoff"
			} else {
				ticket.State = "review"
			}
		}},
		{name: "ownership", edit: func(ticket *ticketclient.Ticket, changed bool) {
			if changed {
				ticket.Assignee = ""
			} else {
				ticket.Assignee = "reviewer"
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			watch := newFakeAcceptanceWatch()
			id := "20260919-10001"
			tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{
				id: {ID: id, State: "signoff", Tags: []string{"api"}},
			}, watch: watch}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch.onWait = func(wait int) error {
				switch wait {
				case 1, 2:
					item := tickets.items[id]
					test.edit(&item, wait == 2)
					tickets.set(id, item)
					watch.signal(true)
				case 3:
					cancel()
					return context.Canceled
				}
				return nil
			}
			harness := &fakeCoderHarness{}
			events := make([]Event, 0)
			config := acceptanceConfig()
			config.EventSink = func(event Event) { events = append(events, event) }
			err := RunAcceptance(ctx, config, tickets, harness, newFakeCoderState(), nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunAcceptance error=%v, want cancellation after rearm", err)
			}
			if len(harness.runs) != 2 || harness.runs[0].TicketID != id || harness.runs[1].TicketID != id || !watch.closed {
				t.Fatalf("runs=%#v watch closed=%v; want one rearmed turn for %s", harness.runs, watch.closed, id)
			}
			staleEvents := 0
			for _, event := range events {
				if event.State == "acceptance_stale" {
					staleEvents++
				}
			}
			if staleEvents != 2 {
				t.Fatalf("acceptance events=%#v; want each unchanged turn classified stale", events)
			}
		})
	}
}

func TestRunAcceptanceKeepsStaleEntrySuppressedAcrossTagFilterChanges(t *testing.T) {
	for _, test := range []struct {
		name    string
		filters ticketclient.QueueFilters
		edit    func(*ticketclient.Ticket, bool)
	}{
		{
			name:    "required tag removal and restoration",
			filters: ticketclient.QueueFilters{Tags: []string{"api"}},
			edit: func(ticket *ticketclient.Ticket, changed bool) {
				if changed {
					ticket.Tags = []string{"api"}
				} else {
					ticket.Tags = []string{"frontend"}
				}
			},
		},
		{
			name:    "excluded tag add and removal",
			filters: ticketclient.QueueFilters{Tags: []string{"api"}, WithoutTags: []string{"blocked"}},
			edit: func(ticket *ticketclient.Ticket, changed bool) {
				if changed {
					ticket.Tags = []string{"api"}
				} else {
					ticket.Tags = []string{"api", "blocked"}
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			watch := newFakeAcceptanceWatch()
			id := "20260919-10001"
			tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{
				id: {ID: id, State: "signoff", Tags: []string{"api"}},
			}, watch: watch}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch.onWait = func(wait int) error {
				switch wait {
				case 1, 2:
					item := tickets.items[id]
					test.edit(&item, wait == 2)
					tickets.set(id, item)
					watch.signal(true)
				case 3:
					cancel()
					return context.Canceled
				}
				return nil
			}
			harness := &fakeCoderHarness{}
			config := acceptanceConfig()
			config.QueueFilters = test.filters
			err := RunAcceptance(ctx, config, tickets, harness, newFakeCoderState(), nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunAcceptance error=%v, want cancellation after tag-only changes", err)
			}
			if len(harness.runs) != 1 || harness.runs[0].TicketID != id || !watch.closed {
				t.Fatalf("runs=%#v watch closed=%v; want one stale turn across selector changes", harness.runs, watch.closed)
			}
			if watch.waits != 3 {
				t.Fatalf("watch waits=%d, want two material changes and final cancellation", watch.waits)
			}
			shows := 0
			for _, shown := range tickets.showIDs {
				if shown == id {
					shows++
				}
			}
			if shows != 4 {
				t.Fatalf("ticket %s Show count=%d, want pre/post turn plus two material-change observations", id, shows)
			}
		})
	}
}

func TestRunAcceptanceClassifiesPostTurnOutcomes(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       string
		assignee    string
		wantEvent   string
		wantCleanup bool
		runErr      string
	}{
		{name: "unchanged", state: "signoff", wantEvent: "acceptance_stale"},
		{name: "unchanged after harness error", state: "signoff", wantEvent: "acceptance_stale", runErr: "harness failed"},
		{name: "human handoff", state: "signoff", assignee: "user", wantEvent: "human_handoff"},
		{name: "other owner", state: "signoff", assignee: "reviewer", wantEvent: "assigned_elsewhere"},
		{name: "review return", state: "review", wantEvent: "review"},
		{name: "rejected", state: "rejected", wantEvent: "rejected", wantCleanup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			watch := newFakeAcceptanceWatch()
			id := "20260919-10001"
			tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{
				id: {ID: id, State: "signoff", Tags: []string{"api"}},
			}, watch: watch}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch.onWait = func(int) error {
				cancel()
				return context.Canceled
			}
			harness := &fakeCoderHarness{}
			if test.runErr != "" {
				harness.runErr = errors.New(test.runErr)
			}
			harness.onRun = func() {
				item := tickets.items[id]
				item.State = test.state
				item.Assignee = test.assignee
				tickets.set(id, item)
			}
			var events []Event
			config := acceptanceConfig()
			config.EventSink = func(event Event) { events = append(events, event) }
			cleanup := &fakeCoderCleanup{}
			err := RunAcceptance(ctx, config, tickets, harness, newFakeCoderState(), cleanup)
			if test.runErr == "" && !errors.Is(err, context.Canceled) {
				t.Fatalf("RunAcceptance error=%v, want cancellation after one outcome", err)
			}
			if test.runErr != "" && (err == nil || !strings.Contains(err.Error(), test.runErr)) {
				t.Fatalf("RunAcceptance error=%v, want harness error %q", err, test.runErr)
			}
			if len(harness.runs) != 1 || !watch.closed {
				t.Fatalf("runs=%d watch closed=%v; want one turn and a reaped watch", len(harness.runs), watch.closed)
			}
			if len(events) != 3 || events[2].State != test.wantEvent {
				t.Fatalf("acceptance events=%#v; want final state %q", events, test.wantEvent)
			}
			if test.wantCleanup != (len(cleanup.tickets) == 1) {
				t.Fatalf("terminal cleanup tickets=%#v, wantCleanup=%t", cleanup.tickets, test.wantCleanup)
			}
		})
	}
}

func TestRunAcceptanceCleansTerminalStaleEntryAfterMaterialChange(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	id := "20260919-10001"
	tickets := &fakeAcceptanceTickets{items: map[string]ticketclient.Ticket{
		id: {ID: id, State: "signoff", Tags: []string{"api"}},
	}, watch: watch}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch.onWait = func(wait int) error {
		if wait == 1 {
			item := tickets.items[id]
			item.State = ticketclient.StateClosed
			tickets.set(id, item)
			watch.signal(true)
			return nil
		}
		cancel()
		return context.Canceled
	}
	harness := &fakeCoderHarness{}
	cleanup := &fakeCoderCleanup{}
	err := RunAcceptance(ctx, acceptanceConfig(), tickets, harness, newFakeCoderState(), cleanup)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error=%v, want cancellation after terminal observation", err)
	}
	if len(harness.runs) != 1 || len(cleanup.tickets) != 1 || cleanup.tickets[0].ID != id || !watch.closed {
		t.Fatalf("runs=%d cleanup=%#v watch closed=%v; want terminal cleanup and no repeated turn", len(harness.runs), cleanup.tickets, watch.closed)
	}
}

func TestRunAcceptanceSkipsChangedEntryBeforeDispatch(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	id := "20260919-10001"
	otherID := "20260919-10002"
	tickets := &fakeAcceptanceTickets{first: []ticketclient.Ticket{{ID: id, State: "signoff", Tags: []string{"api"}}}, items: map[string]ticketclient.Ticket{
		id:      {ID: id, State: "open", Tags: []string{"api"}},
		otherID: {ID: otherID, State: "signoff", Tags: []string{"api"}},
	}, watch: watch}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch.onWait = func(int) error {
		cancel()
		return context.Canceled
	}
	harness := &fakeCoderHarness{}
	err := RunAcceptance(ctx, acceptanceConfig(), tickets, harness, newFakeCoderState(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunAcceptance error = %v, want cancellation", err)
	}
	if len(harness.runs) != 1 || harness.runs[0].TicketID != otherID {
		t.Fatalf("acceptance turns = %#v; want only current eligible entry %s", harness.runs, otherID)
	}
}

func TestRunAcceptanceReobservesTicketAfterFailedHarnessTurn(t *testing.T) {
	watch := newFakeAcceptanceWatch()
	id := "20260919-10001"
	tickets := &fakeAcceptanceTickets{
		items:   map[string]ticketclient.Ticket{id: {ID: id, State: "signoff", Tags: []string{"api"}}},
		watch:   watch,
		showErr: 2,
	}
	harness := &fakeCoderHarness{runErr: errors.New("harness failed")}
	err := RunAcceptance(context.Background(), acceptanceConfig(), tickets, harness, newFakeCoderState(), nil)
	if err == nil || !strings.Contains(err.Error(), "harness failed") || !strings.Contains(err.Error(), "reobserve signoff ticket") {
		t.Fatalf("RunAcceptance error = %v, want harness and post-turn observation failures", err)
	}
	if len(harness.runs) != 1 || !reflect.DeepEqual(tickets.showIDs, []string{id, id}) || !watch.closed {
		t.Fatalf("runs=%d show IDs=%v watch closed=%v; want one turn, pre/post observations, and reaped watch", len(harness.runs), tickets.showIDs, watch.closed)
	}
}

func acceptanceConfig() AcceptanceConfig {
	return AcceptanceConfig{
		WorkerName: "architect-worker", RoleName: "architect-api", Actor: "architect", Harness: "codex",
		SessionPolicy: SessionPolicyTicket, MinimumReuseContextPercent: 20, RepositoryID: acceptanceTestRepositoryID,
		QueueFilters: ticketclient.QueueFilters{Tags: []string{"api"}},
	}
}

func containsText(value, fragment string) bool { return strings.Contains(value, fragment) }

func assertAcceptanceHoldGuidance(t *testing.T, prompt string) {
	t.Helper()
	for _, required := range []string{
		"outstanding holds",
		"in the ticket's repository through Ticket public interfaces",
		"blocker/Handoff and dependencies",
		"P0 holds",
		"missing architectural decisions or permissions",
		"cross-ticket coordination",
		"held prerequisites of final integration",
		"circular waits",
		"without a clear responsible actor",
		"not narrative similarity",
		"affected full Ticket IDs",
		"responsible actor if known, and concrete next action",
		"assess whether it prevents acceptance",
		"Do not approve final integration with incomplete prerequisites",
		"routine external waits",
		"hold does not itself mean permission is absent",
		"do not ask again when it is established",
		"create follow-up tickets",
		"refine unowned related planning tickets",
		"correct mistaken dependencies or split work",
		"Do not remove a genuine prerequisite",
		"do not take over owned work",
		"do not require whole-project completion for every component",
		"Escalate changes to approved scope or architecture",
		"Record the acceptance decision, outstanding integration work, and planning changes made",
		"explicit human handoff",
	} {
		if !strings.Contains(prompt, required) {
			t.Errorf("acceptance guidance missing %q in %q", required, prompt)
		}
	}
}
