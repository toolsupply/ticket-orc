package cli

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type abortSteerClient struct {
	activeID  string
	activeErr error
	active    int
}

func (c *abortSteerClient) ActiveClaims(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	c.active++
	if c.activeErr != nil {
		return ticketclient.ListResult{}, c.activeErr
	}
	if queue == "open" && c.activeID != "" {
		return testTicketList(c.activeID, queue), nil
	}
	return ticketclient.ListResult{}, nil
}

func (c *abortSteerClient) ReadyFrontier(context.Context, string, []string, int) (ticketclient.ListResult, error) {
	return ticketclient.ListResult{}, nil
}

func (c *abortSteerClient) Close() error { return nil }

func TestRequestDynamicSteerAbortTargetsOnlyCurrentPotentialWork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if _, err := state.NewDaemonControlStore(dir).AbortDaemon(ctx); err != nil {
		t.Fatal(err)
	}
	registrations := state.NewRegistrationStore(dir)
	active := joinAbortSteerRegistration(t, registrations, "active", steerTestThread)
	queued := joinAbortSteerRegistration(t, registrations, "queued", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f0")
	idle := joinAbortSteerRegistration(t, registrations, "idle", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f1")
	old := joinAbortSteerRegistration(t, registrations, "replaced", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f2")
	runtime := state.NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{active, queued, idle, old}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Update(ctx, active, "degraded", "queue_rejected"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Update(ctx, queued, "queued", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Update(ctx, old, "queued", ""); err != nil {
		t.Fatal(err)
	}
	currentReplacement, _, changed, err := registrations.Join(ctx, state.SteerRegistration{
		RepositoryID: old.RepositoryID, RepositoryPath: old.RepositoryPath, RepositoryName: old.RepositoryName,
		Actor: old.Actor, Role: old.Role, CodexHome: old.CodexHome,
		ThreadID: "01a0da4e-aa3a-78d3-87ba-b5972a10e6f3",
	})
	if err != nil || !changed {
		t.Fatalf("replace registration changed=%t err=%v", changed, err)
	}
	before, err := runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientsByActor := map[string]*abortSteerClient{
		"active":   {activeID: "20260926-00001"},
		"queued":   {},
		"idle":     {},
		"replaced": {},
	}
	queuedByActor := make(map[string]int)
	var messages []string
	results, err := requestDynamicSteerAbort(ctx, dir, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open", NudgePrompt: "ordinary work prompt"}}, nil,
		func(reg state.SteerRegistration) (steerClient, error) { return clientsByActor[reg.Actor], nil },
		func(_ context.Context, _, thread, message string) error {
			queuedByActor[thread]++
			messages = append(messages, message)
			if thread == queued.ThreadID {
				return errors.New("unavailable")
			}
			return nil
		}, newRegistrationObserver(registrations), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("abort results = %#v, want only active and queued registrations", results)
	}
	if queuedByActor[active.ThreadID] != 1 || queuedByActor[queued.ThreadID] != 1 || queuedByActor[idle.ThreadID] != 0 || queuedByActor[old.ThreadID] != 0 {
		t.Fatalf("requests by session = %#v", queuedByActor)
	}
	if len(messages) != 2 || messages[0] != steerAbortPrompt || messages[1] != steerAbortPrompt {
		t.Fatalf("emergency messages = %#v", messages)
	}
	if results[0].Outcome != "stop_requested" || results[0].Code != "termination_unconfirmed" || results[1].Outcome != "request_failed" {
		t.Fatalf("abort results = %#v", results)
	}
	if clientsByActor["active"].active != 1 || clientsByActor["queued"].active != 2 {
		t.Fatalf("active claim observations active=%d queued=%d", clientsByActor["active"].active, clientsByActor["queued"].active)
	}
	if clientsByActor["idle"].active != 2 {
		t.Fatalf("idle current registration observations=%d, want successful open/review Ticket probes", clientsByActor["idle"].active)
	}
	after, err := runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("abort changed ordinary delivery state:\nbefore=%#v\nafter=%#v", before, after)
	}
	if currentReplacement.RegistrationID == old.RegistrationID {
		t.Fatalf("replacement retained old registration incarnation: %#v", currentReplacement)
	}
}

func TestRequestDynamicSteerAbortUsesOneLocalDirectory(t *testing.T) {
	ctx := context.Background()
	localDir := t.TempDir()
	if _, err := state.NewDaemonControlStore(localDir).AbortDaemon(ctx); err != nil {
		t.Fatal(err)
	}
	registrations := state.NewRegistrationStore(localDir)
	registration := joinAbortSteerRegistration(t, registrations, "coder", steerTestThread)
	runtime := state.NewSteerRuntimeStore(localDir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	var queueCalls int
	results, err := requestConfiguredSteerAbort(ctx, RunConfig{
		StateDir:   localDir,
		SteerRoles: map[string]steerRolePolicy{"coder": {TicketQueue: "open"}},
	},
		func(state.SteerRegistration) (steerClient, error) {
			return &abortSteerClient{activeID: "20260926-00001"}, nil
		},
		func(context.Context, string, string, string) error { queueCalls++; return nil })
	if err != nil {
		t.Fatalf("abort with one local directory: %v", err)
	}
	if queueCalls != 1 || len(results) != 1 || results[0].Actor != registration.Actor || results[0].Outcome != "stop_requested" {
		t.Fatalf("steer abort calls=%d results=%#v, want one active target attempt", queueCalls, results)
	}
	mode, err := state.NewDaemonControlStore(localDir).DaemonControl(ctx)
	if err != nil || mode.Mode != state.DaemonAborted {
		t.Fatalf("durable control mode=%#v err=%v, want aborted", mode, err)
	}
}

func TestRequestDynamicSteerAbortSkipsInactiveAndMakesOneAttemptForPendingWork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if _, err := state.NewDaemonControlStore(dir).AbortDaemon(ctx); err != nil {
		t.Fatal(err)
	}
	registrations := state.NewRegistrationStore(dir)
	offline := joinAbortSteerRegistration(t, registrations, "offline", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f4")
	idle := joinAbortSteerRegistration(t, registrations, "idle", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f5")
	pending := joinAbortSteerRegistration(t, registrations, "pending", "01a0da4e-aa3a-78d3-87ba-b5972a10e6f6")
	runtime := state.NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{offline, idle, pending}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Update(ctx, pending, "queued", ""); err != nil {
		t.Fatal(err)
	}
	queueCalls := 0
	results, err := requestDynamicSteerAbort(ctx, dir, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open"}}, nil,
		func(reg state.SteerRegistration) (steerClient, error) {
			if reg.Actor == "offline" || reg.Actor == "pending" {
				return nil, errors.New("session unavailable")
			}
			return &abortSteerClient{}, nil
		},
		func(ctx context.Context, _, _, message string) error {
			queueCalls++
			if _, ok := ctx.Deadline(); !ok {
				t.Error("abort request did not have a bounded deadline")
			}
			if message != steerAbortPrompt {
				t.Errorf("emergency message = %q", message)
			}
			return errors.New("transport unavailable")
		},
		newRegistrationObserver(registrations), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if queueCalls != 1 {
		t.Fatalf("queued %d emergency requests, want exactly one bounded attempt", queueCalls)
	}
	if len(results) != 1 || results[0].Actor != "pending" || results[0].Outcome != "request_failed" {
		t.Fatalf("pending target result = %#v", results)
	}
}

func TestRequestDynamicSteerAbortRechecksRegistrationBeforeQueue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if _, err := state.NewDaemonControlStore(dir).AbortDaemon(ctx); err != nil {
		t.Fatal(err)
	}
	registrations := state.NewRegistrationStore(dir)
	old := joinAbortSteerRegistration(t, registrations, "coder", steerTestThread)
	runtime := state.NewSteerRuntimeStore(dir)
	if err := runtime.Reconcile(ctx, []state.SteerRegistration{old}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Update(ctx, old, "queued", ""); err != nil {
		t.Fatal(err)
	}
	queueCalls := 0
	results, err := requestDynamicSteerAbort(ctx, dir, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open"}}, nil,
		func(reg state.SteerRegistration) (steerClient, error) {
			_, _, _, joinErr := registrations.Join(ctx, state.SteerRegistration{
				RepositoryID: reg.RepositoryID, RepositoryPath: reg.RepositoryPath, RepositoryName: reg.RepositoryName,
				Actor: reg.Actor, Role: reg.Role, CodexHome: reg.CodexHome,
				ThreadID: "01a0da4e-aa3a-78d3-87ba-b5972a10e6f7",
			})
			if joinErr != nil {
				return nil, joinErr
			}
			return &abortSteerClient{activeID: "20260926-00001"}, nil
		},
		func(context.Context, string, string, string) error { queueCalls++; return nil },
		newRegistrationObserver(registrations), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if queueCalls != 0 || len(results) != 0 {
		t.Fatalf("superseded registration received abort request: calls=%d results=%#v", queueCalls, results)
	}
}

func TestRequestDynamicSteerAbortRequiresDurableAbortedMode(t *testing.T) {
	dir := t.TempDir()
	var queueCalls int
	_, err := requestDynamicSteerAbort(context.Background(), dir, dir, map[string]steerRolePolicy{"coder": {TicketQueue: "open"}}, nil,
		func(state.SteerRegistration) (steerClient, error) {
			return &abortSteerClient{activeID: "20260926-00001"}, nil
		},
		func(context.Context, string, string, string) error { queueCalls++; return nil }, nil, nil)
	if err == nil || queueCalls != 0 {
		t.Fatalf("normal mode abort err=%v queueCalls=%d, want error and no request", err, queueCalls)
	}
}

func joinAbortSteerRegistration(t *testing.T, store *state.RegistrationStore, actor, thread string) state.SteerRegistration {
	t.Helper()
	reg, _, _, err := store.Join(context.Background(), state.SteerRegistration{
		RepositoryID: joinTestRepositoryID, RepositoryPath: t.TempDir(), Actor: actor, Role: "coder",
		CodexHome: t.TempDir(), ThreadID: thread,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}
