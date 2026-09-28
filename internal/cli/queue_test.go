package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type forecastTicketReader struct {
	actor  string
	active map[string]map[string]ticketclient.ListResult
	ready  map[string]ticketclient.ListResult
	calls  *[]string
}

func (r *forecastTicketReader) ActiveClaims(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	*r.calls = append(*r.calls, r.actor+":active:"+queue)
	return r.active[r.actor][queue], nil
}

func (r *forecastTicketReader) ReadyFrontier(_ context.Context, queue string, _ []string, _ int) (ticketclient.ListResult, error) {
	*r.calls = append(*r.calls, r.actor+":ready:"+queue)
	return r.ready[queue], nil
}

func (r *forecastTicketReader) Close() error { return nil }

func TestQueueForecastSharesOrderedFrontierAndLeavesStateUnchanged(t *testing.T) {
	dir := t.TempDir()
	repositoryPath := t.TempDir()
	ctx := context.Background()
	registration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, RepositoryName: "project",
		Actor: "reviewer", Role: "reviewer", CodexHome: dir, ThreadID: steerTestThread,
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictedRegistration, _, _, err := state.NewRegistrationStore(dir).Join(ctx, state.SteerRegistration{
		RepositoryID: localRepoID, RepositoryPath: repositoryPath, RepositoryName: "project",
		Actor: "coder", Role: "reviewer", CodexHome: dir, ThreadID: "01a0da4e-aa3a-78d3-87ba-b5972a10e2a7",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := state.NewSteerRuntimeStore(dir)
	if err := runtimeStore.Reconcile(ctx, []state.SteerRegistration{registration, conflictedRegistration}); err != nil {
		t.Fatal(err)
	}
	if updated, err := runtimeStore.CompleteDelivery(ctx, registration, "queued", true, true); err != nil || !updated {
		t.Fatalf("seed delivery updated=%t err=%v", updated, err)
	}
	registrationBefore, err := state.NewRegistrationStore(dir).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deliveryBefore, err := runtimeStore.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	openFrontier := ticketclient.ListResult{Items: []ticketclient.Ticket{
		{ID: "20260926-10001", Title: "First open item", State: "open", Priority: 1},
		{ID: "20260926-10002", Title: "Second open item", State: "open", Priority: 2},
	}}
	reviewFrontier := ticketclient.ListResult{Items: []ticketclient.Ticket{
		{ID: "20260926-10003", Title: "Review item", State: "review", Priority: 2},
	}}
	active := map[string]map[string]ticketclient.ListResult{
		"coder": {"open": {Items: []ticketclient.Ticket{{ID: "20260926-10000", Title: "Fix workers output formatting", State: "open", Assignee: "coder", Priority: 2}}}},
	}
	ready := map[string]ticketclient.ListResult{"open": openFrontier, "review": reviewFrontier}
	ticketStateBefore := struct {
		Active map[string]map[string]ticketclient.ListResult
		Ready  map[string]ticketclient.ListResult
	}{active, ready}
	var ticketCalls []string
	open := func(identity currentTicketIdentity) (localTicketReader, error) {
		return &forecastTicketReader{actor: identity.Actor, active: active, ready: ready, calls: &ticketCalls}, nil
	}
	loaded := LoadedFileConfig{
		Instance: InstanceContext{InstanceDir: dir, LocalDir: dir},
		Config: FileConfig{
			DefaultRole: "coder",
			Roles: map[string]RoleFileConfig{
				"coder": {TicketQueue: "open"}, "architect": {TicketQueue: "open"}, "reviewer": {TicketQueue: "review"},
			},
			Workers: map[string]WorkerFileConfig{
				"coder-owner": {Role: "coder"}, "architect-owner": {Role: "architect"},
			},
		},
	}
	status := daemon.Status{
		Workers: []daemon.WorkerStatus{
			{Name: "coder-owner", Role: "coder", TicketActor: "coder", State: "running", RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath},
			{Name: "architect-owner", Role: "architect", TicketActor: "architect", State: "running", RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath},
		},
		Steer: []daemon.SteerStatus{
			{RepositoryID: localRepoID, RepositoryName: "project", Role: "reviewer", Actor: "reviewer", Session: steerTestThread, State: "queued"},
			{RepositoryID: localRepoID, RepositoryName: "project", Role: "reviewer", Actor: "coder", Session: conflictedRegistration.ThreadID, State: "conflict", ManagedOwner: "coder-owner"},
		},
	}

	first, err := queueForecast(ctx, loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	second, err := queueForecast(ctx, loaded, status, open)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated forecast changed: first=%#v second=%#v", first, second)
	}
	if len(first) != 4 {
		t.Fatalf("forecast owner count=%d, want two managed workers and two steer sessions: %#v", len(first), first)
	}
	if first[0].Owner != "coder-owner" || first[0].Next == nil || first[0].Next.ID != "20260926-10001" || first[1].Owner != "architect-owner" || first[1].Next == nil || first[1].Next.ID != "20260926-10002" {
		t.Fatalf("same-queue forecast did not preserve managed dispatch order and Ticket frontier: %#v", first[:2])
	}
	var output bytes.Buffer
	if err := renderQueueForecast(&output, first); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.HasPrefix(text, "\n") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("queue output must start and end with a blank line: %q", text)
	}
	for _, want := range []string{"Repository: project (d659917f…) " + repositoryPath} {
		if !strings.Contains(text, want) {
			t.Errorf("queue output missing %q: %s", want, text)
		}
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) != 7 || !reflect.DeepEqual(strings.Fields(lines[2]), []string{"Actor", "Ticket", "Title", "State"}) {
		t.Fatalf("queue table header or row count is wrong: %q", lines)
	}
	wantRows := [][]string{
		{"coder", "20260926-10000", "Fix", "workers", "outp...", "claimed"},
		{"coder", "20260926-10001", "First", "open", "item", "queued"},
		{"architect", "20260926-10002", "Second", "open", "item", "queued"},
		{"reviewer", "20260926-10003", "Review", "item", "queued"},
	}
	for index, want := range wantRows {
		if got := strings.Fields(lines[index+3]); !reflect.DeepEqual(got, want) {
			t.Errorf("queue row[%d]=%v, want %v", index, got, want)
		}
	}
	for _, forbidden := range []string{"coder-owner", "managed", "project/reviewer", "already notified", "  active", "  next"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("queue output included owner diagnostics %q: %s", forbidden, text)
		}
	}
	positions := []int{
		strings.Index(text, "20260926-10000"), strings.Index(text, "20260926-10001"),
		strings.Index(text, "20260926-10002"), strings.Index(text, "20260926-10003"),
	}
	for index := 1; index < len(positions); index++ {
		if positions[index] <= positions[index-1] {
			t.Fatalf("queue tickets are not in forecast order: positions=%v output=%s", positions, text)
		}
	}
	for _, ticketID := range []string{"20260926-10000", "20260926-10001", "20260926-10002", "20260926-10003"} {
		if strings.Count(text, ticketID) != 1 {
			t.Fatalf("Ticket %s was assigned more than once: %s", ticketID, text)
		}
	}
	unconfiguredWorker := status
	unconfiguredWorker.Workers = append(append([]daemon.WorkerStatus(nil), status.Workers...), daemon.WorkerStatus{
		Name: "unconfigured-owner", TicketActor: "coder", State: "running",
		RepositoryID: localRepoID, RepositoryName: "project", RepositoryPath: repositoryPath,
	})
	if _, err := queueForecast(ctx, loaded, unconfiguredWorker, open); err == nil || !strings.Contains(err.Error(), `managed worker "unconfigured-owner" has unsupported role or queue ""`) {
		t.Fatalf("unconfigured managed worker inherited default_role: error=%v", err)
	}
	pausedStatus := status
	pausedStatus.Mode = "paused"
	var pausedOutput bytes.Buffer
	if err := writeQueueForecast(ctx, &pausedOutput, false, loaded, pausedStatus, open); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(pausedOutput.String(), "daemon mode=paused dispatch=inhibited") || !strings.Contains(pausedOutput.String(), "20260926-10001") {
		t.Fatalf("paused queue did not report inhibition and keep forecasting ready Ticket work: %q", pausedOutput.String())
	}

	registrationAfter, err := state.NewRegistrationStore(dir).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deliveryAfter, err := runtimeStore.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(registrationBefore, registrationAfter) || !reflect.DeepEqual(deliveryBefore, deliveryAfter) {
		t.Fatalf("queue forecast mutated Orc state: registrations before/after=%#v/%#v deliveries=%#v/%#v", registrationBefore, registrationAfter, deliveryBefore, deliveryAfter)
	}
	ticketStateAfter := struct {
		Active map[string]map[string]ticketclient.ListResult
		Ready  map[string]ticketclient.ListResult
	}{active, ready}
	if !reflect.DeepEqual(ticketStateBefore, ticketStateAfter) {
		t.Fatalf("queue forecast mutated Ticket fixture: before=%#v after=%#v", ticketStateBefore, ticketStateAfter)
	}
	if len(ticketCalls) == 0 {
		t.Fatal("forecast did not query Ticket")
	}
	for _, call := range ticketCalls {
		if strings.Contains(call, "claim") || strings.Contains(call, "release") || strings.Contains(call, "next") || strings.Contains(call, "wait") {
			t.Fatalf("forecast issued a mutating Ticket operation: %q", call)
		}
	}
}

func TestQueueForecastEmptyOutputHasBlankBoundaries(t *testing.T) {
	var output bytes.Buffer
	if err := renderQueueForecast(&output, nil); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.HasPrefix(got, "\n") || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("empty queue output must start and end with a blank line: %q", got)
	}
}

func TestQueueOutputReportsDispatchInhibitionAndKeepsForecast(t *testing.T) {
	dir := t.TempDir()
	loaded := LoadedFileConfig{Instance: InstanceContext{InstanceDir: dir, LocalDir: dir}}
	status := daemon.Status{Mode: "paused"}
	open := func(currentTicketIdentity) (localTicketReader, error) { return &forecastTicketReader{}, nil }
	var compact bytes.Buffer
	if err := writeQueueForecast(context.Background(), &compact, false, loaded, status, open); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compact.String(), "daemon mode=paused dispatch=inhibited") || !strings.Contains(compact.String(), "No configured or registered execution owners") {
		t.Fatalf("compact queue output did not show inhibited mode and forecast: %q", compact.String())
	}
	var encoded bytes.Buffer
	if err := writeQueueForecast(context.Background(), &encoded, true, loaded, status, open); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Mode              string               `json:"daemon_mode"`
		DispatchInhibited bool                 `json:"dispatch_inhibited"`
		Owners            []queueForecastOwner `json:"owners"`
	}
	if err := json.Unmarshal(encoded.Bytes(), &result); err != nil {
		t.Fatalf("decode queue JSON %q: %v", encoded.String(), err)
	}
	if result.Mode != "paused" || !result.DispatchInhibited || result.Owners == nil {
		t.Fatalf("queue JSON omitted mode or owners: %#v", result)
	}
}

func TestQueueCommandAndConsoleHelp(t *testing.T) {
	var output, stderr bytes.Buffer
	if code := run([]string{"help", "queue"}, &output, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 || !strings.Contains(output.String(), "read-only forecast") {
		t.Fatalf("queue help code=%d out=%q err=%q", code, output.String(), stderr.String())
	}
	output.Reset()
	if code := run([]string{"queue", "--help"}, &output, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 || !strings.Contains(output.String(), "ticket-orc queue") {
		t.Fatalf("queue command help code=%d out=%q err=%q", code, output.String(), stderr.String())
	}
	command, err := parseConsoleCommand("queue")
	if err != nil || command.name != "queue" {
		t.Fatalf("console queue command=%#v err=%v", command, err)
	}
	if _, err := parseConsoleCommand("queue detail"); err == nil {
		t.Fatal("queue accepted unsupported arguments")
	}
	var consoleHelp bytes.Buffer
	writeConsoleTopicHelp(&consoleHelp, "queue")
	if !strings.Contains(consoleHelp.String(), "scheduling forecast") {
		t.Fatalf("console queue help=%q", consoleHelp.String())
	}
}
