package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const localRepoID = "d659917f-5939-4e93-bfde-6346a0f2bc50"

func localIdentityEnv(dir string) envLookup {
	return mapEnv(map[string]string{"TICKET_ORC": dir, "CODEX_THREAD_ID": steerTestThread, "CODEX_HOME": filepath.Join(dir, "codex")})
}

type localQueueStub struct {
	active, ready ticketclient.ListResult
	activeByQueue map[string]ticketclient.ListResult
	readyByQueue  map[string]ticketclient.ListResult
	filters       map[string]ticketclient.QueueFilters
	calls         []string
	closed        bool
}

func (stub *localQueueStub) ActiveClaims(_ context.Context, queue string, limit int) (ticketclient.ListResult, error) {
	stub.calls = append(stub.calls, "active:"+queue+":"+fmt.Sprint(limit))
	if result, ok := stub.activeByQueue[queue]; ok {
		return result, nil
	}
	return stub.active, nil
}
func (stub *localQueueStub) ReadyFrontier(_ context.Context, queue string, filters ticketclient.QueueFilters, limit int) (ticketclient.ListResult, error) {
	stub.calls = append(stub.calls, "ready:"+queue+":"+fmt.Sprint(limit))
	if stub.filters == nil {
		stub.filters = make(map[string]ticketclient.QueueFilters)
	}
	stub.filters[queue] = filters
	if result, ok := stub.readyByQueue[queue]; ok {
		return result, nil
	}
	return stub.ready, nil
}
func (stub *localQueueStub) Close() error { stub.closed = true; return nil }

func localQueueFactory(stub *localQueueStub) localTicketReaderFactory {
	return func(currentTicketIdentity) (localTicketReader, error) { return stub, nil }
}

func registerLocalSession(t *testing.T, dir string, role string) state.SteerRegistration {
	t.Helper()
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer
	lookup := localIdentityEnv(dir)
	if err := joinWithRunner(context.Background(), role, &output, lookup, runner); err != nil {
		t.Fatal(err)
	}
	registration, ok, err := state.NewRegistrationStore(dir).Find(context.Background(), localRepoID, "reviewer")
	if err != nil || !ok {
		t.Fatalf("registration=%#v ok=%v err=%v", registration, ok, err)
	}
	return registration
}

func explicitLocalSelector() steerEndpointSelector {
	return steerEndpointSelector{Harness: "future-harness", SessionID: "session:42/opaque", Transport: "spool", Explicit: true}
}

func registerExplicitLocalSession(t *testing.T, dir string, selector steerEndpointSelector) state.SteerRegistration {
	t.Helper()
	runner, _ := fakeTicketRunner("")
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	var output bytes.Buffer
	if err := joinWithEndpointConfigRunner(context.Background(), "reviewer", "", selector, true, &output, lookup, runner, nil); err != nil {
		t.Fatal(err)
	}
	registration, ok, err := state.NewRegistrationStore(dir).Find(context.Background(), localRepoID, "reviewer")
	if err != nil || !ok {
		t.Fatalf("registration=%#v ok=%v err=%v", registration, ok, err)
	}
	return registration
}

func TestNextUsesReadOnlyClaimsAndReadyFrontier(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registerLocalSession(t, dir, "reviewer")
	identityRunner, _ := fakeTicketRunner("")
	queries := &localQueueStub{ready: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260926-12345", Title: "Review it", State: "review", Priority: 2}}}}
	var output bytes.Buffer
	if err := nextWithRunner(context.Background(), false, &output, localIdentityEnv(dir), identityRunner, localQueueFactory(queries)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "P2  20260926-12345  Review it") {
		t.Fatalf("next output=%q", output.String())
	}
	if !strings.Contains(strings.Join(queries.calls, ","), "active:open:20") || !strings.Contains(strings.Join(queries.calls, ","), "active:review:20") || !strings.Contains(strings.Join(queries.calls, ","), "ready:review:20") || !queries.closed {
		t.Fatalf("Ticket observations=%v closed=%v", queries.calls, queries.closed)
	}
}

func TestLocalOwnershipPrecedesRoleQueueAndReadyUsesRoleFilters(t *testing.T) {
	dir := t.TempDir()
	config := fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"open","nudge_prompt":"Work.","ticket_tags":["urgent"]}},"review":{"skip_tags":["blocked"]}}`, dir)
	writeConfigFixture(t, filepath.Join(dir, "config.json"), config)
	registerLocalSession(t, dir, "reviewer")
	runner, _ := fakeTicketRunner("")
	active := ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260930-11111", State: "review", Assignee: "reviewer"}}}
	queries := &localQueueStub{
		activeByQueue: map[string]ticketclient.ListResult{"review": active},
		readyByQueue:  map[string]ticketclient.ListResult{"open": {Items: []ticketclient.Ticket{{ID: "20260930-22222", State: "open"}}}},
	}
	var output bytes.Buffer
	if err := nextWithRunner(context.Background(), true, &output, localIdentityEnv(dir), runner, localQueueFactory(queries)); err != nil {
		t.Fatal(err)
	}
	var next map[string]any
	if err := json.Unmarshal(output.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if next["queue"] != "open" || next["role_queue"] != "open" || next["active_queue"] != "review" || next["active_claim"] != true {
		t.Fatalf("cross-queue claim output=%#v", next)
	}
	if strings.Contains(strings.Join(queries.calls, ","), "ready:") {
		t.Fatalf("queried ready work despite active claim: %v", queries.calls)
	}
	if !strings.Contains(strings.Join(queries.calls, ","), "active:open:20") || !strings.Contains(strings.Join(queries.calls, ","), "active:review:20") {
		t.Fatalf("did not inspect both ownership queues: %v", queries.calls)
	}
	loaded, err := loadSteerConfig(localIdentityEnv(dir))
	if err != nil {
		t.Fatal(err)
	}
	stateQueries := &localQueueStub{activeByQueue: map[string]ticketclient.ListResult{"review": active}}
	stateResult, err := currentSessionStateWithConfig(context.Background(), dir, loaded, localIdentityEnv(dir), runner, localQueueFactory(stateQueries), func(context.Context, string) (daemon.Status, error) {
		return daemon.Status{}, nil
	})
	if err != nil || stateResult.RoleQueue != "open" || stateResult.ActiveQueue != "review" || stateResult.ReadyCount != 0 {
		t.Fatalf("cross-queue current state=%#v err=%v", stateResult, err)
	}

	writeConfigFixture(t, filepath.Join(dir, "config.json"), fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"Work.","ticket_tags":["urgent"]}},"review":{"skip_tags":["blocked"]}}`, dir))
	queries = &localQueueStub{readyByQueue: map[string]ticketclient.ListResult{"review": {Items: []ticketclient.Ticket{{ID: "20260930-33333", State: "review"}}}}}
	output.Reset()
	if err := nextWithRunner(context.Background(), false, &output, localIdentityEnv(dir), runner, localQueueFactory(queries)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "20260930-33333") || !reflect.DeepEqual(queries.filters["review"], ticketclient.QueueFilters{Tags: []string{"urgent"}, WithoutTags: []string{"blocked"}}) {
		t.Fatalf("role selector not applied, output=%q filters=%#v", output.String(), queries.filters)
	}
}

func TestNextRejectsAbsentSessionWithoutTicketQueueQueries(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer
	err := nextWithRunner(context.Background(), false, &output, localIdentityEnv(dir), runner, func(currentTicketIdentity) (localTicketReader, error) {
		t.Fatal("Ticket reader opened for an absent session")
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "not joined") {
		t.Fatalf("next error=%v", err)
	}
}

func TestExplicitConfigSelectsOneLocalRootForSteerCommands(t *testing.T) {
	ambientDir := t.TempDir()
	selectedDir := t.TempDir()
	selectedLocalDir := filepath.Join(selectedDir, "selected-runtime")
	writeSteerConfig(t, ambientDir)
	configPath := filepath.Join(selectedDir, "team.json")
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":"2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."}}}`, selectedLocalDir))
	lookup := localIdentityEnv(ambientDir)
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer

	if err := joinWithConfigRunner(context.Background(), "", configPath, &output, lookup, runner); err != nil {
		t.Fatal(err)
	}
	selectedStore := state.NewRegistrationStore(selectedLocalDir)
	registration, ok, err := selectedStore.Find(context.Background(), localRepoID, "reviewer")
	if err != nil || !ok {
		t.Fatalf("selected config registration=%#v ok=%v err=%v", registration, ok, err)
	}
	if _, ok, err := state.NewRegistrationStore(filepath.Join(ambientDir, ".local", "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa")).Find(context.Background(), localRepoID, "reviewer"); err != nil || ok {
		t.Fatalf("ambient config unexpectedly received selected session: ok=%v err=%v", ok, err)
	}

	output.Reset()
	if err := whoamiWithConfigRunner(context.Background(), true, configPath, &output, lookup, runner); err != nil || !strings.Contains(output.String(), `"joined": true`) {
		t.Fatalf("whoami output=%q err=%v", output.String(), err)
	}
	queries := &localQueueStub{ready: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260927-12345", Title: "Selected queue", State: "review"}}}}
	output.Reset()
	if err := nextWithConfigRunner(context.Background(), false, configPath, &output, lookup, runner, localQueueFactory(queries)); err != nil || !strings.Contains(output.String(), "Selected queue") {
		t.Fatalf("next output=%q err=%v", output.String(), err)
	}
	if !queries.closed {
		t.Fatal("Ticket reader was not closed")
	}
	if removed, err := leaveWithConfigRunner(context.Background(), configPath, &output, lookup, runner); err != nil || !removed {
		t.Fatalf("leave removed=%v err=%v", removed, err)
	}
	if _, ok, err := selectedStore.Find(context.Background(), localRepoID, "reviewer"); err != nil || ok {
		t.Fatalf("selected registration remains after leave: ok=%v err=%v", ok, err)
	}
}

func TestCurrentSessionStateShowsJoinedDeliveryAndReadySummary(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registration := registerLocalSession(t, dir, "reviewer")
	store := state.NewSteerRuntimeStore(dir)
	if err := store.Reconcile(context.Background(), []state.SteerRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(context.Background(), registration, "queued", ""); err != nil {
		t.Fatal(err)
	}
	runner, _ := fakeTicketRunner("")
	queries := &localQueueStub{activeByQueue: map[string]ticketclient.ListResult{"review": {Items: []ticketclient.Ticket{{ID: "20260926-54321", State: "review", Assignee: "reviewer"}}}}, ready: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260926-12345", State: "review"}}, More: true}}
	result, err := currentSessionStateWithRunner(context.Background(), dir, localIdentityEnv(dir), runner, localQueueFactory(queries), readLocalDaemonStatus)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Joined || result.Role != "reviewer" || result.ReadyCount != 0 || result.ReadyMore || result.RoleQueue != "review" || result.ActiveQueue != "review" || len(result.ActiveClaims) != 1 || result.Delivery != "queued" {
		t.Fatalf("session state=%#v", result)
	}
	var output bytes.Buffer
	if err := renderCurrentSessionState(&output, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Work: none") || !strings.Contains(output.String(), "Active: 20260926-54321") || !strings.Contains(output.String(), "Delivery: notification queued") {
		t.Fatalf("state output=%q", output.String())
	}
}

func TestCurrentSessionStateIgnoresStatusFromDifferentHarness(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registration := registerLocalSession(t, dir, "reviewer")
	lookup := localIdentityEnv(dir)
	loaded, err := loadSteerConfig(lookup)
	if err != nil {
		t.Fatal(err)
	}
	runner, _ := fakeTicketRunner("")
	queries := &localQueueStub{}
	result, err := currentSessionStateWithEndpointConfig(context.Background(), dir,
		loaded, steerEndpointSelector{}, lookup, runner, localQueueFactory(queries), func(context.Context, string) (daemon.Status, error) {
			return daemon.Status{Steer: []daemon.SteerStatus{{
				RepositoryID: registration.RepositoryID, Actor: registration.Actor, Harness: "replacement-harness",
				Session: registration.SessionID, State: "conflict", Code: "stale_status",
			}}}, nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ManagedOwnership != "unknown" || result.StatusCode != "session_status_missing" {
		t.Fatalf("stale daemon status was accepted: %#v", result)
	}
}

func TestCurrentSessionStateReportsAbsentRegistration(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	runner, _ := fakeTicketRunner("")
	result, err := currentSessionStateWithRunner(context.Background(), dir, localIdentityEnv(dir), runner, func(currentTicketIdentity) (localTicketReader, error) {
		t.Fatal("Ticket reader opened for an absent session")
		return nil, nil
	}, readLocalDaemonStatus)
	if err != nil || result.Joined || result.Role != "" || result.ReadyCount != 0 {
		t.Fatalf("state=%#v err=%v", result, err)
	}
}

func TestGenericEndpointSelectionForWhoamiNextAndState(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	selector := explicitLocalSelector()
	registration := registerExplicitLocalSession(t, dir, selector)
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	runner, _ := fakeTicketRunner("")

	var output bytes.Buffer
	if err := whoamiWithEndpointConfigRunner(context.Background(), true, "", selector, &output, lookup, runner, nil); err != nil {
		t.Fatal(err)
	}
	var whoami map[string]any
	if err := json.Unmarshal(output.Bytes(), &whoami); err != nil {
		t.Fatal(err)
	}
	transport, _ := whoami["transport"].(map[string]any)
	if whoami["joined"] != true || whoami["harness"] != selector.Harness || whoami["session"] != selector.SessionID ||
		whoami["registration_id"] != registration.RegistrationID || transport["kind"] != "spool" {
		t.Fatalf("generic whoami=%#v", whoami)
	}

	queries := &localQueueStub{ready: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260929-11223", State: "review"}}}}
	output.Reset()
	if err := nextWithEndpointConfigRunner(context.Background(), true, "", selector, &output, lookup, runner, localQueueFactory(queries), nil); err != nil {
		t.Fatal(err)
	}
	var next map[string]any
	if err := json.Unmarshal(output.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if next["harness"] != selector.Harness || next["session"] != selector.SessionID || !queries.closed {
		t.Fatalf("generic next=%#v closed=%v", next, queries.closed)
	}

	loaded, err := loadSteerConfig(lookup)
	if err != nil {
		t.Fatal(err)
	}
	stateQueries := &localQueueStub{}
	result, err := currentSessionStateWithEndpointConfig(context.Background(), dir, loaded, selector, lookup, runner,
		localQueueFactory(stateQueries), func(context.Context, string) (daemon.Status, error) { return daemon.Status{}, nil }, nil)
	if err != nil || !result.Joined || result.Harness != selector.Harness || result.Session != selector.SessionID || !stateQueries.closed {
		t.Fatalf("generic state=%#v closed=%v err=%v", result, stateQueries.closed, err)
	}

	config, help, err := parseStateConfig([]string{"--harness", selector.Harness, "--session", selector.SessionID, "--transport", "spool", "--output", "json"}, lookup)
	if err != nil || help || !config.Endpoint.Explicit || config.Endpoint.Harness != selector.Harness {
		t.Fatalf("state endpoint config=%#v help=%t err=%v", config, help, err)
	}
	if _, _, err := parseStateConfig([]string{"--harness", selector.Harness}, lookup); err == nil {
		t.Fatal("state accepted partial explicit endpoint selection")
	}
}

func TestGenericWhoamiDoesNotExposeAnotherEndpoint(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registered := explicitLocalSelector()
	registerExplicitLocalSession(t, dir, registered)
	selected := registered
	selected.SessionID = "different-session"
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer
	if err := whoamiWithEndpointConfigRunner(context.Background(), true, "", selected, &output, lookup, runner, nil); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["joined"] != false || result["transport"] != nil || result["registration_id"] != nil || result["incarnation_id"] != nil {
		t.Fatalf("mismatched endpoint leaked the current registration: %#v", result)
	}
	if err := nextWithEndpointConfigRunner(context.Background(), false, "", selected, &output, lookup, runner,
		func(currentTicketIdentity) (localTicketReader, error) {
			t.Fatal("Ticket reader opened for a mismatched endpoint")
			return nil, nil
		}, nil); err == nil || !strings.Contains(err.Error(), "not joined") {
		t.Fatalf("next accepted mismatched endpoint: %v", err)
	}
	loaded, err := loadSteerConfig(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := currentSessionStateWithEndpointConfig(context.Background(), dir, loaded, selected, lookup, runner,
		func(currentTicketIdentity) (localTicketReader, error) {
			t.Fatal("Ticket reader opened for a mismatched endpoint")
			return nil, nil
		}, nil, nil); err == nil || !strings.Contains(err.Error(), "not joined") {
		t.Fatalf("state accepted mismatched endpoint: %v", err)
	}
}

func TestNextRejectsMismatchedSpoolManifest(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	selector := explicitLocalSelector()
	registration := registerExplicitLocalSession(t, dir, selector)
	manifestPath := filepath.Join(dir, "steer-spool", registration.RegistrationID, registration.IncarnationID, "registration.json")
	if err := os.WriteFile(manifestPath, []byte(`{"protocol":1,"registration_id":"ffffffffffffffffffffffffffffffff"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	runner, _ := fakeTicketRunner("")
	if err := nextWithEndpointConfigRunner(context.Background(), false, "", selector, io.Discard, lookup, runner,
		func(currentTicketIdentity) (localTicketReader, error) {
			t.Fatal("Ticket reader opened for a mismatched spool manifest")
			return nil, nil
		}, nil); err == nil || !strings.Contains(err.Error(), "verify current steer endpoint") {
		t.Fatalf("next accepted mismatched spool manifest: %v", err)
	}
}

func TestNextRejectsMissingSpoolEndpointWithoutRecreatingIt(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	selector := explicitLocalSelector()
	registration := registerExplicitLocalSession(t, dir, selector)
	endpointRoot := filepath.Join(dir, "steer-spool", registration.RegistrationID, registration.IncarnationID)
	if err := os.RemoveAll(endpointRoot); err != nil {
		t.Fatal(err)
	}
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	runner, _ := fakeTicketRunner("")
	if err := nextWithEndpointConfigRunner(context.Background(), false, "", selector, io.Discard, lookup, runner,
		func(currentTicketIdentity) (localTicketReader, error) {
			t.Fatal("Ticket reader opened for a missing spool endpoint")
			return nil, nil
		}, nil); err == nil || !strings.Contains(err.Error(), "verify current steer endpoint") {
		t.Fatalf("next accepted missing spool endpoint: %v", err)
	}
	if _, err := os.Lstat(endpointRoot); !os.IsNotExist(err) {
		t.Fatalf("next recreated the missing spool endpoint: %v", err)
	}
}

func TestExplicitLeaveComparesExactIncarnationAndRetiresOnlyCurrentEndpoint(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	selector := explicitLocalSelector()
	first := registerExplicitLocalSession(t, dir, selector)
	oldRoot := filepath.Join(dir, "steer-spool", first.RegistrationID, first.IncarnationID)
	second := registerExplicitLocalSession(t, dir, selector)
	if first.RegistrationID != second.RegistrationID || first.IncarnationID == second.IncarnationID {
		t.Fatalf("rejoin identity transition first=%#v second=%#v", first, second)
	}
	if _, err := os.Stat(oldRoot); !os.IsNotExist(err) {
		t.Fatalf("old endpoint was not retired on rejoin: %v", err)
	}
	runner, _ := fakeTicketRunner("")
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	var output bytes.Buffer
	removed, err := leaveWithEndpointConfigRunner(context.Background(), "", selector, first.RegistrationID, first.IncarnationID, true, &output, lookup, runner, nil)
	if err != nil || removed {
		t.Fatalf("stale exact leave removed=%v err=%v json=%s", removed, err, output.String())
	}
	current, ok, err := state.NewRegistrationStore(dir).Find(context.Background(), localRepoID, "reviewer")
	if err != nil || !ok || current.IncarnationID != second.IncarnationID {
		t.Fatalf("stale leave changed current registration=%#v ok=%v err=%v", current, ok, err)
	}

	output.Reset()
	removed, err = leaveWithEndpointConfigRunner(context.Background(), "", selector, second.RegistrationID, second.IncarnationID, true, &output, lookup, runner, nil)
	if err != nil || !removed {
		t.Fatalf("current exact leave removed=%v err=%v json=%s", removed, err, output.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "steer-spool", second.RegistrationID, second.IncarnationID)); !os.IsNotExist(err) {
		t.Fatalf("current endpoint was not retired: %v", err)
	}
}

func TestWhoamiJSONReportsAbsentCurrentSession(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer
	if err := whoamiWithRunner(context.Background(), true, &output, localIdentityEnv(dir), runner); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["joined"] != false || result["session"] != steerTestThread || result["actor"] != "reviewer" {
		t.Fatalf("whoami=%#v", result)
	}
}

func TestConsoleShowsManagedWorkersAndExternalSessionsSeparately(t *testing.T) {
	var output bytes.Buffer
	renderConsoleStatus(&output, daemon.Status{
		Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", State: "running"}},
		Steer:   []daemon.SteerStatus{{RepositoryName: "repo", RepositoryID: localRepoID, Role: "reviewer", Actor: "reviewer", Session: steerTestThread, State: "conflict", ManagedOwner: "coder"}},
	})
	text := output.String()
	for _, want := range []string{"Managed workers:", "coder", "Sessions:", "repo", "reviewer", "Ticket actor", "conflict", "actor owned by managed worker"} {
		if !strings.Contains(text, want) {
			t.Fatalf("console status missing %q: %s", want, text)
		}
	}
	if strings.Contains(text, "generation") || strings.Contains(text, "worker-reviewer") {
		t.Fatalf("console leaked internal identity: %s", text)
	}
}

func TestSteerWatchEventUsesSessionIdentityAndHumanState(t *testing.T) {
	var output bytes.Buffer
	event := daemon.Event{Type: "steer.delivery", RepositoryName: "repo", RepositoryID: localRepoID, Role: "reviewer", Actor: "reviewer", Session: steerTestThread, State: "queued", Ticket: "20260926-54321"}
	if !consoleWatchEvent(event) {
		t.Fatal("dynamic session state was hidden from watch")
	}
	renderConsoleEvent(&output, event)
	text := output.String()
	if !strings.Contains(text, "repo") || !strings.Contains(text, "reviewer") || !strings.Contains(text, "01a0da4e…") || !strings.Contains(text, "queued notification for 20260926-54321") || strings.Contains(text, "steer.delivery") || strings.Contains(text, "worker-reviewer") || strings.Contains(text, "generation") {
		t.Fatalf("steer watch output=%q", text)
	}
}

func TestSteerWatchEventExplainsRegistrationStateFailures(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"registration_state_lost", "registration state disappeared; retained registrations are unavailable until steer.json is restored"},
		{"registration_state_malformed", "registration state is malformed; repair steer.json"},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			var output bytes.Buffer
			event := daemon.Event{Type: "steer.status", RepositoryName: "repo", RepositoryID: localRepoID, Role: "reviewer", Actor: "reviewer", Session: steerTestThread, State: "degraded", Code: test.code}
			renderConsoleEvent(&output, event)
			if !strings.Contains(output.String(), test.want) {
				t.Fatalf("watch event did not explain %s: %q", test.code, output.String())
			}
		})
	}
}

func TestCurrentSessionStateRendersExplicitManagedConflict(t *testing.T) {
	var output bytes.Buffer
	result := currentSessionState{Joined: true, Actor: "reviewer", Role: "reviewer", ManagedOwner: "managed-reviewer", ActiveClaims: []localTicket{}}
	if err := renderCurrentSessionState(&output, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "State: conflict") || !strings.Contains(output.String(), "Reason: actor is owned by managed worker managed-reviewer") {
		t.Fatalf("conflict output=%q", output.String())
	}
}

func TestCurrentSessionStatePreservesOwnershipUncertaintyWhenDaemonUnavailable(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registerLocalSession(t, dir, "reviewer")
	runner, _ := fakeTicketRunner("")
	queries := &localQueueStub{}
	result, err := currentSessionStateWithRunner(context.Background(), dir, localIdentityEnv(dir), runner, localQueueFactory(queries), func(context.Context, string) (daemon.Status, error) {
		return daemon.Status{}, errors.New("not reachable")
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManagedOwnership != "unknown" || result.StatusCode != "daemon_status_unavailable" {
		t.Fatalf("ownership result=%#v", result)
	}
	var output bytes.Buffer
	if err := renderCurrentSessionState(&output, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Managed ownership: unknown (daemon status unavailable)") || strings.Contains(output.String(), "Managed ownership: clear") {
		t.Fatalf("uncertain state output=%q", output.String())
	}
}

func TestCurrentSessionStateShowsPendingPersistenceFailure(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	registerLocalSession(t, dir, "reviewer")
	runner, _ := fakeTicketRunner("")
	queries := &localQueueStub{}
	result, err := currentSessionStateWithRunner(context.Background(), dir, localIdentityEnv(dir), runner, localQueueFactory(queries), func(context.Context, string) (daemon.Status, error) {
		return daemon.Status{Steer: []daemon.SteerStatus{{RepositoryID: localRepoID, Actor: "reviewer", Harness: "codex", Session: steerTestThread, State: "sending", Code: "runtime_state_write_failed", PersistenceCode: "runtime_state_write_failed"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManagedOwnership != "clear" || result.StatusCode != "runtime_state_write_failed" || result.PersistenceCode != "runtime_state_write_failed" {
		t.Fatalf("session status=%#v", result)
	}
	var output bytes.Buffer
	if err := renderCurrentSessionState(&output, result); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Session status: Orc could not save notification status", "Persistence: Orc could not save notification status"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("state output missing %q: %s", want, output.String())
		}
	}
}

func TestCurrentSessionStateExplainsRegistrationStateFailures(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"registration_state_lost", "registration state disappeared; retained registrations are unavailable until steer.json is restored"},
		{"registration_state_malformed", "registration state is malformed; repair steer.json"},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			var output bytes.Buffer
			result := currentSessionState{Joined: true, Actor: "reviewer", Role: "reviewer", Delivery: "degraded", StatusCode: test.code, ManagedOwnership: "clear"}
			if err := renderCurrentSessionState(&output, result); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), "Session status: "+test.want) {
				t.Fatalf("state output did not explain %s: %s", test.code, output.String())
			}
		})
	}
}

func TestCurrentSessionStateJSONKeepsStructuredCounts(t *testing.T) {
	data, err := json.Marshal(currentSessionState{Joined: true, ReadyCount: 2, ActiveClaims: []localTicket{}})
	if err != nil || !strings.Contains(string(data), `"ready_count":2`) {
		t.Fatalf("state JSON=%s err=%v", data, err)
	}
}
