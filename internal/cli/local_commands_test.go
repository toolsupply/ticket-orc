package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
	calls         []string
	closed        bool
}

func (stub *localQueueStub) ActiveClaims(_ context.Context, queue string, _ []string, limit int) (ticketclient.ListResult, error) {
	stub.calls = append(stub.calls, "active:"+queue+":"+fmt.Sprint(limit))
	return stub.active, nil
}
func (stub *localQueueStub) ReadyFrontier(_ context.Context, queue string, _ []string, limit int) (ticketclient.ListResult, error) {
	stub.calls = append(stub.calls, "ready:"+queue+":"+fmt.Sprint(limit))
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
	if !strings.Contains(strings.Join(queries.calls, ","), "active:review:20") || !strings.Contains(strings.Join(queries.calls, ","), "ready:review:20") || !queries.closed {
		t.Fatalf("Ticket observations=%v closed=%v", queries.calls, queries.closed)
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
	queries := &localQueueStub{active: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260926-54321", State: "review", Assignee: "reviewer"}}}, ready: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260926-12345", State: "review"}}, More: true}}
	result, err := currentSessionStateWithRunner(context.Background(), dir, localIdentityEnv(dir), runner, localQueueFactory(queries), readLocalDaemonStatus)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Joined || result.Role != "reviewer" || result.ReadyCount != 1 || !result.ReadyMore || len(result.ActiveClaims) != 1 || result.Delivery != "queued" {
		t.Fatalf("session state=%#v", result)
	}
	var output bytes.Buffer
	if err := renderCurrentSessionState(&output, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Work: 1+ ready") || !strings.Contains(output.String(), "Active: 20260926-54321") || !strings.Contains(output.String(), "Delivery: notification queued") {
		t.Fatalf("state output=%q", output.String())
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
		return daemon.Status{Steer: []daemon.SteerStatus{{RepositoryID: localRepoID, Actor: "reviewer", Session: steerTestThread, State: "sending", Code: "runtime_state_write_failed", PersistenceCode: "runtime_state_write_failed"}}}, nil
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
