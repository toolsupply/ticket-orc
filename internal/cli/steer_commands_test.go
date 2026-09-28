package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
)

const steerTestThread = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"

func writeSteerConfig(t *testing.T, dir string) {
	t.Helper()
	writeConfigFixture(t, filepath.Join(dir, "config.json"), fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."},"architect":{"ticket_queue":"open","nudge_prompt":"Plan."}}}`, dir))
}

func fakeTicketRunner(lockDir string, repositoryPaths ...string) (ticketCommandRunner, *[][]string) {
	calls := make([][]string, 0)
	repositoryPath := ""
	if len(repositoryPaths) != 0 {
		repositoryPath = repositoryPaths[0]
	} else {
		repositoryPath, _ = os.Getwd()
	}
	info, err := json.Marshal(map[string]any{
		"path": repositoryPath, "name": filepath.Base(repositoryPath),
		"id": "d659917f-5939-4e93-bfde-6346a0f2bc50", "format_version": 1,
		"storage_version": 1, "scope": nil,
	})
	runner := func(ctx context.Context, args ...string) ([]byte, error) {
		if err != nil {
			return nil, fmt.Errorf("encode fake Ticket info: %w", err)
		}
		calls = append(calls, append([]string(nil), args...))
		if lockDir != "" {
			lock, err := state.AcquireLock(ctx, lockDir, time.Second)
			if err != nil {
				return nil, fmt.Errorf("Ticket ran while state lock was held: %w", err)
			}
			if err := lock.Release(); err != nil {
				return nil, err
			}
		}
		switch strings.Join(args, " ") {
		case "actor -j":
			return []byte(`{"actor":"reviewer","source":"TICKET_ACTOR"}`), nil
		case "info -j":
			return info, nil
		case "check --active -j":
			return []byte(`{"ok":true}`), nil
		default:
			return nil, fmt.Errorf("unexpected Ticket arguments: %v", args)
		}
	}
	return runner, &calls
}

func TestJoinRejoinRoleChangeAndLeaveContract(t *testing.T) {
	dir := t.TempDir()
	repositoryDir := t.TempDir()
	writeSteerConfig(t, dir)
	thread := steerTestThread
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir, "CODEX_THREAD_ID": thread, "CODEX_HOME": filepath.Join(dir, "codex")})
	runner, calls := fakeTicketRunner(dir, repositoryDir)
	var output bytes.Buffer
	if err := joinWithRunner(context.Background(), "", &output, lookup, runner); err != nil {
		t.Fatalf("initial join: %v", err)
	}
	if !reflect.DeepEqual(*calls, [][]string{{"actor", "-j"}, {"info", "-j"}, {"check", "--active", "-j"}}) {
		t.Fatalf("Ticket command calls = %#v", *calls)
	}
	store := state.NewRegistrationStore(dir)
	registration, ok, err := store.Find(context.Background(), "d659917f-5939-4e93-bfde-6346a0f2bc50", "reviewer")
	if err != nil || !ok || registration.Role != "reviewer" || registration.CodexHome != filepath.Join(dir, "codex") || registration.RegistrationID == "" {
		t.Fatalf("registration=%#v ok=%v err=%v", registration, ok, err)
	}
	firstRegistrationID := registration.RegistrationID
	if registration.RepositoryPath != repositoryDir || registration.ThreadID != thread {
		t.Fatalf("registration routing = %#v", registration)
	}
	output.Reset()
	if err := joinWithRunner(context.Background(), "", &output, lookup, runner); err != nil || !strings.Contains(output.String(), "already joined as reviewer") {
		t.Fatalf("exact rejoin output=%q err=%v", output.String(), err)
	}
	registration, _, err = store.Find(context.Background(), registration.RepositoryID, registration.Actor)
	if err != nil || registration.RegistrationID != firstRegistrationID {
		t.Fatalf("exact rejoin changed incarnation ID: %#v err=%v", registration, err)
	}
	output.Reset()
	if err := joinWithRunner(context.Background(), "architect", &output, lookup, runner); err != nil || !strings.Contains(output.String(), "role changed: reviewer -> architect") {
		t.Fatalf("role change output=%q err=%v", output.String(), err)
	}
	registration, _, err = store.Find(context.Background(), registration.RepositoryID, registration.Actor)
	if err != nil || registration.Role != "architect" || registration.RegistrationID == firstRegistrationID {
		t.Fatalf("role change registration=%#v err=%v", registration, err)
	}
	output.Reset()
	removed, err := leaveWithRunner(context.Background(), &output, lookup, runner)
	if err != nil || !removed {
		t.Fatalf("leave removed=%v err=%v output=%q", removed, err, output.String())
	}
	if _, ok, err := store.Find(context.Background(), registration.RepositoryID, registration.Actor); err != nil || ok {
		t.Fatalf("registration remains after leave: ok=%v err=%v", ok, err)
	}
}

func TestJoinRolePrecedenceAndUnknownRole(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir, "TICKET_ORC_ROLE": "reviewer", "CODEX_THREAD_ID": steerTestThread})
	runner, _ := fakeTicketRunner("")
	var output bytes.Buffer
	if err := joinWithRunner(context.Background(), "architect", &output, lookup, runner); err != nil {
		t.Fatal(err)
	}
	registration, ok, err := state.NewRegistrationStore(dir).Find(context.Background(), "d659917f-5939-4e93-bfde-6346a0f2bc50", "reviewer")
	if err != nil || !ok || registration.Role != "architect" {
		t.Fatalf("positional role registration=%#v ok=%v err=%v", registration, ok, err)
	}
	unknownRunner, unknownCalls := fakeTicketRunner("")
	if err := joinWithRunner(context.Background(), "missing", &output, lookup, unknownRunner); err == nil || !strings.Contains(err.Error(), "unknown role") || len(*unknownCalls) != 0 {
		t.Fatalf("unknown role error=%v Ticket calls=%v", err, *unknownCalls)
	}
	if _, err := resolveSteerRole("missing", FileConfig{Roles: map[string]RoleFileConfig{"coder": {}}}, emptyEnv); err == nil || !strings.Contains(err.Error(), "unknown role") {
		t.Fatalf("unknown role error=%v", err)
	}
	if role, err := resolveSteerRole("", FileConfig{DefaultRole: "coder", Roles: map[string]RoleFileConfig{"coder": {}, "reviewer": {}}}, lookup); err != nil || role != "reviewer" {
		t.Fatalf("environment role=%q err=%v", role, err)
	}
}

func TestJoinInfersRoleFromTicketActorAndPreservesExplicitOverrides(t *testing.T) {
	tests := []struct {
		name        string
		actor       string
		requested   string
		envRole     string
		defaultRole string
		wantRole    string
		wantError   string
	}{
		{name: "matching actor overrides different default", actor: "reviewer", defaultRole: "coder", wantRole: "reviewer"},
		{name: "actor matches default", actor: "coder", defaultRole: "coder", wantRole: "coder"},
		{name: "named actor uses default", actor: "alice", defaultRole: "coder", wantRole: "coder"},
		{name: "role matching remains case sensitive", actor: "Reviewer", defaultRole: "coder", wantRole: "coder"},
		{name: "environment override", actor: "reviewer", envRole: "coder", defaultRole: "reviewer", wantRole: "coder"},
		{name: "positional override", actor: "reviewer", requested: "architect", envRole: "coder", defaultRole: "reviewer", wantRole: "architect"},
		{name: "invalid positional remains an error", actor: "reviewer", requested: "missing", defaultRole: "coder", wantError: "unknown role"},
		{name: "invalid environment remains an error", actor: "reviewer", envRole: "missing", defaultRole: "coder", wantError: "unknown role"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			defaultField := ""
			if test.defaultRole != "" {
				defaultField = `,"default_role":"` + test.defaultRole + `"`
			}
			writeConfigFixture(t, filepath.Join(dir, "config.json"), fmt.Sprintf(`{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q%s,"roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."},"architect":{"ticket_queue":"open","nudge_prompt":"Plan."}}}`, dir, defaultField))
			values := map[string]string{"TICKET_ORC": dir, "CODEX_THREAD_ID": steerTestThread}
			if test.envRole != "" {
				values["TICKET_ORC_ROLE"] = test.envRole
			}
			lookup := mapEnv(values)
			runner, calls := fakeTicketRunner("")
			actorRunner := func(ctx context.Context, args ...string) ([]byte, error) {
				if strings.Join(args, " ") == "actor -j" {
					*calls = append(*calls, append([]string(nil), args...))
					return []byte(`{"actor":"` + test.actor + `","source":"TICKET_ACTOR"}`), nil
				}
				return runner(ctx, args...)
			}
			var output bytes.Buffer
			err := joinWithRunner(context.Background(), test.requested, &output, lookup, actorRunner)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("join error=%v, want %q", err, test.wantError)
				}
				if test.requested != "" || test.envRole != "" {
					if len(*calls) != 0 {
						t.Fatalf("invalid explicit role made Ticket calls: %v", *calls)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("join: %v", err)
			}
			registration, ok, err := state.NewRegistrationStore(dir).Find(context.Background(), "d659917f-5939-4e93-bfde-6346a0f2bc50", test.actor)
			if err != nil || !ok || registration.Role != test.wantRole {
				t.Fatalf("registration=%#v ok=%v err=%v, want role %q", registration, ok, err, test.wantRole)
			}
		})
	}
	if _, err := resolveSteerRole("", FileConfig{DefaultRole: "missing", Roles: map[string]RoleFileConfig{"coder": {}}}, emptyEnv); err == nil || !strings.Contains(err.Error(), "unknown role") {
		t.Fatalf("invalid default role error=%v", err)
	}
	if _, err := resolveSteerRole("", FileConfig{Roles: map[string]RoleFileConfig{"coder": {}}}, emptyEnv); err == nil || !strings.Contains(err.Error(), "no role selected") {
		t.Fatalf("absent default role error=%v", err)
	}
}

func TestJoinRejectsFailedActiveCheckAndPreservesRegistration(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir, "CODEX_THREAD_ID": steerTestThread})
	runner, _ := fakeTicketRunner("")
	var calls int
	failedCheck := func(ctx context.Context, args ...string) ([]byte, error) {
		calls++
		if strings.Join(args, " ") == "check --active -j" {
			return []byte(`{"ok":false}`), nil
		}
		return runner(ctx, args...)
	}
	var output bytes.Buffer
	if err := joinWithRunner(context.Background(), "coder", &output, lookup, failedCheck); err == nil || !strings.Contains(err.Error(), "did not confirm valid active metadata") {
		t.Fatalf("active check error=%v", err)
	}
	if calls != 3 {
		t.Fatalf("Ticket calls=%d, want actor/info/check", calls)
	}
	if _, err := os.Stat(filepath.Join(dir, "steer.json")); !os.IsNotExist(err) {
		t.Fatalf("failed active check wrote registrations: %v", err)
	}
}

func TestWhoamiJSONUsesCurrentSessionRegistration(t *testing.T) {
	dir := t.TempDir()
	writeSteerConfig(t, dir)
	thread := steerTestThread
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir, "CODEX_THREAD_ID": thread, "CODEX_HOME": filepath.Join(dir, "codex")})
	runner, _ := fakeTicketRunner("")
	var stdout bytes.Buffer
	if err := joinWithRunner(context.Background(), "reviewer", io.Discard, lookup, runner); err != nil {
		t.Fatal(err)
	}
	if err := whoamiWithRunner(context.Background(), true, &stdout, lookup, runner); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["orc_id"] != "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa" || got["actor"] != "reviewer" || got["role"] != "reviewer" || got["joined"] != true {
		t.Fatalf("whoami JSON=%#v", got)
	}
}

func TestTicketIdentityUsesEffectiveDefaultCodexHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	runner, _ := fakeTicketRunner("")
	identity, err := discoverCurrentTicketIdentity(context.Background(), mapEnv(map[string]string{"CODEX_THREAD_ID": steerTestThread}), runner)
	if err != nil {
		t.Fatal(err)
	}
	if identity.CodexHome != filepath.Join(home, ".codex") || !filepath.IsAbs(identity.CodexHome) {
		t.Fatalf("effective Codex home=%q, want absolute default under %q", identity.CodexHome, home)
	}
}
