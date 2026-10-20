package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// The fixture accepts exactly one public command and fails on any reread.
func runRepositoryDeleteTicket() int {
	log, err := os.OpenFile(os.Getenv("ORC_TEST_DELETE_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return 90
	}
	defer log.Close()
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		var req struct {
			Args []string `json:"args"`
		}
		if json.Unmarshal(scan.Bytes(), &req) != nil {
			return 91
		}
		fmt.Fprintln(log, string(scan.Bytes()))
		if os.Getenv("TICKET_ACTOR") != "ui-user" || os.Getenv("TICKET_REPOSITORY") != os.Getenv("ORC_TEST_DELETE_REPOSITORY") {
			return 92
		}
		if !reflect.DeepEqual(req.Args, []string{"delete", "20261017-00001"}) {
			return 93
		}
		switch os.Getenv("ORC_TEST_DELETE_RESULT") {
		case "missing":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"not_found","message":"private /path","details":{"mutation_applied":false}}}`)
		case "archived":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"archived","message":"private /path","details":{"mutation_applied":false}}}`)
		case "applied":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"sync_failed","message":"private /path","details":{"mutation_applied":true}}}`)
		case "null-certainty":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"sync_failed","message":"private /path","details":{"mutation_applied":null}}}`)
		case "absent-certainty":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"sync_failed","message":"private /path","details":{}}}`)
		case "malformed-certainty":
			fmt.Fprintln(os.Stdout, `{"error":{"code":"sync_failed","message":"private /path","details":{"mutation_applied":"false"}}}`)
		case "unknown":
			return 94
		case "malformed":
			fmt.Fprintln(os.Stdout, `{"id":"20261017-00001"}`)
		case "wrong-id":
			fmt.Fprintln(os.Stdout, `{"id":"20261017-00002","changed":true}`)
		case "unchanged":
			fmt.Fprintln(os.Stdout, `{"id":"20261017-00001","changed":false}`)
		default:
			fmt.Fprintln(os.Stdout, `{"id":"20261017-00001","changed":true}`)
		}
	}
	return 0
}

func TestRepositoryDeleteHTTPGatewayAndClient(t *testing.T) {
	dir := t.TempDir()
	name := "ticket"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	data, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ORC_TEST_DELETE", "1")
	repository := filepath.Join(dir, "repository")
	t.Setenv("ORC_TEST_DELETE_REPOSITORY", repository)
	ctx := context.Background()
	manager := newWorkerManager(ctx, RunConfig{StateDir: t.TempDir(), Repositories: supervisor.RepositoryRegistry{"project": {
		ID: joinTestRepositoryID, Key: "project", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: repository},
	}}}, "", io.Discard, io.Discard, nil)
	server, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir(), RepositoryGateway: manager.repositoryGateway()})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(); waitForTestDaemonStopped(t, server) })
	client, err := daemonclient.NewWithEndpoint("", server.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	id := "20261017-00001"
	for _, tc := range []struct {
		mode, code              string
		known, applied, removed bool
	}{
		{"success", "", false, false, true},
		{"unchanged", "", false, false, false},
		{"missing", "ticket_not_found", true, false, false},
		{"archived", "ticket_archived", true, false, false},
		{"applied", "repository_mutation_failed", true, true, true},
		{"null-certainty", "repository_mutation_failed", false, false, false},
		{"absent-certainty", "repository_mutation_failed", false, false, false},
		{"malformed-certainty", "repository_mutation_failed", false, false, false},
		{"unknown", "repository_mutation_failed", false, false, false},
		{"malformed", "repository_mutation_failed", false, false, false},
		{"wrong-id", "repository_mutation_failed", false, false, false},
		{"busy", "repository_workers_running", true, false, false},
		{"cleanup-failure", "deletion_cleanup_failed", true, true, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "calls")
			t.Setenv("ORC_TEST_DELETE_LOG", logPath)
			t.Setenv("ORC_TEST_DELETE_RESULT", tc.mode)
			store := state.NewForRepository(manager.localDir, joinTestRepositoryID)
			other := state.NewForRepository(manager.localDir, joinOtherRepositoryID)
			for _, s := range []*state.Store{store, other} {
				if err := s.SetSession(ctx, state.Session{Ticket: id, Role: "coder", Harness: "codex", ID: "session"}); err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.Participate(ctx, id, 3); err != nil {
					t.Fatal(err)
				}
			}
			// An unrelated repository's live child never prevents deletion here.
			identity := joinOtherRepositoryID
			if tc.mode == "busy" {
				identity = joinTestRepositoryID
			}
			manager.lifecycle.TrackChild("child", &runChild{worker: supervisor.RunWorker{Config: supervisor.RoleConfig{RepositoryIdentity: identity}}})
			defer manager.lifecycle.ForgetChild("child")
			if tc.mode == "cleanup-failure" {
				original := manager.localDir
				manager.localDir = filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(manager.localDir, []byte("blocked"), 0600); err != nil {
					t.Fatal(err)
				}
				defer func() { manager.localDir = original }()
			}
			result, err := client.DeleteRepositoryTicket(ctx, joinTestRepositoryID, id, daemon.RepositoryTicketDeleteRequest{Actor: "ui-user"})
			if tc.code == "" {
				if err != nil || result.ID != id || result.RepositoryID != joinTestRepositoryID || result.RepositoryKey != "project" || result.Operation != "delete" || result.Changed != tc.removed {
					t.Fatalf("result=%#v err=%v", result, err)
				}
			} else {
				var failure *daemonclient.Error
				if !errors.As(err, &failure) || failure.Code != tc.code || failure.Applied != tc.applied || failure.AppliedKnown != tc.known || strings.Contains(err.Error(), "private") {
					t.Fatalf("result=%#v err=%#v", result, err)
				}
			}
			_, found, err := store.GetSession(ctx, id, "coder", "codex")
			if err != nil || found == tc.removed {
				t.Fatalf("session found=%v err=%v", found, err)
			}
			_, loop, err := store.TicketLoop(ctx, id)
			if err != nil || loop == tc.removed {
				t.Fatalf("loop found=%v err=%v", loop, err)
			}
			_, found, err = other.GetSession(ctx, id, "coder", "codex")
			if err != nil || !found {
				t.Fatalf("other repository affected: %v %v", found, err)
			}
			calls, err := os.ReadFile(logPath)
			if tc.mode == "busy" {
				if !os.IsNotExist(err) && len(calls) != 0 {
					t.Fatalf("busy deletion invoked Ticket: %s", calls)
				}
				return
			}
			if err != nil || len(strings.Split(strings.TrimSpace(string(calls)), "\n")) != 1 {
				t.Fatalf("delete retried or reread: %s err=%v", calls, err)
			}
		})
	}
	for _, tc := range []struct {
		target, body string
		status       int
	}{
		{"20261017-00001/delete", `{"actor":"ui-user","path":"/bad"}`, 400},
		{"20261017-00001/delete", `{"actor":"ui-user","message":"no"}`, 400},
		{"20261017-00001/delete", `{"actor":"ui-user"} {}`, 400},
		{"20261017-00001/delete", `{"actor":"bad actor"}`, 400},
		{"partial/delete", `{"actor":"ui-user"}`, 400},
		{"20261017-00001/delete?args=all", `{"actor":"ui-user"}`, 400},
	} {
		req, _ := http.NewRequest(http.MethodPost, server.Endpoint().CapabilityURL()+"/v1/repositories/"+joinTestRepositoryID+"/tickets/"+tc.target, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != tc.status {
			t.Fatalf("%s: status=%d", tc.target, res.StatusCode)
		}
	}
	_, err = client.DeleteRepositoryTicket(ctx, "project", id, daemon.RepositoryTicketDeleteRequest{Actor: "ui-user"})
	var failure *daemonclient.Error
	if !errors.As(err, &failure) || failure.Code != "repository_not_found" {
		t.Fatalf("unknown repository: %v", err)
	}
}

func testDeleteKey(t *testing.T) string {
	t.Helper()
	key, err := daemon.GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}
