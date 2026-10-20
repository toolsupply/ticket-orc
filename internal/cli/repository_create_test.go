package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// runRepositoryCreateTicket records the subprocess contract. Ticket itself owns
// validation and atomic filesystem publication; the fixture rejects explicit
// invalid values so the gateway's failure behavior can be asserted.
func runRepositoryCreateTicket() int {
	log, err := os.OpenFile(os.Getenv("ORC_TEST_CREATE_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return 90
	}
	defer log.Close()
	scan := bufio.NewScanner(os.Stdin)
	var input struct {
		State    *string `json:"state"`
		Assignee *string `json:"assignee"`
	}
	for scan.Scan() {
		var req struct {
			Args  []string `json:"args"`
			Stdin *string  `json:"stdin"`
		}
		if json.Unmarshal(scan.Bytes(), &req) != nil {
			return 91
		}
		fmt.Fprintln(log, string(scan.Bytes()))
		if os.Getenv("TICKET_ACTOR") != "ui-user" || os.Getenv("TICKET_REPOSITORY") != os.Getenv("ORC_TEST_CREATE_REPOSITORY") {
			return 92
		}
		switch {
		case reflect.DeepEqual(req.Args, []string{"create", "--input", "-"}):
			if req.Stdin == nil || json.Unmarshal([]byte(*req.Stdin), &input) != nil {
				return 93
			}
			if (input.State != nil && (*input.State == "" || *input.State == "invalid")) || (input.Assignee != nil && (*input.Assignee == "" || *input.Assignee == "bad owner")) {
				fmt.Fprintln(os.Stdout, `{"error":{"code":"invalid_argument","message":"invalid create fixture","details":{"mutation_applied":false}}}`)
			} else {
				// Deliberately omit lifecycle fields: the response must use the reread.
				fmt.Fprintln(os.Stdout, `{"id":"20261017-00001","changed":true}`)
			}
		case len(req.Args) >= 2 && req.Args[0] == "show" && req.Args[1] == "20261017-00001":
			state, owner := "open", ""
			if input.State != nil {
				state = *input.State
			}
			if input.Assignee != nil {
				owner = *input.Assignee
			}
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"id": "20261017-00001", "state": state, "assignee": owner, "readiness": map[string]any{"ready": false}})
		case reflect.DeepEqual(req.Args, []string{"status", "20261017-00001"}):
			fmt.Fprintln(os.Stdout, `{"id":"20261017-00001","created":"2026-10-17","modified":"2026-10-17T12:00:00Z"}`)
		default:
			return 94
		}
	}
	return 0
}

func TestRepositoryCreateInitialStateAndAssignee(t *testing.T) {
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
	t.Setenv("ORC_TEST_CREATE", "1")
	repository := filepath.Join(dir, "repository")
	t.Setenv("ORC_TEST_CREATE_REPOSITORY", repository)
	manager := &workerManager{repositoriesByID: map[string]supervisor.ConfiguredRepository{joinTestRepositoryID: {
		ID: joinTestRepositoryID, Key: "project", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: repository},
	}}}
	key, err := daemon.GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	server, err := daemon.NewServer(daemon.Config{EndpointKey: key, StateDir: t.TempDir(), RepositoryGateway: manager.repositoryGateway()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(); waitForTestDaemonStopped(t, server) })
	client, err := daemonclient.NewWithEndpoint("", server.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		state, owner *string
		fail         bool
	}{
		{"hold", strCreate("hold"), strCreate("engineer"), false},
		{"review", strCreate("review"), nil, false},
		{"open", strCreate("open"), nil, false},
		{"signoff", strCreate("signoff"), nil, false},
		{"closed", strCreate("closed"), nil, false},
		{"rejected", strCreate("rejected"), nil, false},
		{"assignee only", nil, strCreate("engineer"), false},
		{"omitted", nil, nil, false},
		{"empty state", strCreate(""), nil, true},
		{"invalid state", strCreate("invalid"), nil, true},
		{"empty owner", nil, strCreate(""), true},
		{"invalid owner", nil, strCreate("bad owner"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "calls.jsonl")
			t.Setenv("ORC_TEST_CREATE_LOG", logPath)
			request := daemon.RepositoryTicketCreateRequest{Actor: "ui-user", Title: "Populated", State: tc.state, Assignee: tc.owner, Sections: map[string]string{"objective": "Implement safely"}}
			result, err := client.CreateRepositoryTicket(ctx, joinTestRepositoryID, request)
			if tc.fail {
				if err == nil || result.Ticket.ID != "" {
					t.Fatalf("result=%#v err=%v", result, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				state, owner := "open", ""
				if tc.state != nil {
					state = *tc.state
				}
				if tc.owner != nil {
					owner = *tc.owner
				}
				if result.RepositoryID != joinTestRepositoryID || result.Ticket.State != state || result.Ticket.Assignee != owner {
					t.Fatalf("result=%#v", result)
				}
			}
			calls, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
			count := 4
			if tc.fail {
				count = 1
			}
			if len(lines) != count {
				t.Fatalf("calls=%s", calls)
			}
			var first struct {
				Args  []string `json:"args"`
				Stdin string   `json:"stdin"`
			}
			if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(first.Args, []string{"create", "--input", "-"}) {
				t.Fatalf("args=%q", first.Args)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(first.Stdin), &payload); err != nil {
				t.Fatal(err)
			}
			for field, want := range map[string]*string{"state": tc.state, "assignee": tc.owner} {
				got, exists := payload[field]
				if (want == nil && exists) || (want != nil && got != *want) {
					t.Fatalf("%s=%#v exists=%v want=%v", field, got, exists, want)
				}
			}
			if !reflect.DeepEqual(payload["sections"], map[string]any{"objective": "Implement safely"}) {
				t.Fatalf("payload=%v", payload)
			}
			if !tc.fail {
				wantReads := [][]string{
					{"show", "20261017-00001", "--readiness"},
					{"status", "20261017-00001"},
					{"show", "20261017-00001", "--full", "--max-bytes", "262144"},
				}
				for i, want := range wantReads {
					var reread struct {
						Args []string `json:"args"`
					}
					if err := json.Unmarshal([]byte(lines[i+1]), &reread); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(reread.Args, want) {
						t.Fatalf("follow-up=%q want=%q", reread.Args, want)
					}
				}
			}
		})
	}
	t.Run("unknown field", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "calls.jsonl")
		t.Setenv("ORC_TEST_CREATE_LOG", logPath)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.Endpoint().CapabilityURL()+"/v1/repositories/"+joinTestRepositoryID+"/tickets", strings.NewReader(`{"actor":"ui-user","title":"bad","unrelated":true}`))
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("status=%d", response.StatusCode)
		}
		if _, err := os.Stat(logPath); !os.IsNotExist(err) {
			t.Fatalf("unexpected subprocess call: %v", err)
		}
	})
}

func strCreate(s string) *string { return &s }
