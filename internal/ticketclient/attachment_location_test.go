package ticketclient

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttachmentLocationRealTicketTargets(t *testing.T) {
	executable, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("Ticket executable unavailable")
	}
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "config-scope"}[scoped], func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			target := Target{Repository: root}
			if scoped {
				config := filepath.Join(t.TempDir(), "scopes.json")
				data, _ := json.Marshal(map[string]any{"scopes": map[string]any{"alias": map[string]any{"repository": root}}})
				if err = os.WriteFile(config, data, 0600); err != nil {
					t.Fatal(err)
				}
				target = Target{Config: config, Scope: "alias"}
			}
			run := func(args ...string) []byte {
				t.Helper()
				cmd := exec.Command(executable, append(target.args(), args...)...)
				cmd.Dir = root
				cmd.Env = ApplyTargetEnvironment(os.Environ(), "tester", target)
				data, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("Ticket %v: %v %s", args, err, data)
				}
				return data
			}
			run("init")
			var created struct {
				ID string `json:"id"`
			}
			if err = json.Unmarshal(run("create", "attachment fixture", "resolver protocol test", "-j"), &created); err != nil {
				t.Fatal(err)
			}
			var info RepositoryInfo
			if err = json.Unmarshal(run("info", "-j"), &info); err != nil {
				t.Fatal(err)
			}
			resolve := func(cwd string) AttachmentLocation {
				t.Helper()
				client, err := NewWithExecutableWorkingDirAndTarget("tester", executable, cwd, target)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				loc, err := client.ResolveAttachmentLocation(context.Background(), info.ID, created.ID)
				if err != nil {
					t.Fatal(err)
				}
				return loc
			}
			// Ticket omits attachment_path outside its root; absence is advisory.
			unrelated := t.TempDir()
			loc := resolve(unrelated)
			if loc.RepositoryRoot != info.Path || loc.TicketDirectory != created.ID || loc.Archived {
				t.Fatal(loc)
			}
			attachmentRoot := filepath.Join(info.Path, filepath.FromSlash(loc.TicketDirectory), "attachments")
			if err = os.Mkdir(attachmentRoot, 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(attachmentRoot, "binary"), []byte{0, 1, 255}, 0600); err != nil {
				t.Fatal(err)
			}
			for _, cwd := range []string{info.Path, unrelated} {
				got := resolve(cwd)
				if got != loc {
					t.Fatalf("location changed with cwd: %v %v", loc, got)
				}
			}
			client, err := NewWithExecutableWorkingDirAndTarget("tester", executable, unrelated, target)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, err = client.ResolveAttachmentLocation(context.Background(), "8d1268c4-6a64-4b9b-95c9-d5598a150e86", created.ID)
			if !errors.Is(err, ErrProtocol) {
				t.Fatal("wrong repository accepted", err)
			}
			var deleted struct {
				ID string `json:"id"`
			}
			if err = json.Unmarshal(run("create", "deleted attachment fixture", "must not resolve", "-j"), &deleted); err != nil {
				t.Fatal(err)
			}
			run("delete", deleted.ID)
			_, err = client.ResolveAttachmentLocation(context.Background(), info.ID, deleted.ID)
			var missing *CommandError
			if !errors.As(err, &missing) || missing.Code != "not_found" {
				t.Fatalf("deleted ticket: %v", err)
			}
			// Archive/deletion are exercised only through Ticket's lifecycle CLI.
			run("claim", created.ID)
			run("submit", created.ID)
			run("approve", created.ID)
			run("close", created.ID, "--outcome", "fixture complete")
			run("archive", created.ID)
			loc = resolve(unrelated)
			if !loc.Archived || loc.TicketDirectory != "archive/"+created.ID {
				t.Fatal(loc)
			}
		})
	}
}

func TestAttachmentLocationProtocolValidation(t *testing.T) {
	for _, scenario := range []string{"wrong-repository", "wrong-show-id", "wrong-path-id", "escaping-path", "mismatched-path", "missing-archived", "advisory-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "attachment-"+scenario)
			client := startHelperClient(t, "tester")
			defer client.Close()
			_, err := client.ResolveAttachmentLocation(context.Background(), "8d1268c4-6a64-4b9b-95c9-d5598a150e86", testID)
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected protocol rejection, got %v", err)
			}
		})
	}
}

func attachmentLocationHelper(scenario string, args []string) {
	const repo = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	switch args[0] {
	case "info":
		id := repo
		if scenario == "attachment-wrong-repository" {
			id = "8d1268c4-6a64-4b9b-95c9-d5598a150e87"
		}
		writeHelperJSON(map[string]any{"id": id, "path": filepath.Join(string(filepath.Separator), "repo")})
	case "show":
		if strings.Join(args, " ") != "show "+testID+" --max-bytes 1" {
			writeHelperError("bad_args", "unexpected unbounded show", nil)
			return
		}
		payload := map[string]any{"id": testID, "path": testID + "/TASK.md", "archived": false}
		switch scenario {
		case "attachment-wrong-show-id":
			payload["id"] = "20261025-12345"
		case "attachment-escaping-path":
			payload["path"] = "../" + testID + "/TASK.md"
		case "attachment-missing-archived":
			delete(payload, "archived")
		case "attachment-advisory-mismatch":
			payload["attachment_path"] = "wrong/attachments"
		}
		writeHelperJSON(payload)
	case "path":
		id := testID
		path := testID + "/TASK.md"
		if scenario == "attachment-wrong-path-id" {
			id = "20261025-12345"
		}
		if scenario == "attachment-escaping-path" {
			path = "../" + path
		}
		if scenario == "attachment-mismatched-path" {
			path = "archive/" + path
		}
		writeHelperJSON(map[string]any{"id": id, "path": path})
	}
}
