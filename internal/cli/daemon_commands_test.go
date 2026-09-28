package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func TestDaemonErrorNeedsStatusUsesStructuredResult(t *testing.T) {
	for _, test := range []struct {
		name string
		err  *daemonclient.Error
		want bool
	}{
		{name: "uncertain result", err: &daemonclient.Error{Kind: daemonclient.ErrorApplication, Result: &daemon.MutationResult{Applied: false}}, want: true},
		{name: "applied result", err: &daemonclient.Error{Kind: daemonclient.ErrorApplication, Result: &daemon.MutationResult{Applied: true}, Applied: true}},
		{name: "no result", err: &daemonclient.Error{Kind: daemonclient.ErrorApplication}},
		{name: "transport", err: &daemonclient.Error{Kind: daemonclient.ErrorTransport, Result: &daemon.MutationResult{Applied: false}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := daemonErrorNeedsStatus(test.err); got != test.want {
				t.Fatalf("daemonErrorNeedsStatus() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestDaemonCompactStatusHasBlankBoundaries(t *testing.T) {
	var output bytes.Buffer
	status := daemon.Status{Version: "dev", Protocol: 1, Mode: "paused"}
	renderDaemonCompactStatus(&output, status)
	if got := output.String(); !strings.HasPrefix(got, "\n") || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("daemon status output must start and end with a blank line: %q", got)
	}
	if !strings.Contains(output.String(), "daemon mode=paused") {
		t.Fatalf("compact status omitted daemon mode: %q", output.String())
	}
	encoded, err := json.Marshal(status)
	if err != nil || !strings.Contains(string(encoded), `"mode":"paused"`) {
		t.Fatalf("JSON status omitted daemon mode: %s err=%v", encoded, err)
	}
}

func TestDaemonAbortHelpAndCompactResults(t *testing.T) {
	var help, output bytes.Buffer
	writeDaemonHelp(&help, "abort")
	for _, want := range []string{"managed workers", "best-effort", "remote termination is not guaranteed", "--endpoint URL", "--config PATH", "--output MODE"} {
		if !strings.Contains(help.String(), want) {
			t.Errorf("abort help omitted %q: %s", want, help.String())
		}
	}
	result := daemon.DaemonControlResult{Mode: "aborted", Applied: true, Targets: []daemon.DaemonAbortTargetResult{
		{Kind: "managed", Worker: "coder", Outcome: "terminated"},
		{Kind: "steer", RepositoryID: "repo-id", Actor: "reviewer", Outcome: "queued", Code: "abort_queued"},
	}}
	renderDaemonAbortCompact(&output, result)
	for _, want := range []string{"daemon mode=aborted mutation_applied=true", "worker=coder", "outcome=terminated", "actor=reviewer", "outcome=queued", "code=abort_queued"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("abort result omitted %q: %s", want, output.String())
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(encoded), `"mode":"aborted"`) || !strings.Contains(string(encoded), `"targets"`) {
		t.Fatalf("abort JSON result=%s err=%v", encoded, err)
	}
}

func TestParseDaemonOptionsResolveOneLocalRoot(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "orc.json")
	writeConfigFixture(t, configPath, `{"version":1,"local_dir":"runtime"}`)
	lookup := mapEnv(map[string]string{"TICKET_ORC_STATE_DIR": "/env/state"})
	options, positional, help, err := parseDaemonCommandOptions([]string{"--endpoint", "http://127.0.0.1:1/cap", "--config", configPath, "--output", "json"}, lookup, true)
	if err != nil || help || len(positional) != 0 {
		t.Fatalf("parse abort options positional=%v help=%t err=%v", positional, help, err)
	}
	if options.localDir != filepath.Join(filepath.Dir(configPath), "runtime") || options.endpoint != "http://127.0.0.1:1/cap" || options.configPath != configPath || options.output != OutputJSON {
		t.Fatalf("parsed abort options=%#v", options)
	}
	if _, _, _, err := parseDaemonCommandOptions([]string{"--state-dir", "/split/root"}, emptyEnv, false); err == nil || !strings.Contains(err.Error(), "unknown option --state-dir") {
		t.Fatalf("state-dir override error = %v", err)
	}
}

func TestTopLevelAbortRoutesToTypedDaemonEndpoint(t *testing.T) {
	var method, path string
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		_ = json.NewEncoder(w).Encode(daemon.DaemonControlResult{Mode: "aborted", Applied: true})
	}))
	defer server.Close()
	var output, stderr bytes.Buffer
	endpoint, err := daemon.ReadEndpoint(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"abort", "--endpoint", endpoint.CapabilityURL(), "--output", "json"}, &output, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 {
		t.Fatalf("abort code=%d output=%q stderr=%q", code, output.String(), stderr.String())
	}
	if method != http.MethodPost || path != "/v1/abort" || !strings.Contains(output.String(), `"mode":"aborted"`) {
		t.Fatalf("abort routed method=%q path=%q output=%q", method, path, output.String())
	}
}
