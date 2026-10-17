package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func TestParseConsoleCommandGrammarAndBounds(t *testing.T) {
	tests := []struct {
		line string
		want string
		err  bool
	}{
		{line: "status", want: "status"},
		{line: "status coder", want: "status"},
		{line: "status detail", want: "status"},
		{line: "workers", want: "workers"},
		{line: "workers verbose", want: "workers"},
		{line: "list", want: "workers"},
		{line: "ls", want: "workers"},
		{line: "list detail", want: "workers"},
		{line: "ls verbose", want: "workers"},
		{line: "list --help", want: "help"},
		{line: "ls -h", want: "help"},
		{line: "worker status verbose", want: "worker"},
		{line: "help worker", want: "help"},
		{line: "help group", want: "help"},
		{line: "quit", want: "quit"},
		{line: "watch", want: "watch"},
		{line: "watch --full", err: true},
		{line: "watch -f", err: true},
		{line: "watch --follow", err: true},
		{line: "doctor", want: "doctor"},
		{line: "worker pause coder", want: "worker"},
		{line: "daemon pause", want: "daemon"},
		{line: "daemon resume", want: "daemon"},
		{line: "daemon abort", want: "daemon"},
		{line: "daemon pause --help", want: "help"},
		{line: "group start backend", want: "group"},
		{line: "shutdown", want: "shutdown"},
		{line: "shutdown confirm", want: "shutdown"},
		{line: "worker list", err: true},
		{line: "initialize all", err: true},
		{line: "worker initialize coder", err: true},
		{line: "worker invalid coder", err: true},
		{line: "daemon", err: true},
		{line: "daemon pause now", err: true},
		{line: "daemon status", err: true},
		{line: "shutdown now", err: true},
		{line: "help status", want: "help"},
		{line: "unknown", err: true},
	}
	for _, test := range tests {
		command, err := parseConsoleCommand(test.line)
		if test.err {
			if err == nil {
				t.Fatalf("line=%q accepted command=%#v", test.line, command)
			}
			continue
		}
		if err != nil || command.name != test.want {
			t.Fatalf("line=%q command=%#v err=%v", test.line, command, err)
		}
	}
	if _, err := parseConsoleCommand("watch --unknown"); err == nil {
		t.Fatal("watch accepted an unknown option")
	}
	if _, err := parseConsoleCommand(strings.Repeat("x", consoleMaxLine+1)); err == nil {
		t.Fatal("overlong command accepted")
	}
}

func TestListAliasesUseWorkersHelp(t *testing.T) {
	for _, line := range []string{"list --help", "ls -h"} {
		command, err := parseConsoleCommand(line)
		if err != nil {
			t.Fatalf("line=%q err=%v", line, err)
		}
		if command.name != "help" || len(command.args) != 1 || command.args[0] != "workers" {
			t.Fatalf("line=%q command=%#v, want workers help", line, command)
		}
	}
}

func TestParseConsoleDirectWorkerCommandsAndContextualHelp(t *testing.T) {
	for _, line := range []string{"start all", "stop coder", "restart all", "pause reviewer", "resume all", "worker status coder", "group start all", "start -h", "worker help", "worker status -h", "group -h", "status help"} {
		command, err := parseConsoleCommand(line)
		if err != nil {
			t.Fatalf("line=%q err=%v", line, err)
		}
		if line == "start -h" || line == "worker help" || line == "worker status -h" || line == "group -h" || line == "status help" {
			if command.name != "help" {
				t.Fatalf("line=%q command=%#v, want contextual help", line, command)
			}
		}
	}
	if _, err := parseConsoleCommand("detach"); err == nil {
		t.Fatal("detach command accepted")
	}
}

func TestConsoleWorkerPauseAndResumeFormsRemainWorkerCommands(t *testing.T) {
	for _, test := range []struct{ line, name, operation, worker string }{
		{"pause coder", "pause", "pause", "coder"},
		{"pause all", "pause", "pause", "all"},
		{"resume reviewer", "resume", "resume", "reviewer"},
		{"resume all", "resume", "resume", "all"},
		{"worker pause coder", "worker", "pause", "coder"},
		{"worker resume all", "worker", "resume", "all"},
	} {
		command, err := parseConsoleCommand(test.line)
		if err != nil {
			t.Fatalf("line=%q err=%v", test.line, err)
		}
		if command.name != test.name {
			t.Fatalf("line=%q parsed as %#v, want command %q", test.line, command, test.name)
		}
		operation, worker := command.name, command.args[0]
		if command.name == "worker" {
			operation, worker = command.args[0], command.args[1]
		}
		if operation != test.operation || worker != test.worker {
			t.Fatalf("line=%q operation=%q worker=%q, want %q %q", test.line, operation, worker, test.operation, test.worker)
		}
	}
}

func TestConsoleStatusShowsGlobalDaemonMode(t *testing.T) {
	var output bytes.Buffer
	renderConsoleStatus(&output, daemon.Status{Mode: "aborted"})
	if !strings.Contains(output.String(), "Daemon mode: aborted") {
		t.Fatalf("console status omitted global daemon mode: %q", output.String())
	}
}

func TestConsoleDaemonNamespaceRoutesTypedControls(t *testing.T) {
	var routes []string
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routes = append(routes, r.Method+" "+r.URL.Path)
		mode := map[string]string{"/v1/pause": "paused", "/v1/resume": "running", "/v1/abort": "aborted"}[r.URL.Path]
		if mode == "" {
			http.NotFound(w, r)
			return
		}
		result := daemon.DaemonControlResult{Mode: mode, Applied: true}
		if r.URL.Path == "/v1/abort" {
			result.Targets = []daemon.DaemonAbortTargetResult{{Kind: "steer", Actor: "reviewer", Outcome: "queued"}}
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"pause", "resume", "abort"} {
		command, err := parseConsoleCommand("daemon " + operation)
		if err != nil {
			t.Fatal(err)
		}
		var output, errorsOut bytes.Buffer
		if done, ok := executeConsoleCommandResult(context.Background(), client, command, &output, &errorsOut); done || !ok || errorsOut.Len() != 0 {
			t.Fatalf("daemon %s done=%t ok=%t output=%q errors=%q", operation, done, ok, output.String(), errorsOut.String())
		}
		if !strings.Contains(output.String(), map[string]string{"pause": "mode=paused", "resume": "mode=running", "abort": "actor=reviewer"}[operation]) {
			t.Fatalf("daemon %s output=%q", operation, output.String())
		}
	}
	if !reflect.DeepEqual(routes, []string{"POST /v1/pause", "POST /v1/resume", "POST /v1/abort"}) {
		t.Fatalf("daemon console routes=%v", routes)
	}
}

func TestConsoleHelpUsesPageBoundaries(t *testing.T) {
	var output bytes.Buffer
	if done, ok := executeConsoleCommandResult(context.Background(), nil, consoleCommand{name: "help"}, &output, io.Discard); done || !ok {
		t.Fatal("help ended the console or failed")
	}
	text := output.String()
	if !strings.HasPrefix(text, "\nticket-orc interactive console\n\n") || !strings.HasSuffix(text, "\n\n") || !strings.Contains(text, "Worker management:") || !strings.Contains(text, "Queue control:") || !strings.Contains(text, "Daemon control:") || !strings.Contains(text, "group") || !strings.Contains(text, "Control worker groups") || strings.Contains(text, "[NAME|verbose|detail]") || strings.Contains(text, "\n  list") {
		t.Fatalf("console help boundaries=%q", text)
	}
}

func TestConsoleHelpCommandDescriptionsShareShutdownWidth(t *testing.T) {
	var output bytes.Buffer
	writeConsoleHelp(&output)
	text := output.String()
	descriptions := []string{
		"Show managed workers, external sessions, and repository health",
		"List workers and show their status",
		"Start a worker",
		"Stop a worker",
		"Restart a worker",
		"Pause a worker",
		"Resume a worker",
		"Pause, resume, or abort daemon-wide work dispatch",
		"Control worker groups",
		"Diagnose and recover workers",
		"Show the current scheduling forecast",
		"Show live Orc activity until Ctrl-C",
		"Reload worker configuration",
		"Stop the daemon",
		"End this interactive session; leave the daemon running in attach mode; stop the foreground supervisor in run --interactive mode",
		"Show this help page",
	}
	column := func(description string) int {
		for _, line := range strings.Split(text, "\n") {
			if strings.Contains(line, description) {
				return strings.Index(line, description)
			}
		}
		return -1
	}
	want := column(descriptions[0])
	if want < 0 {
		t.Fatalf("missing command description %q in %q", descriptions[0], text)
	}
	for _, want := range []string{"leave the daemon running in attach mode", "stop the foreground supervisor in run --interactive mode"} {
		if !strings.Contains(text, want) {
			t.Fatalf("main console help omitted quit behavior %q: %q", want, text)
		}
	}
	for _, description := range descriptions[1:] {
		if got := column(description); got != want {
			t.Errorf("description %q starts at column %d, want %d", description, got, want)
		}
	}
}

func TestConsoleTopicHelpEndsWithBlankLineAndDescribesCommands(t *testing.T) {
	for topic := range consoleHelpTopics {
		t.Run(topic, func(t *testing.T) {
			var output bytes.Buffer
			writeConsoleTopicHelp(&output, topic)
			if !strings.HasSuffix(output.String(), "\n\n") {
				t.Fatalf("topic help lacks a blank line at the end: %q", output.String())
			}
			if !strings.HasPrefix(output.String(), "\nInteractive console help: ") {
				t.Fatalf("topic help is not labeled as console grammar: %q", output.String())
			}
			if strings.Contains(output.String(), "ticket-orc "+topic) {
				t.Fatalf("topic help implies top-level CLI syntax: %q", output.String())
			}
			if strings.Contains(output.String(), "See the interactive console help page") {
				t.Fatalf("topic help redirects instead of explaining the command: %q", output.String())
			}
		})
	}
	var quit, reload, doctor bytes.Buffer
	writeConsoleTopicHelp(&quit, "quit")
	writeConsoleTopicHelp(&reload, "reload")
	writeConsoleTopicHelp(&doctor, "doctor")
	for _, want := range []string{"End this interactive session", "attach mode the daemon keeps running", "foreground supervisor stops"} {
		if !strings.Contains(quit.String(), want) {
			t.Errorf("quit help omitted %q: %q", want, quit.String())
		}
	}
	if strings.Contains(quit.String(), "exit") || strings.Contains(quit.String(), "q  ") {
		t.Errorf("quit help documents convenience aliases: %q", quit.String())
	}
	if !strings.Contains(reload.String(), "Use doctor to start workers that need recovery") {
		t.Errorf("reload help omitted doctor recovery behavior: %q", reload.String())
	}
	if !strings.Contains(doctor.String(), "safely recoverable configured worker") {
		t.Errorf("doctor help omitted recovery behavior: %q", doctor.String())
	}
}

func TestConsoleHelpAdvertisesAcceptedConsoleGrammar(t *testing.T) {
	tests := []struct {
		topic string
		line  string
		form  string
	}{
		{"worker", "worker status [NAME|verbose|detail]", "worker status detail"},
		{"worker", "worker start|stop|restart|pause|resume NAME", "worker resume coder"},
		{"daemon", "daemon pause|resume|abort", "daemon abort"},
		{"group", "group start|stop NAME", "group start all"},
		{"status", "status [NAME|verbose|detail]", "status verbose"},
		{"workers", "workers [NAME|verbose|detail]", "workers coder"},
		{"start", "start NAME", "start all"},
		{"stop", "stop NAME", "stop coder"},
		{"restart", "restart NAME", "restart coder"},
		{"pause", "pause NAME", "pause coder"},
		{"resume", "resume NAME", "resume coder"},
		{"queue", "queue  Show", "queue"},
		{"watch", "watch  Show", "watch"},
		{"doctor", "doctor  Reload", "doctor"},
		{"reload", "reload  Apply", "reload"},
		{"shutdown", "shutdown confirm", "shutdown confirm"},
		{"quit", "quit  End", "quit"},
	}
	for _, test := range tests {
		t.Run(test.topic+"/"+test.form, func(t *testing.T) {
			var output bytes.Buffer
			writeConsoleTopicHelp(&output, test.topic)
			if !strings.Contains(output.String(), test.line) {
				t.Fatalf("console help omitted grammar %q: %q", test.line, output.String())
			}
			if _, err := parseConsoleCommand(test.form); err != nil {
				t.Fatalf("advertised console form %q is rejected: %v", test.form, err)
			}
		})
	}
	for _, form := range []string{"ticket-orc queue", "config check", "report review", "status --state-dir .ticket-orc"} {
		if _, err := parseConsoleCommand(form); err == nil {
			t.Errorf("top-level CLI form %q was accepted by the console parser", form)
		}
	}
}

func TestConsoleHelpRendersAlignedLongCommands(t *testing.T) {
	var output bytes.Buffer
	writeConsoleHelp(&output)
	lines := strings.Split(output.String(), "\n")
	workerCommandStart := -1
	for _, line := range lines {
		if strings.HasPrefix(line, "  status ") {
			workerCommandStart = strings.Index(line, "Show managed workers")
		}
		if strings.HasPrefix(line, "  workers ") && strings.Index(line, "List workers") != workerCommandStart {
			t.Fatalf("worker command description is misaligned: status column=%d line=%q", workerCommandStart, line)
		}
	}
	if workerCommandStart < 0 || !strings.Contains(output.String(), "watch") || !strings.Contains(output.String(), "Show live Orc activity") || strings.Contains(output.String(), "--full") || strings.Contains(output.String(), "--follow") {
		t.Fatalf("console help omitted or failed to align long command form: %q", output.String())
	}
}

func TestConsoleWorkersCommandSpecificHelp(t *testing.T) {
	for _, line := range []string{"help workers", "workers -h", "workers --help"} {
		command, err := parseConsoleCommand(line)
		if err != nil || command.name != "help" || len(command.args) != 1 || command.args[0] != "workers" {
			t.Fatalf("line=%q command=%#v err=%v", line, command, err)
		}
		var output bytes.Buffer
		if _, ok := executeConsoleCommandResult(context.Background(), nil, command, &output, io.Discard); !ok {
			t.Fatalf("line=%q help failed", line)
		}
		if !strings.Contains(output.String(), "workers [NAME|verbose|detail]") {
			t.Fatalf("line=%q help=%q", line, output.String())
		}
	}
	if _, err := parseConsoleCommand("help list"); err == nil {
		t.Fatal("removed list command still has a help topic")
	}
}

func TestExecuteConsoleAllWorkersIsSequentialAndReportsPartialFailures(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workers" {
			_ = json.NewEncoder(w).Encode(struct {
				Workers []daemon.WorkerStatus `json:"workers"`
			}{Workers: []daemon.WorkerStatus{{Name: "coder"}, {Name: "reviewer"}}})
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 4 && parts[0] == "v1" && parts[1] == "workers" {
			mu.Lock()
			requests = append(requests, parts[2]+"/"+parts[3])
			mu.Unlock()
			if parts[2] == "reviewer" && parts[3] == "start" {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "worker_unavailable", "message": "worker unavailable"}})
				return
			}
			_ = json.NewEncoder(w).Encode(daemon.MutationResult{Worker: parts[2], State: "accepted", Applied: true})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	var output, errorsOut bytes.Buffer
	if _, ok := executeConsoleCommandResult(context.Background(), client, consoleCommand{name: "start", args: []string{"all"}}, &output, &errorsOut); ok {
		t.Fatal("partial start unexpectedly succeeded")
	}
	if !strings.Contains(output.String(), "worker coder") || !strings.Contains(output.String(), "worker reviewer request failed") || !strings.Contains(output.String(), "start all: 1 succeeded, 1 failed") {
		t.Fatalf("partial start output=%q errors=%q", output.String(), errorsOut.String())
	}
	mu.Lock()
	got := append([]string(nil), requests...)
	mu.Unlock()
	want := []string{"coder/start", "reviewer/start"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request order=%#v want=%#v", got, want)
	}
}
