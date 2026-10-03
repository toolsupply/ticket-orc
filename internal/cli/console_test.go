package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestRenderConsoleEventIncludesTicketIDWhenAvailable(t *testing.T) {
	var output bytes.Buffer
	renderConsoleEvent(&output, daemon.Event{Type: "ticket.lifecycle", Worker: "reviewer", Role: "reviewer", State: "review", Ticket: "20260922-73452"})
	text := output.String()
	if !strings.Contains(text, "20260922-73452") || !strings.Contains(text, "entered review") {
		t.Fatalf("watch event did not append ticket ID to event text: %q", text)
	}
	customRoleMessage := consoleEventMessage(daemon.Event{Type: "ticket.lifecycle", Role: "architect", State: "review"})
	if customRoleMessage != "entered review" || strings.Contains(customRoleMessage, "reviewer") {
		t.Fatalf("custom-role review event=%q", customRoleMessage)
	}
	if len(text) < len(consoleTimestampLayout) || !strings.Contains(text[:len(consoleTimestampLayout)], ":") || strings.Contains(text[:len(consoleTimestampLayout)], "T") || strings.Contains(text[:len(consoleTimestampLayout)], "Z") {
		t.Fatalf("watch event timestamp is not local shell format: %q", text)
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

func TestRenderConsoleEventPrefixesTimestampAndTicket(t *testing.T) {
	var output bytes.Buffer
	renderConsoleEvent(&output, daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260922-02651"})
	fields := strings.Fields(output.String())
	if len(fields) < 5 || !strings.Contains(output.String(), "claimed 20260922-02651") || strings.Contains(output.String(), "Ticket") {
		t.Fatalf("timestamped event output=%q", output.String())
	}
	if _, err := time.ParseInLocation(consoleTimestampLayout, fields[0], time.Local); err != nil {
		t.Fatalf("event timestamp=%q err=%v", fields[0], err)
	}
	if !strings.Contains(output.String(), "coder") || !strings.Contains(output.String(), "20260922-02651") {
		t.Fatalf("event output omitted worker or ticket: %q", output.String())
	}
}

func TestConsoleWatchHeaderMovesSessionAndRemovesTicketColumn(t *testing.T) {
	if got, want := strings.Fields(consoleWatchHeader()), []string{"Time", "Repository", "Session", "Role", "Actor", "Event"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("watch header fields=%#v, want %#v", got, want)
	}
}

func TestConsoleWatchDescribesNotificationDeliveryAndClaimIDs(t *testing.T) {
	const ticketID = "20260926-54321"
	for _, test := range []struct {
		event daemon.Event
		want  string
	}{
		{event: daemon.Event{Type: "steer.delivery", State: "sending", Ticket: ticketID}, want: "sending notification for " + ticketID},
		{event: daemon.Event{Type: "steer.delivery", State: "queued", Ticket: ticketID}, want: "queued notification for " + ticketID},
		{event: daemon.Event{Type: "steer.status", State: "consumed", Ticket: ticketID}, want: "claimed " + ticketID},
	} {
		if got := consoleEventMessage(test.event); got != test.want {
			t.Errorf("consoleEventMessage(%#v)=%q, want %q", test.event, got, test.want)
		}
	}
}

func TestConsoleRepositoryWatchEventShowsKnownTicketDetails(t *testing.T) {
	const ticketID = "20260926-54321"
	for _, test := range []struct {
		name  string
		event daemon.Event
		want  string
	}{
		{name: "closed", event: daemon.Event{Type: "ticket.repository_changed", Ticket: ticketID, State: "closed", Actor: "coder", Code: "closed"}, want: "ticket " + ticketID + " modified (closed)"},
		{name: "claimed", event: daemon.Event{Type: "ticket.repository_changed", Ticket: ticketID, State: "review", Actor: "reviewer", Code: "claimed"}, want: "ticket " + ticketID + " modified (review, reviewer)"},
		{name: "old assignee attribution", event: daemon.Event{Type: "ticket.repository_changed", Ticket: ticketID, State: "review", Actor: "coder", Code: "submitted"}, want: "ticket " + ticketID + " modified (review)"},
		{name: "unknown details", event: daemon.Event{Type: "ticket.repository_changed", Ticket: ticketID}, want: "ticket " + ticketID + " modified"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := consoleEventMessage(test.event); got != test.want {
				t.Fatalf("consoleEventMessage=%q, want %q", got, test.want)
			}
			var output bytes.Buffer
			renderConsoleEventAt(&output, test.event, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC), nil)
			if got := strings.Count(output.String(), ticketID); got != 1 {
				t.Fatalf("rendered repository event contains ticket ID %d times: %q", got, output.String())
			}
		})
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

func TestConsoleWorkersRendersOnlySessionTable(t *testing.T) {
	var output bytes.Buffer
	status := daemon.Status{
		Steer: []daemon.SteerStatus{
			{RepositoryID: "repo-id", RepositoryName: "tickets", Role: "coder", Actor: "coder", Session: "01a0dcb8-6a64-4b9b-95c9-d5598a150e86", State: "none"},
			{RepositoryID: "repo-id", RepositoryName: "tickets", Role: "reviewer", Actor: "reviewer", Session: "01a0dcb4-6a64-4b9b-95c9-d5598a150e86", State: "none"},
		},
		Repositories: []daemon.RepositoryStatus{{ID: "repo-id", Key: "tickets", Path: "/private/tickets", State: "healthy"}},
	}
	renderConsoleWorkers(&output, status)
	text := output.String()
	if !strings.HasPrefix(text, "\n") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("workers output spacing=%q", text)
	}
	if strings.Contains(text, "Repositories:") || strings.Contains(text, "Sessions:") || strings.Contains(text, "Managed workers:") {
		t.Fatalf("workers output included a section heading: %q", text)
	}
	lines := strings.Split(text, "\n")
	wantHeader := renderColumnRow([]string{"Repository", "Session", "Role", "Ticket actor", "State", "Activity"}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0})
	wantCoder := renderColumnRow([]string{"tickets", "01a0dcb8…", "coder", "coder", "idle", "no work"}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0})
	wantReviewer := renderColumnRow([]string{"tickets", "01a0dcb4…", "reviewer", "reviewer", "idle", "no work"}, []int{consoleRepositoryLabelCol, consoleStatusSession, consoleStatusRoleCol, consoleStatusRoleCol, consoleStatusState, 0})
	if len(lines) != 6 || lines[0] != "" || lines[1] != wantHeader || lines[2] != wantCoder || lines[3] != wantReviewer || lines[4] != "" || lines[5] != "" {
		t.Fatalf("workers table lines=%q", lines)
	}
}

func TestRenderConsoleDetailStatusShowsFailureReason(t *testing.T) {
	var output bytes.Buffer
	renderConsoleDetailStatus(&output, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "failed", Reason: "worker failure requires operator recovery; run doctor", Failure: &daemon.WorkerFailure{Classification: "child_exit", Phase: "worker operation", ExitCode: 7}}}})
	text := output.String()
	if !strings.Contains(text, "Failure reason: worker failure requires operator recovery; run doctor") {
		t.Fatalf("failure reason missing: %q", text)
	}
	if !strings.Contains(text, "Failure: child_exit during worker operation exit_code=7") {
		t.Fatalf("failure metadata missing: %q", text)
	}
}

func TestRenderConsoleDetailStatusShowsTicketTransportCategory(t *testing.T) {
	var output bytes.Buffer
	renderConsoleDetailStatus(&output, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "reviewer", State: "failed", Reason: "ticket_transport_failed during reviewer ownership-baseline observation [transport process_exit]", Failure: &daemon.WorkerFailure{Classification: "ticket_transport_failed", Phase: "reviewer ownership-baseline observation", ExitCode: 9, TransportCategory: "process_exit"}}}})
	text := output.String()
	if !strings.Contains(text, "Failure reason: ticket_transport_failed during reviewer ownership-baseline observation [transport process_exit]") || !strings.Contains(text, "Failure: ticket_transport_failed during reviewer ownership-baseline observation exit_code=9 transport=process_exit") {
		t.Fatalf("ticket transport metadata missing: %q", text)
	}
}

func TestRenderConsoleDoctorReportsNamedOutcomes(t *testing.T) {
	var output bytes.Buffer
	renderConsoleDoctor(&output, daemon.DoctorResult{Reloaded: true, Workers: []daemon.DoctorWorkerResult{{Worker: "ticket-coder", Outcome: "recovered", Action: "restarted"}, {Worker: "ticket-reviewer", Outcome: "recovered", Action: "; start"}, {Worker: "failed-reviewer", Outcome: "failed", Reason: "exited during startup verification"}}})
	text := output.String()
	for _, want := range []string{"configuration reloaded", "ticket-coder: restarted", "ticket-reviewer: started", "failed-reviewer: failed — exited during startup verification"} {
		if !strings.Contains(text, want) {
			t.Fatalf("doctor output missing %q: %q", want, text)
		}
	}
}

func TestConsoleStatusAndWatchRowsHaveNoLeadingIndentation(t *testing.T) {
	var status, event bytes.Buffer
	renderConsoleStatus(&status, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", TicketActor: "coder", Harness: "codex", State: "running"}}})
	renderConsoleEvent(&event, daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260923-90139"})
	for _, line := range strings.Split(status.String(), "\n") {
		if strings.HasPrefix(line, "coder") {
			break
		}
		if strings.HasPrefix(line, "  coder") {
			t.Fatalf("status row retains indentation: %q", line)
		}
	}
	fields := strings.Fields(event.String())
	if len(fields) < 7 || fields[4] != "coder" || !strings.Contains(strings.Join(fields[5:], " "), "20260923-90139") {
		t.Fatalf("watch row omitted actor or full Ticket ID: %q", event.String())
	}
}

func TestConsoleVerboseStatusHasBlankBoundaries(t *testing.T) {
	var output bytes.Buffer
	renderConsoleVerboseStatus(&output, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
	if got := output.String(); !strings.HasPrefix(got, "\n") || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("verbose status output must start and end with a blank line: %q", got)
	}
}

func TestRenderConsoleDetailStatusIncludesTicketActivityTicket(t *testing.T) {
	activityAt := time.Date(2026, 9, 21, 15, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	renderConsoleDetailStatus(&output, daemon.Status{Workers: []daemon.WorkerStatus{{
		Name:                 "coder",
		State:                "running",
		TicketActivityAt:     activityAt,
		TicketActivity:       "claimed",
		TicketActivityTicket: "20260921-12345",
		TicketActivitySource: "ticket",
	}}})
	text := output.String()
	for _, want := range []string{"Ticket activity: claimed (ticket 20260921-12345)", activityAt.Local().Format(time.RFC3339)} {
		if !strings.Contains(text, want) {
			t.Fatalf("detail status missing %q: %q", want, text)
		}
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

func TestConsoleEventTrackerSuppressesResyncAndDuplicateState(t *testing.T) {
	tracker := newConsoleEventTracker()
	tracker.seed(daemon.Status{
		Workers:      []daemon.WorkerStatus{{Name: "coder", State: "running"}},
		Repositories: []daemon.RepositoryStatus{{ID: "repo-id", Key: "orc", State: "healthy"}},
	})
	if tracker.accept(daemon.Event{Type: "status.resync", Worker: "coder", State: "running"}) {
		t.Fatal("status resync should stay internal")
	}
	if tracker.accept(daemon.Event{Type: "status.resync", RepositoryID: "repo-id", RepositoryKey: "orc", State: "healthy"}) {
		t.Fatal("repository status resync should stay internal")
	}
	if tracker.accept(daemon.Event{Type: "worker.state", Worker: "coder", State: "running"}) {
		t.Fatal("duplicate state should be coalesced")
	}
	if !tracker.accept(daemon.Event{Type: "worker.state", Worker: "coder", State: "paused"}) {
		t.Fatal("state transition should be rendered")
	}
	claim := daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260926-00001"}
	if !tracker.accept(claim) || tracker.accept(claim) {
		t.Fatal("duplicate adjacent activity should be suppressed")
	}

	tracker = newConsoleEventTracker()
	ready := daemon.Event{Type: "worker.state", Worker: "coder", State: "running"}
	claimed := daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260926-00002"}
	if !tracker.accept(ready) || !tracker.accept(claimed) || tracker.accept(claimed) {
		t.Fatal("ready and claimed events were not deduplicated")
	}
	failure := daemon.Event{Type: "worker.failure", Worker: "coder", State: "failed"}
	if !tracker.accept(failure) || !tracker.accept(failure) {
		t.Fatal("worker failure was hidden by duplicate coalescing")
	}
}

func TestConsoleStatusRendersRepositoryHealthWithoutWorkers(t *testing.T) {
	var output bytes.Buffer
	renderConsoleStatus(&output, daemon.Status{Repositories: []daemon.RepositoryStatus{{
		Key: "orc", ID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Path: "/private/tickets", Name: "Ticket Orc", State: "degraded", Failure: "watch_exit",
	}}})
	text := output.String()
	for _, want := range []string{"Sessions: none", "Repositories:", "needs attention", "Ticket Orc", "/private/tickets"} {
		if !strings.Contains(text, want) {
			t.Fatalf("repository status missing %q: %q", want, text)
		}
	}
	if strings.Index(text, "Repositories:") > strings.Index(text, "Sessions:") {
		t.Fatalf("repository section should precede sessions: %q", text)
	}
	if strings.Contains(text, "Managed workers:") || strings.Contains(text, "watch_exit") || strings.Contains(text, "repository_id") || strings.Contains(text, "8d1268c4-") {
		t.Fatalf("compact repository table exposed diagnostics: %q", text)
	}
	var detail bytes.Buffer
	renderConsoleDetailStatus(&detail, daemon.Status{Repositories: []daemon.RepositoryStatus{{Key: "orc", ID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Name: "Ticket Orc", Path: "/work/orc/tickets", State: "degraded", Failure: "watch_exit", RestartCount: 2}}})
	for _, want := range []string{"Repository ID: 8d1268c4-6a64-4b9b-95c9-d5598a150e86", "Path: /work/orc/tickets", "Failure: watch_exit", "Restart count: 2"} {
		if !strings.Contains(detail.String(), want) {
			t.Fatalf("detail repository output missing %q: %q", want, detail.String())
		}
	}
}

func TestConsoleRepositorySummaryBoundsNameAndStateAndShowsFullPath(t *testing.T) {
	const repositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	const repositoryPath = "/srv/repos/a-long-canonical-repository-path"
	const repositoryName = "a-very-long-repository-name"
	var output bytes.Buffer
	renderConsoleRepositorySummary(&output, []daemon.RepositoryStatus{{ID: repositoryID, Name: repositoryName, Path: repositoryPath, State: "healthy"}})
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 4 || lines[0] != "Repositories:" {
		t.Fatalf("repository summary lines=%q", lines)
	}
	header := strings.Fields(lines[2])
	row := strings.Fields(lines[3])
	if !reflect.DeepEqual(header, []string{"Repository", "State", "Repository", "path"}) {
		t.Fatalf("repository summary header=%q", header)
	}
	if !reflect.DeepEqual(row, []string{"a-very-long-r...", "ready", repositoryPath}) {
		t.Fatalf("repository summary row=%q", row)
	}
	degraded := renderColumnRow([]string{repositoryName, "needs attention", repositoryPath}, []int{consoleRepositoryLabelCol, consoleRepositoryStateCol, 0})
	if !strings.HasPrefix(degraded, "a-very-long-r... needs attention ") || !strings.HasSuffix(degraded, repositoryPath) {
		t.Fatalf("fixed repository/state columns truncated the state or path: %q", degraded)
	}
}

func TestConsoleStartupShowsFullStatus(t *testing.T) {
	var output bytes.Buffer
	renderConsoleBanner(&output)
	renderConsoleStatus(&output, daemon.Status{Mode: "running", Repositories: []daemon.RepositoryStatus{{ID: "repo-id", Path: "/canonical/path", State: "healthy"}}})
	text := output.String()
	const banner = "[ticket-orc] interactive console; type help for commands."
	if !strings.Contains(text, banner+"\n\nDaemon mode: running\n\nRepositories:") {
		t.Fatalf("startup banner spacing is incorrect: %q", text)
	}
	if !strings.Contains(text, "Sessions: none") {
		t.Fatalf("startup omitted empty sessions: %q", text)
	}
}

func TestConsoleVerboseStatusOrdersRepositoriesBeforeSessions(t *testing.T) {
	var output bytes.Buffer
	renderConsoleVerboseStatus(&output, daemon.Status{
		Steer: []daemon.SteerStatus{
			{RepositoryID: "repo-id", RepositoryName: "repo", Role: "coder", Actor: "reviewer", Session: "01a0dcb8-6a64-4b9b-95c9-d5598a150e86", State: "checking"},
			{RepositoryID: "unmatched-id", RepositoryName: "fallback-repo", Role: "reviewer", Actor: "external", Session: "02abcdef-6a64-4b9b-95c9-d5598a150e87", State: "checking"},
		},
		Repositories: []daemon.RepositoryStatus{{ID: "repo-id", Key: "repo", Name: "display-name", Path: "/canonical/repository/path", State: "healthy"}},
	})
	text := output.String()
	repositorySection := strings.Index(text, "Repositories:")
	sessionSection := strings.Index(text, "Sessions:")
	if repositorySection < 0 || sessionSection < 0 || repositorySection > sessionSection {
		t.Fatalf("verbose repository section should precede sessions: %q", text)
	}
	if !strings.Contains(text[repositorySection:sessionSection], "display-name") {
		t.Fatalf("repository view should include its display name: %q", text)
	}
	sessions := text[sessionSection:]
	matchedRepositoryLine := strings.Index(sessions, "Repository: display-name\n")
	matchedSessionLine := strings.Index(sessions, "Codex session: 01a0dcb8…")
	matchedRoleLine := strings.Index(sessions, "Role: coder\n")
	if matchedRepositoryLine < 0 || matchedSessionLine < matchedRepositoryLine || matchedRoleLine < matchedSessionLine {
		t.Fatalf("matched session should use repository display name and keep session ID before role: %q", text)
	}
	fallbackRepositoryLine := strings.Index(sessions, "Repository: fallback-repo\n")
	fallbackSessionLine := strings.Index(sessions, "Codex session: 02abcdef…")
	if fallbackRepositoryLine < 0 || fallbackSessionLine < 0 || fallbackRepositoryLine > fallbackSessionLine {
		t.Fatalf("unmatched session should use its repository name fallback: %q", text)
	}
}

func TestConsoleRendererPreservesPromptAndPartialInputForAsyncNotice(t *testing.T) {
	var output bytes.Buffer
	renderer := newConsoleRenderer(&output, &output)
	renderer.terminal = false
	renderer.promptLine()
	_, _ = io.WriteString(&output, "partial")
	fmt.Fprint(renderer.asyncWriter(), "worker coder: paused\n")
	text := output.String()
	if !strings.Contains(text, "ticket-orc> partial\r\nworker coder: paused\nticket-orc> ") {
		t.Fatalf("async console output=%q", text)
	}
}

func TestConsoleRendererUsesTerminalLineInsertionForPartialInput(t *testing.T) {
	var output bytes.Buffer
	renderer := newConsoleRenderer(&output, &output)
	renderer.terminal = true
	renderer.promptLine()
	_, _ = io.WriteString(&output, "partial")
	fmt.Fprint(renderer.asyncWriter(), "worker coder: paused\n")
	text := output.String()
	if !strings.Contains(text, "\x1b[s\r\x1b[1Lworker coder: paused\n\x1b[u\x1b[1B") || !strings.Contains(text, "ticket-orc> partial") {
		t.Fatalf("terminal async output=%q", text)
	}
}

func TestConsoleRendererRedrawsTrackedTerminalInputAroundAsyncNotice(t *testing.T) {
	var output bytes.Buffer
	renderer := newConsoleRenderer(&output, &output)
	renderer.terminal = true
	renderer.promptLine()
	renderer.setPartialInput("status")
	fmt.Fprint(renderer.asyncWriter(), "worker coder: paused\n")
	text := output.String()
	if !strings.Contains(text, "\r\x1b[2Kworker coder: paused\n") || !strings.Contains(text, "ticket-orc> status") {
		t.Fatalf("tracked terminal input was not redrawn: %q", text)
	}
}

func TestReadConsoleLinesReportsPartialInputWithoutExceedingBound(t *testing.T) {
	var output bytes.Buffer
	lines := make(chan string, 2)
	readConsoleLinesWithPartial(context.Background(), strings.NewReader("sta\ntus"), lines, func(value string) { output.WriteString("<" + value + ">") })
	if got := <-lines; got != "sta" {
		t.Fatalf("line=%q, want sta", got)
	}
	if !strings.Contains(output.String(), "<sta><>") || !strings.Contains(output.String(), "<tus>") {
		t.Fatalf("partial updates=%q", output.String())
	}
}

func TestInteractiveSupervisorConsoleExitRequestsShutdown(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var output, errorsOut bytes.Buffer
	var exits atomic.Int32
	if err := runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, strings.NewReader("quit\n"), &output, &errorsOut, nil, "", false, func() { exits.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if exits.Load() != 1 {
		t.Fatalf("shutdown callback count=%d output=%q errors=%q", exits.Load(), output.String(), errorsOut.String())
	}
	if !strings.Contains(output.String(), "stopping supervisor") {
		t.Fatalf("exit output=%q", output.String())
	}
	if !strings.HasPrefix(output.String(), "[ticket-orc] interactive console; type help for commands.\n") {
		t.Fatalf("console banner has unexpected spacing: %q", output.String())
	}
}

func TestInteractiveSupervisorConsoleSecondInterruptRequestsShutdown(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	interrupts := make(chan os.Signal, 2)
	var output, errorsOut bytes.Buffer
	var exits atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, reader, &output, &errorsOut, interrupts, "", false, func() { exits.Add(1) })
	}()
	time.Sleep(100 * time.Millisecond)
	_, _ = io.WriteString(writer, "not-a-command\n")
	time.Sleep(50 * time.Millisecond)
	interrupts <- os.Interrupt
	time.Sleep(20 * time.Millisecond)
	interrupts <- os.Interrupt
	_ = writer.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second interrupt did not stop console")
	}
	if exits.Load() != 1 || !strings.Contains(output.String(), "press Ctrl-C again") || !strings.Contains(output.String(), "stopping supervisor") || !strings.Contains(errorsOut.String(), "command:") {
		t.Fatalf("interrupt output=%q exits=%d errors=%q", output.String(), exits.Load(), errorsOut.String())
	}
}

func TestInteractiveSupervisorConsoleUsesItsOwnInstanceEndpoint(t *testing.T) {
	var localCalls, ambientCalls atomic.Int32
	newHandler := func(calls *atomic.Int32) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			switch r.URL.Path {
			case "/v1/status":
				_ = json.NewEncoder(w).Encode(daemon.Status{})
			case "/v1/events":
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, ": connected\n\n")
				<-r.Context().Done()
			default:
				http.NotFound(w, r)
			}
		})
	}
	localServer, localStateDir := newConsoleTestServer(t, newHandler(&localCalls))
	defer localServer.Close()
	ambientServer, ambientStateDir := newConsoleTestServer(t, newHandler(&ambientCalls))
	defer ambientServer.Close()
	data, err := os.ReadFile(daemon.EndpointPath(ambientStateDir))
	if err != nil {
		t.Fatal(err)
	}
	var ambientEndpoint daemon.Endpoint
	if err := json.Unmarshal(data, &ambientEndpoint); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TICKET_ORC_ENDPOINT", ambientEndpoint.CapabilityURL())

	var output, errorsOut bytes.Buffer
	var exits atomic.Int32
	if err := runInteractiveSupervisorConsoleWithSelection(context.Background(), localStateDir, strings.NewReader("quit\n"), &output, &errorsOut, "", func() { exits.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if localCalls.Load() == 0 || ambientCalls.Load() != 0 || exits.Load() != 1 {
		t.Fatalf("console endpoint calls local=%d ambient=%d exits=%d output=%q errors=%q", localCalls.Load(), ambientCalls.Load(), exits.Load(), output.String(), errorsOut.String())
	}
}

func TestConsoleWatchEventFiltering(t *testing.T) {
	for _, test := range []struct {
		event daemon.Event
		want  bool
	}{
		{event: daemon.Event{Type: "status.resync", Worker: "coder"}, want: false},
		{event: daemon.Event{Type: "worker.control", Worker: "coder", Phase: "pause", Code: "intent"}, want: false},
		{event: daemon.Event{Type: "worker.state", Worker: "coder", State: "paused"}, want: true},
		{event: daemon.Event{Type: "ticket.lifecycle", Role: "reviewer", Ticket: "20260926-12345", State: "review"}, want: true},
		{event: daemon.Event{Type: "steer.status", Role: "coder", Actor: "reviewer", State: "queued"}, want: true},
		{event: daemon.Event{Type: "steer.delivery", Role: "coder", Actor: "reviewer", State: "sending"}, want: true},
		{event: daemon.Event{Type: "delivery.reconciled"}, want: false},
	} {
		if got := consoleWatchEvent(test.event); got != test.want {
			t.Fatalf("event=%#v watchable=%t want=%t", test.event, got, test.want)
		}
	}
}

func TestStatusAndWatchUseSeparateIdentityFieldsAndRepositoryFallback(t *testing.T) {
	const repositoryID = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
	const sessionID = "01a0dcb8-6a64-4b9b-95c9-d5598a150e86"
	status := daemon.Status{
		Steer:        []daemon.SteerStatus{{RepositoryID: repositoryID, Role: "coder", Actor: "reviewer", Harness: "codex", Session: sessionID, State: "queued", Code: "awaiting_claim"}},
		Repositories: []daemon.RepositoryStatus{{ID: repositoryID, Path: "/work/project-root", State: "healthy"}},
	}
	var output bytes.Buffer
	renderConsoleStatus(&output, status)
	text := output.String()
	if !strings.HasPrefix(text, "\n") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("status output must start and end with a blank line: %q", text)
	}
	for _, want := range []string{"Repository", "Role", "Ticket actor", "Session", "State", "Activity", "project-root", "coder", "reviewer", "01a0dcb8…", "notified", "waiting for Ticket claim"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status missing %q: %q", want, text)
		}
	}
	if strings.Index(text, "Repositories:") > strings.Index(text, "Sessions:") || strings.Index(text, "Repository") > strings.Index(text, "Session") {
		t.Fatalf("status section or session column order is wrong: %q", text)
	}
	var sessionHeader, sessionRow []string
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 6 && fields[0] == "Repository" && fields[1] == "Session" && fields[2] == "Role" {
			sessionHeader = fields
		}
		if len(fields) >= 4 && fields[0] == "project-root" {
			sessionRow = fields
		}
	}
	if len(sessionHeader) == 0 || len(sessionRow) < 4 || sessionRow[1] != "01a0dcb8…" {
		t.Fatalf("session ID should follow repository in the table: header=%q row=%q output=%q", sessionHeader, sessionRow, text)
	}
	if strings.Contains(text, "coder / reviewer") || strings.Contains(text, "queued") || strings.Contains(text, "dynamic:") {
		t.Fatalf("status exposed combined/raw identity: %q", text)
	}

	var watched bytes.Buffer
	renderer := &consoleWatchRenderer{}
	renderer.seedRepositories(status.Repositories)
	renderer.render(&watched, daemon.Event{Type: "ticket.lifecycle", RepositoryID: repositoryID, Role: "coder", Actor: "reviewer", Session: sessionID, Ticket: "20260926-12345", State: "review"}, time.Date(2026, 9, 26, 19, 4, 12, 0, time.UTC))
	watch := watched.String()
	for _, want := range []string{"project-root", "coder", "reviewer", "01a0dcb8…", "20260926-12345", "entered review", "19:04:12"} {
		if !strings.Contains(watch, want) {
			t.Fatalf("watch output missing %q: %q", want, watch)
		}
	}
	if strings.Contains(watch, "ticket.lifecycle") || strings.Contains(watch, repositoryID) || strings.Contains(watch, sessionID) {
		t.Fatalf("watch exposed internal event or full IDs: %q", watch)
	}
	tracker := newConsoleEventTracker()
	tracker.seed(status)
	roleChange := tracker.prepare(daemon.Event{Type: "steer.status", RepositoryID: repositoryID, Role: "architect", Actor: "reviewer", Harness: "codex", Session: shortIdentity(sessionID), State: "none"})
	if got := consoleEventMessage(roleChange); got != "role changed" {
		t.Fatalf("role change event=%#v message=%q", roleChange, got)
	}
	harnessReplacement := tracker.prepare(daemon.Event{Type: "steer.status", RepositoryID: repositoryID, Role: "architect", Actor: "reviewer", Harness: "future-harness", Session: shortIdentity(sessionID), State: "none"})
	if got := consoleEventMessage(harnessReplacement); got != "session replaced" {
		t.Fatalf("harness replacement event=%#v message=%q", harnessReplacement, got)
	}
	replacement := tracker.prepare(daemon.Event{Type: "steer.status", RepositoryID: repositoryID, Role: "architect", Actor: "reviewer", Harness: "future-harness", Session: shortIdentity("01a0e118-6a64-4b9b-95c9-d5598a150e86"), State: "none"})
	if got := consoleEventMessage(replacement); got != "session replaced" {
		t.Fatalf("session replacement event=%#v message=%q", replacement, got)
	}
}

func TestWatchEnrichesManagedWorkerRoleActorAndRepositoryLocally(t *testing.T) {
	tracker := newConsoleEventTracker()
	tracker.seed(daemon.Status{
		Workers:      []daemon.WorkerStatus{{Name: "managed-reviewer", Role: "architect", TicketActor: "reviewer", State: "running", RepositoryID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86"}},
		Repositories: []daemon.RepositoryStatus{{ID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Path: "/work/ticket-orc"}},
	})
	renderer := &consoleWatchRenderer{}
	renderer.seedRepositories([]daemon.RepositoryStatus{{ID: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", Path: "/work/ticket-orc"}})
	for _, event := range []daemon.Event{
		{Type: "worker.state", Worker: "managed-reviewer", State: "paused"},
		{Type: "ticket.claim", Worker: "managed-reviewer", Ticket: "20260926-54321"},
	} {
		event = tracker.prepare(event)
		var output bytes.Buffer
		renderer.render(&output, event, time.Date(2026, 9, 26, 19, 4, 12, 0, time.UTC))
		text := output.String()
		for _, want := range []string{"ticket-orc", "architect", "reviewer"} {
			if !strings.Contains(text, want) {
				t.Fatalf("managed event %#v missing %q: %q", event, want, text)
			}
		}
		if event.Type == "ticket.claim" && !strings.Contains(text, "20260926-54321") {
			t.Fatalf("managed Ticket event lost full ID: %q", text)
		}
	}
}

func TestWatchAddsDateSeparatorOnlyWhenDayChanges(t *testing.T) {
	var output bytes.Buffer
	renderer := &consoleWatchRenderer{}
	first := time.Date(2026, 9, 26, 23, 59, 59, 0, time.Local)
	second := first.Add(2 * time.Second)
	renderer.render(&output, daemon.Event{Type: "daemon.started"}, first)
	renderer.render(&output, daemon.Event{Type: "daemon.stopping"}, second)
	text := output.String()
	if strings.Count(text, "--- "+second.Format("2006-01-02")+" ---") != 1 || strings.Contains(text, "--- "+first.Format("2006-01-02")+" ---") {
		t.Fatalf("watch date separators = %q", text)
	}
}

func TestConsoleWatchUsesStatusRepositoryNamesAndTruncation(t *testing.T) {
	firstID := "11111111-1111-4111-8111-111111111111"
	secondID := "22222222-2222-4222-8222-222222222222"
	renderer := &consoleWatchRenderer{}
	renderer.seedRepositories([]daemon.RepositoryStatus{
		{ID: firstID, Name: "Ticket Alpha", Path: "/work/ticket/tickets"},
		{ID: secondID, Name: "Ticket Beta With A Long Display Name", Path: "/work/ticket-orc/tickets"},
	})
	var output bytes.Buffer
	for _, repositoryID := range []string{firstID, secondID} {
		renderer.render(&output, daemon.Event{Type: "ticket.repository_changed", RepositoryID: repositoryID}, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	}
	text := output.String()
	if !strings.Contains(text, "Ticket Alpha") || !strings.Contains(text, consoleEllipsize("Ticket Beta With A Long Display Name", consoleRepositoryLabelCol)) {
		t.Fatalf("watch repository labels do not match status names/truncation: %q", text)
	}
	if strings.Contains(text, "tickets") {
		t.Fatalf("watch fell back to colliding path basenames despite status names: %q", text)
	}
}

func TestConsoleWatchRefreshesUnknownRepositoryFromStatus(t *testing.T) {
	const repositoryID = "33333333-3333-4333-8333-333333333333"
	var statusCalls, eventCalls atomic.Int32
	watchStarted := make(chan struct{})
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/status"):
			status := daemon.Status{Workers: []daemon.WorkerStatus{}}
			if statusCalls.Add(1) > 1 {
				status.Repositories = []daemon.RepositoryStatus{{ID: repositoryID, Name: "New Project", Path: "/work/new-project/tickets", State: "healthy"}}
			}
			_ = json.NewEncoder(w).Encode(status)
		case strings.HasSuffix(r.URL.Path, "/v1/events"):
			eventCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id: 0\nevent: stream.sync\ndata: {\"seq\":0,\"type\":\"stream.sync\"}\n\n")
			w.(http.Flusher).Flush()
			<-watchStarted
			_, _ = io.WriteString(w, "id: 1\ndata: {\"seq\":1,\"type\":\"ticket.repository_changed\",\"repository_id\":\""+repositoryID+"\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inputReader, inputWriter := io.Pipe()
	var errorsOut bytes.Buffer
	watchOutput := &consoleWatchStartWriter{started: watchStarted}
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{localDir: stateDir}, inputReader, watchOutput, &errorsOut, nil, "", false, nil)
	}()
	go func() { _, _ = io.WriteString(inputWriter, "watch\n") }()
	deadline := time.After(time.Second)
	for statusCalls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("watch did not refresh status for unknown repository (status calls=%d, event streams=%d)", statusCalls.Load(), eventCalls.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	_ = inputWriter.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("console did not stop after cancellation")
	}
	if got := watchOutput.String(); !strings.Contains(got, "New Project") || strings.Contains(got, "tickets") {
		t.Fatalf("new runtime repository label=%q", got)
	}
}

func TestConsoleWatchWritesDateMarkerAndTimeRowAtomically(t *testing.T) {
	var output countingConsoleWriter
	renderer := &consoleWatchRenderer{}
	first := time.Date(2026, 9, 27, 23, 59, 59, 0, time.Local)
	second := first.Add(2 * time.Second)
	renderer.render(&output, daemon.Event{Type: "daemon.started"}, first)
	if output.writes != 1 {
		t.Fatalf("ordinary watch render writes=%d, want one", output.writes)
	}
	output.buffer.Reset()
	output.writes = 0
	renderer.render(&output, daemon.Event{Type: "daemon.stopping"}, second)
	if output.writes != 1 {
		t.Fatalf("day transition split marker and event into %d writes", output.writes)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 2 || lines[0] != "--- "+second.Format("2006-01-02")+" ---" {
		t.Fatalf("day transition output=%q, want marker and one row", output.String())
	}
	if len(lines[1]) < len(consoleTimestampLayout) {
		t.Fatalf("watch event row is missing its time cell: %q", lines[1])
	}
	if got := strings.TrimSpace(lines[1][:len(consoleTimestampLayout)]); got != second.Format(consoleTimestampLayout) {
		t.Fatalf("watch time cell=%q, want HH:MM:SS", got)
	}
}

type countingConsoleWriter struct {
	buffer bytes.Buffer
	writes int
}

func (writer *countingConsoleWriter) Write(data []byte) (int, error) {
	writer.writes++
	return writer.buffer.Write(data)
}

func (writer *countingConsoleWriter) String() string { return writer.buffer.String() }

type consoleWatchStartWriter struct {
	bytes.Buffer
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	stopOnce sync.Once
}

func (writer *consoleWatchStartWriter) Write(data []byte) (int, error) {
	text := string(data)
	if strings.Contains(text, "Watching activity.") {
		writer.once.Do(func() { close(writer.started) })
	}
	if writer.stopped != nil && strings.Contains(text, "watch stopped") {
		writer.stopOnce.Do(func() { close(writer.stopped) })
	}
	return writer.Buffer.Write(data)
}

func TestConsoleWatchReturnsToPromptOnInterrupt(t *testing.T) {
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	reader, writer := io.Pipe()
	interrupts := make(chan os.Signal, 1)
	var output, errorsOut bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, reader, &output, &errorsOut, interrupts, "", false, nil)
	}()
	_, _ = io.WriteString(writer, "watch\n")
	time.Sleep(100 * time.Millisecond)
	interrupts <- os.Interrupt
	time.Sleep(100 * time.Millisecond)
	_, _ = io.WriteString(writer, "quit\n")
	_ = writer.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watch console did not return")
	}
	text := output.String()
	if !strings.Contains(text, "Watching activity") || !strings.Contains(text, "watch stopped") || !strings.Contains(text, consolePrompt) || !strings.Contains(text, "Role") || !strings.Contains(text, "Actor") || strings.Contains(text, "Responsibility") {
		t.Fatalf("watch console output=%q errors=%q", text, errorsOut.String())
	}
	marker := "Watching activity. Press Ctrl-C to return."
	index := strings.Index(text, marker)
	if index < 1 || text[index-1:index] != "\n" || !strings.HasPrefix(text[index+len(marker):], "\n\n") {
		t.Fatalf("watch banner lacks surrounding blank lines: %q", text)
	}
	headerIndex := strings.Index(text, consoleWatchHeader())
	if headerIndex < 0 || !strings.HasPrefix(text[headerIndex+len(consoleWatchHeader()):], "\nwatch stopped") {
		t.Fatalf("watch header should be followed directly by activity, then stop notice: %q", text)
	}
}

func TestConsoleWatchAppendsActivityAsNDJSON(t *testing.T) {
	const ticketID = "20260926-52505"
	eventReady := make(chan struct{})
	sendEvent := make(chan struct{})
	var eventReadyOnce sync.Once
	eventHandlerDone := make(chan struct{})
	var eventHandlerDoneOnce sync.Once
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{})
		case "/v1/events":
			defer eventHandlerDoneOnce.Do(func() { close(eventHandlerDone) })
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			w.(http.Flusher).Flush()
			eventReadyOnce.Do(func() { close(eventReady) })
			select {
			case <-sendEvent:
				_, _ = io.WriteString(w, "id: 1\ndata: {\"seq\":1,\"type\":\"ticket.repository_changed\",\"ticket\":\""+ticketID+"\",\"repository_id\":\""+joinTestRepositoryID+"\",\"repository_key\":\"project\",\"code\":\"submitted\"}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			case <-r.Context().Done():
			}
		default:
			http.NotFound(w, r)
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	interrupts := make(chan os.Signal, 1)
	var errorsOut bytes.Buffer
	watchStarted := make(chan struct{})
	watchStopped := make(chan struct{})
	watchOutput := &consoleWatchStartWriter{started: watchStarted, stopped: watchStopped}
	done := make(chan error, 1)
	consoleExited := make(chan struct{})
	go func() {
		done <- runConsoleLoopWithSelectionPolicy(ctx, stateDir, reader, watchOutput, &errorsOut, interrupts, "", false, nil)
		close(consoleExited)
	}()
	t.Cleanup(func() {
		cancel()
		_ = writer.Close()
		_ = reader.Close()
		select {
		case <-consoleExited:
		case <-time.After(time.Second):
			t.Errorf("console goroutine did not stop during cleanup")
		}
		select {
		case <-eventReady:
			select {
			case <-eventHandlerDone:
			case <-time.After(time.Second):
				t.Errorf("event request did not stop during cleanup")
			}
		default:
		}
		server.CloseClientConnections()
		serverClosed := make(chan struct{})
		go func() {
			server.Close()
			close(serverClosed)
		}()
		select {
		case <-serverClosed:
		case <-time.After(time.Second):
			t.Errorf("test HTTP server did not close promptly")
		}
	})
	select {
	case <-eventReady:
	case <-time.After(time.Second):
		t.Fatal("console event stream did not connect")
	}
	if _, err := io.WriteString(writer, "watch\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-watchStarted:
	case <-time.After(time.Second):
		t.Fatal("console did not enter watch mode")
	}
	close(sendEvent)

	activityPath := filepath.Join(stateDir, "logs", "activity.jsonl")
	var data []byte
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(activityPath)
		if len(data) != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	var event daemon.Event
	if err := json.Unmarshal(bytes.TrimSpace(data), &event); err != nil {
		t.Fatalf("activity log is not a JSON event: data=%q err=%v", data, err)
	}
	if event.Type != "ticket.repository_changed" || event.Seq != 1 || event.Ticket != ticketID || event.RepositoryID != joinTestRepositoryID || event.RepositoryKey != "project" {
		t.Fatalf("logged activity=%#v", event)
	}
	interrupts <- os.Interrupt
	select {
	case <-watchStopped:
	case <-time.After(time.Second):
		t.Fatal("console did not stop watch mode after interrupt")
	}
	_, _ = io.WriteString(writer, "quit\n")
	_ = writer.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("console did not exit after logging activity")
	}
}

func TestConsoleEventsResynchronizeAfterReconnectAndGap(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(": connected\n\n"))
			if eventCalls.Add(1) == 1 {
				_, _ = w.Write([]byte("id: 1\ndata: {\"type\":\"worker.state\",\"seq\":1,\"worker\":\"coder\",\"state\":\"running\"}\n\n"))
				return
			}
			_, _ = w.Write([]byte("id: 3\ndata: {\"type\":\"ticket.claim\",\"seq\":3,\"worker\":\"coder\",\"ticket\":\"20260926-12345\"}\n\n"))
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{
				Workers:      []daemon.WorkerStatus{{Name: "coder", State: "paused"}},
				Repositories: []daemon.RepositoryStatus{{Key: "orc", State: "healthy"}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events := make(chan daemon.Event, 16)
	errorsOut := make(chan error, 4)
	go readConsoleEvents(ctx, client, events, errorsOut)
	var sawResync, sawRepositoryResync, sawClaim bool
	for !sawResync || !sawRepositoryResync || !sawClaim {
		select {
		case event := <-events:
			if event.Type == "status.resync" {
				if event.Worker != "" {
					sawResync = true
				}
				if event.RepositoryKey == "orc" {
					sawRepositoryResync = true
				}
			}
			if event.Type == "ticket.claim" {
				sawClaim = true
			}
		case eventErr := <-errorsOut:
			if eventErr != nil {
				t.Fatalf("event stream error: %v", eventErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for resync=%t repository_resync=%t claim=%t", sawResync, sawRepositoryResync, sawClaim)
		}
	}
}

func TestConsoleEventsReadsMultipleEventsFromOneSubscription(t *testing.T) {
	var eventCalls atomic.Int32
	connected := make(chan struct{})
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		call := eventCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "id: 40\nevent: stream.sync\ndata: {\"seq\":40,\"type\":\"stream.sync\"}\n\n")
		for sequence, eventType := range []string{"worker.state", "ticket.claim", "repository.observer"} {
			frame := fmt.Sprintf("id: %d\ndata: {\"seq\":%d,\"type\":%q}\n\n", sequence+41, sequence+41, eventType)
			_, _ = io.WriteString(w, frame)
		}
		w.(http.Flusher).Flush()
		if call == 1 {
			close(connected)
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("event stream did not connect")
	}
	want := []string{"worker.state", "ticket.claim", "repository.observer"}
	for i, eventType := range want {
		event := nextConsoleTestEvent(t, events)
		if event.Type != eventType || event.Seq != uint64(i+41) {
			t.Fatalf("event[%d]=%#v, want type=%q seq=%d", i, event, eventType, i+41)
		}
	}
	if got := eventCalls.Load(); got != 1 {
		t.Fatalf("event subscriptions=%d, want one for all events", got)
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected extra event, possibly synthetic stream.sync: %#v", event)
	default:
	}
	select {
	case err := <-errorsOut:
		t.Fatalf("event stream error: %v", err)
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
}

func TestConsoleEventGapResyncKeepsCurrentSubscription(t *testing.T) {
	var eventCalls, statusCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			statusCalls.Add(1)
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "paused"}}})
		case "/v1/events":
			eventCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id: 5\nevent: stream.sync\ndata: {\"seq\":5,\"type\":\"stream.sync\"}\n\nid: 6\ndata: {\"seq\":6,\"type\":\"worker.state\",\"worker\":\"coder\"}\n\nid: 8\ndata: {\"seq\":8,\"type\":\"ticket.claim\",\"ticket\":\"20260928-12345\"}\n\nid: 9\ndata: {\"seq\":9,\"type\":\"worker.state\",\"worker\":\"coder\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	want := []struct {
		typeName string
		seq      uint64
	}{
		{typeName: "worker.state", seq: 6},
		{typeName: "status.resync"},
		{typeName: "ticket.claim", seq: 8},
		{typeName: "worker.state", seq: 9},
	}
	for i, expected := range want {
		event := nextConsoleTestEvent(t, events)
		if event.Type != expected.typeName || event.Seq != expected.seq {
			t.Fatalf("event[%d]=%#v, want type=%q seq=%d", i, event, expected.typeName, expected.seq)
		}
		if i == 1 && (event.Worker != "coder" || event.State != "paused") {
			t.Fatalf("resync event=%#v, want authoritative paused worker", event)
		}
	}
	if got := eventCalls.Load(); got != 1 {
		t.Fatalf("event subscriptions=%d, want current stream retained after gap", got)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("status reconciliations=%d, want one", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
}

func TestConsoleEventEOFReconnectsAndClosesPreviousStream(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", State: "running"}}})
		case "/v1/events":
			call := eventCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			if call == 1 {
				_, _ = io.WriteString(w, "id: 100\nevent: stream.sync\ndata: {\"seq\":100,\"type\":\"stream.sync\"}\n\nid: 101\ndata: {\"seq\":101,\"type\":\"worker.state\",\"worker\":\"first\"}\n\n")
				w.(http.Flusher).Flush()
				return
			}
			_, _ = io.WriteString(w, "id: 200\nevent: stream.sync\ndata: {\"seq\":200,\"type\":\"stream.sync\"}\n\nid: 201\ndata: {\"seq\":201,\"type\":\"worker.state\",\"worker\":\"second\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	trackedBodies := make(chan *consoleTrackedEventBody, 4)
	httpClient := &http.Client{Transport: consoleEventTrackingTransport{base: http.DefaultTransport, bodies: trackedBodies}}
	client, err := daemonclient.NewWithHTTPClient(stateDir, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 8)
	errorsOut := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	firstBody := nextTrackedConsoleBody(t, trackedBodies)
	firstEvent := nextConsoleTestEvent(t, events)
	if firstEvent.Type != "worker.state" || firstEvent.Seq != 101 || firstEvent.Worker != "first" {
		t.Fatalf("first event=%#v", firstEvent)
	}
	select {
	case <-firstBody.closed:
	case <-time.After(time.Second):
		t.Fatal("previous event response body was not closed on EOF")
	}
	secondBody := nextTrackedConsoleBody(t, trackedBodies)
	var sawResync, sawSecond bool
	for !sawResync || !sawSecond {
		event := nextConsoleTestEvent(t, events)
		if event.Type == "status.resync" && event.Worker == "coder" {
			sawResync = true
		}
		if event.Type == "worker.state" && event.Seq == 201 && event.Worker == "second" {
			sawSecond = true
		}
	}
	if got := eventCalls.Load(); got != 2 {
		t.Fatalf("event subscriptions=%d, want reconnect with new sync baseline", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader did not stop after cancellation")
	}
	select {
	case <-secondBody.closed:
	case <-time.After(time.Second):
		t.Fatal("current event response body was not closed on cancellation")
	}
}

func TestConsoleEventReconnectFailuresRemainBounded(t *testing.T) {
	var eventCalls atomic.Int32
	server, stateDir := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		eventCalls.Add(1)
		http.Error(w, `{"error":{"code":"temporarily_unavailable","message":"unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := daemonclient.New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan daemon.Event, 1)
	errorsOut := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		readConsoleEvents(ctx, client, events, errorsOut)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event reader exceeded its bounded reconnect attempts")
	}
	if got := eventCalls.Load(); got != 3 {
		t.Fatalf("event subscription attempts=%d, want 3", got)
	}
	select {
	case err := <-errorsOut:
		if err == nil {
			t.Fatal("terminal reconnect error is nil")
		}
	default:
		t.Fatal("bounded reconnect failure was not reported")
	}
}

func nextConsoleTestEvent(t *testing.T, events <-chan daemon.Event) daemon.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for console event")
		return daemon.Event{}
	}
}

type consoleTrackedEventBody struct {
	io.ReadCloser
	closed chan struct{}
	once   sync.Once
}

func (body *consoleTrackedEventBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return body.ReadCloser.Close()
}

type consoleEventTrackingTransport struct {
	base   http.RoundTripper
	bodies chan<- *consoleTrackedEventBody
}

func (transport consoleEventTrackingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil || !strings.HasSuffix(request.URL.Path, "/v1/events") {
		return response, err
	}
	body := &consoleTrackedEventBody{ReadCloser: response.Body, closed: make(chan struct{})}
	response.Body = body
	transport.bodies <- body
	return response, nil
}

func nextTrackedConsoleBody(t *testing.T, bodies <-chan *consoleTrackedEventBody) *consoleTrackedEventBody {
	t.Helper()
	select {
	case body := <-bodies:
		return body
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event response body")
		return nil
	}
}

func newConsoleTestServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	token, err := daemon.GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := "/" + token
		if !strings.HasPrefix(r.URL.Path, prefix+"/") {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		handler.ServeHTTP(w, r)
	}))
	return server, writeConsoleEndpoint(t, server.URL, token)
}

func writeConsoleEndpoint(t *testing.T, serverURL, token string) string {
	t.Helper()
	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint := daemon.Endpoint{Version: 1, Protocol: 1, PID: os.Getpid(), URL: serverURL, EndpointKey: token}
	data, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "endpoint.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return stateDir
}
