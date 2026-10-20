package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

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

func TestConsoleWorkersSteerOnlyRendersSessionTable(t *testing.T) {
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

func TestSignoffWaitingStatusHasOperatorMeaning(t *testing.T) {
	session := daemon.SteerStatus{RepositoryName: "repo", RepositoryID: localRepoID, Role: "architect-api", Actor: "reviewer", Session: steerTestThread, State: "queued", Code: "awaiting_signoff_outcome"}
	var table, detail bytes.Buffer
	renderConsoleSteerStatus(&table, []daemon.SteerStatus{session}, nil, false)
	renderConsoleSteerStatus(&detail, []daemon.SteerStatus{session}, nil, true)
	if !strings.Contains(table.String(), "awaiting signoff outcome") {
		t.Fatalf("console table omitted signoff outcome: %s", table.String())
	}
	if !strings.Contains(detail.String(), "waiting for a signoff outcome") || strings.Contains(detail.String(), "delivery issue") {
		t.Fatalf("console detail misreported signoff wait: %s", detail.String())
	}
	message := consoleEventMessage(daemon.Event{Type: "steer.status", State: "queued", Code: "awaiting_signoff_outcome"})
	if message != "waiting for a signoff outcome" {
		t.Fatalf("console watch message=%q", message)
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
