package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

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
	repositoryRendered := make(chan struct{})
	watchOutput := &consoleWatchStartWriter{started: watchStarted, matched: repositoryRendered, match: "New Project"}
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{localDir: stateDir}, inputReader, watchOutput, &errorsOut, nil, "", false, nil)
	}()
	go func() { _, _ = io.WriteString(inputWriter, "watch\n") }()
	select {
	case <-repositoryRendered:
		// The refresh response has been consumed and rendered by the console.
	case <-time.After(2 * time.Second):
		t.Fatalf("watch did not render refreshed repository (status calls=%d, event streams=%d)", statusCalls.Load(), eventCalls.Load())
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
	var eventRow string
	for _, line := range strings.Split(watchOutput.String(), "\n") {
		if strings.Contains(line, "ticket modified") {
			eventRow = line
			break
		}
	}
	if eventRow == "" {
		t.Fatalf("watch output did not contain the repository event row: %q", watchOutput.String())
	}
	if !strings.Contains(eventRow, consoleEllipsize("New Project", consoleWatchRepositoryCol)) ||
		strings.Contains(eventRow, "new-project") || strings.Contains(eventRow, consoleEllipsize(repositoryID, consoleWatchRepositoryCol)) {
		t.Fatalf("event row did not use the refreshed repository name: %q", eventRow)
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
	var errorsOut bytes.Buffer
	watchStarted := make(chan struct{})
	watchStopped := make(chan struct{})
	prompts := make(chan struct{}, 3)
	watchOutput := &consoleWatchStartWriter{started: watchStarted, stopped: watchStopped, prompted: prompts}
	done := make(chan error, 1)
	go func() {
		done <- runConsoleLoopWithSelectionPolicy(context.Background(), stateDir, reader, watchOutput, &errorsOut, interrupts, "", false, nil)
	}()
	waitConsoleMilestone(t, prompts, "initial prompt")
	_, _ = io.WriteString(writer, "watch\n")
	waitConsoleMilestone(t, watchStarted, "watch mode")
	interrupts <- os.Interrupt
	waitConsoleMilestone(t, watchStopped, "watch stop notice")
	waitConsoleMilestone(t, prompts, "prompt after watch")
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
	text := watchOutput.String()
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
	eventRendered := make(chan struct{})
	prompts := make(chan struct{}, 3)
	watchOutput := &consoleWatchStartWriter{started: watchStarted, stopped: watchStopped, prompted: prompts, matched: eventRendered, match: "modified"}
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
	waitConsoleMilestone(t, prompts, "initial prompt")
	if _, err := io.WriteString(writer, "watch\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-watchStarted:
	case <-time.After(time.Second):
		t.Fatal("console did not enter watch mode")
	}
	close(sendEvent)
	waitConsoleMilestone(t, eventRendered, "repository watch event row")

	activityPath := filepath.Join(stateDir, "logs", "activity.jsonl")
	data, err := os.ReadFile(activityPath)
	if err != nil {
		t.Fatalf("read rendered activity log: %v", err)
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
	waitConsoleMilestone(t, prompts, "prompt after watch stop")
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
