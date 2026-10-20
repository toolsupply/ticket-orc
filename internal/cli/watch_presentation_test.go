package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestBareWatchMeansAllIncludingVerbose(t *testing.T) {
	for _, args := range [][]string{nil, {"--verbose"}} {
		bare, err := parseWatchOptions(args)
		if err != nil {
			t.Fatal(err)
		}
		all, err := parseWatchOptions(append([]string{"all"}, args...))
		if err != nil || bare != all || bare.selector != "all" {
			t.Fatalf("bare=%#v all=%#v err=%v", bare, all, err)
		}
		console, err := parseConsoleCommand("watch " + strings.Join(args, " "))
		if err != nil || console.watch != all {
			t.Fatalf("console=%#v want=%#v err=%v", console, all, err)
		}
	}
}

func TestUnifiedWatchEventProse(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 3, 15, 0, time.Local)
	renderer := &consoleWatchRenderer{unified: true}
	renderer.seedRepositories([]daemon.RepositoryStatus{{ID: joinTestRepositoryID, Name: "ticket-orc-repo"}})
	for _, test := range []struct {
		event daemon.Event
		want  string
	}{
		{daemon.Event{Type: "steer.status", State: "none", Code: "idle", Phase: "session_joined"}, "joined, no work available"},
		{daemon.Event{Type: "steer.status", State: "none", Code: "ready", Phase: "session_replaced"}, "replaced, ready work available"},
		{daemon.Event{Type: "steer.status", State: "consumed", Code: "busy", Phase: "role_changed"}, "changed role, claimed 20261017-99478"},
		{daemon.Event{Type: "steer.status", State: "left"}, "left"},
		{daemon.Event{Type: "steer.status", State: "none", Code: "busy"}, "claimed 20261017-99478"},
		{daemon.Event{Type: "steer.status", State: "none", Code: "idle", Phase: "ticket_observation_recovered"}, "Ticket observation recovered; no work"},
		{daemon.Event{Type: "steer.status", State: "degraded", Code: "ticket_observation_failed", Failure: &daemon.WorkerFailure{Classification: "ticket_transport_failed", Operation: "work", TransportCategory: "start", ExitCode: 3}}, "Ticket work failed: Ticket process or transport is unavailable; transport=start; exit_code=3"},
		{daemon.Event{Type: "steer.delivery", State: "sending"}, "sending notification for 20261017-99478"},
		{daemon.Event{Type: "steer.delivery", State: "queued"}, "queued notification for 20261017-99478"},
		{daemon.Event{Type: "ticket.claim"}, "claimed 20261017-99478"},
		{daemon.Event{Type: "ticket.lifecycle", State: "review"}, "entered review 20261017-99478"},
		{daemon.Event{Type: "ticket.lifecycle", State: "signoff"}, "entered signoff 20261017-99478"},
		{daemon.Event{Type: "ticket.lifecycle", State: "closed"}, "closed 20261017-99478"},
		{daemon.Event{Type: "ticket.lifecycle", State: "open"}, "reopened 20261017-99478"},
		{daemon.Event{Type: "ticket.repository_changed", State: "review"}, "ticket 20261017-99478 modified (review)"},
		{daemon.Event{Type: "ticket.repository_observer", State: "degraded", Code: "watch_exit"}, "repository watch needs attention"},
		{daemon.Event{Type: "ticket.repository_observer", State: "healthy", Code: "observer_recovered"}, "repository watch recovered"},
		{daemon.Event{Type: "ticket.repository_observer", State: "healthy", Code: "observer_restarted"}, "repository watch restarted"},
		{daemon.Event{Type: "worker.state", State: "paused"}, "paused"},
		{daemon.Event{Type: "worker.failure", Failure: &daemon.WorkerFailure{Classification: "ticket_protocol_failed", Phase: "observation", Remediation: "inspect Ticket JSON/protocol compatibility before retrying"}}, "ticket_protocol_failed during observation remediation=inspect Ticket JSON/protocol compatibility before retrying"},
		{daemon.Event{Type: "doctor.completed"}, "recovery completed"},
		{daemon.Event{Type: "doctor.failed"}, "recovery failed"},
		{daemon.Event{Type: "config.reloaded"}, "configuration reloaded"},
		{daemon.Event{Type: "config.reload_failed"}, "configuration reload failed"},
	} {
		t.Run(test.event.Type+"/"+test.event.State+"/"+test.event.Phase, func(t *testing.T) {
			event := test.event
			event.RepositoryID, event.RepositoryName = joinTestRepositoryID, "stale name"
			event.Role, event.Actor, event.Session, event.Worker = "reviewer", "orc-reviewer", "01a148f7-abcd", "stable-worker"
			event.Ticket = "20261017-99478"
			var out bytes.Buffer
			if err := renderer.render(&out, event, at); err != nil {
				t.Fatal(err)
			}
			got := out.String()
			for _, want := range []string{"12:03:15 [ticket-orc-repo:orc] reviewer worker stable-worker session 01a148f7… (actor orc-reviewer) ", test.want, event.Ticket} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q: %q", want, got)
				}
			}
			for _, unwanted := range []string{consoleWatchHeader(), "—", "stale name", "  ", "ticket.lifecycle"} {
				if strings.Contains(got, unwanted) {
					t.Errorf("unexpected %q: %q", unwanted, got)
				}
			}
		})
	}
	var out bytes.Buffer
	if err := renderer.render(&out, daemon.Event{Type: "steer.status", RepositoryID: joinTestRepositoryID, Role: "reviewer", Session: "01a148f7-abcd", State: "none", Code: "idle", Phase: "session_joined"}, at); err != nil {
		t.Fatal(err)
	}
	if want := "12:03:15 [ticket-orc-repo:orc] reviewer session 01a148f7… joined, no work available\n"; out.String() != want {
		t.Fatalf("join=%q want=%q", out.String(), want)
	}
}

func TestUnifiedWatchKeepsRecoveryDistinctFromJoin(t *testing.T) {
	for _, sawFailure := range []bool{false, true} {
		tracker := newConsoleEventTracker()
		event := daemon.Event{Type: "steer.status", RepositoryID: joinTestRepositoryID, Actor: "alice", Role: "reviewer", Session: "01a148f7…", State: "none", Code: "idle"}
		if sawFailure {
			failed := event
			failed.State, failed.Code = "degraded", "ticket_observation_failed"
			failed.Failure = &daemon.WorkerFailure{Classification: "ticket_observation_timeout", Operation: "work"}
			tracker.prepare(failed)
		}
		event.Phase = "ticket_observation_recovered"
		prepared := tracker.prepare(event)
		if prepared.Phase != event.Phase || strings.Contains(unifiedEventMessage(prepared), "joined") || !strings.Contains(unifiedEventMessage(prepared), "observation recovered") {
			t.Fatalf("recovery became join: %#v", prepared)
		}
		// The recovered session is retained for subsequent ordinary observations.
		event.Phase = ""
		if got := tracker.prepare(event); got.Phase != "" {
			t.Fatalf("ordinary observation became join after recovery: %#v", got)
		}
	}
}

func TestUnifiedWatchRepositoryActorLabelsAndSanitization(t *testing.T) {
	renderer := &consoleWatchRenderer{unified: true}
	longName := "a repository name longer than the legacy table column"
	renderer.seedRepositories([]daemon.RepositoryStatus{
		{ID: joinTestRepositoryID, Name: longName, Path: "/ignored/path"},
		{ID: joinOtherRepositoryID, Path: "/work/second-project"},
	})
	for _, test := range []struct {
		worker daemon.WorkerStatus
		want   string
	}{
		{daemon.WorkerStatus{Name: "stable-name", TicketActor: "alice", RepositoryID: joinTestRepositoryID, RepositoryName: "stale"}, longName + ":alice"},
		{daemon.WorkerStatus{Name: "stable-name", RepositoryID: joinOtherRepositoryID}, "second-project:stable-name"},
		{daemon.WorkerStatus{Name: "fallback", RepositoryID: "33333333-abcd"}, "33333333…:fallback"},
		{daemon.WorkerStatus{Name: "fallback"}, "unknown repository:fallback"},
		{daemon.WorkerStatus{Name: "fallback", TicketActor: "\x1b\a", RepositoryName: "\x1b"}, "unknown repository:fallback"},
		{daemon.WorkerStatus{TicketActor: "alice\x1b\n:[]\u009b", RepositoryName: "repo\r\t\x00[]:"}, "repo  ()∶:alice ∶()"},
	} {
		if got := renderer.workerLabel(test.worker); got != test.want {
			t.Errorf("label=%q want=%q", got, test.want)
		}
	}
	if label := watchLabel(strings.Repeat("界", 200)); len([]rune(label)) != 129 || !strings.HasSuffix(label, "…") {
		t.Fatalf("unbounded or invalid Unicode label %q", label)
	}
	var out bytes.Buffer
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.Local)
	for _, event := range []daemon.Event{{Type: "daemon.started"}, {Type: "daemon.stopping"}, {Type: "config.reloaded"}} {
		if err := renderer.render(&out, event, at); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Count(out.String(), "12:00:00 [orc] ") != 3 || strings.Contains(out.String(), "unknown") {
		t.Fatalf("global events invented repository: %q", out.String())
	}
	out.Reset()
	if err := renderer.render(&out, daemon.Event{Type: "ticket.claim", RepositoryID: "33333333-abcd", Ticket: "20261017-99478"}, at); err != nil {
		t.Fatal(err)
	}
	if out.String() != "12:00:00 [33333333…:orc] claimed 20261017-99478\n" {
		t.Fatalf("missing metadata fallback: %q", out.String())
	}
}

func TestUnifiedWorkerLabelsKeepRepositoryActorAndStableMatching(t *testing.T) {
	root := t.TempDir()
	status := daemon.Status{
		Repositories: []daemon.RepositoryStatus{{ID: joinTestRepositoryID, Name: "first-repo"}, {ID: joinOtherRepositoryID, Name: "second-repo"}},
		Workers: []daemon.WorkerStatus{
			{Name: "api", TicketActor: "alice", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID},
			{Name: "api.coder", TicketActor: "bob", Role: "coder", Harness: "codex", RepositoryID: joinOtherRepositoryID},
			{Name: "fallback", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID},
		},
	}
	var out bytes.Buffer
	watch, err := newWorkerLogWatch(root, status, watchOptions{selector: "all"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	at := time.Date(2026, 10, 17, 12, 3, 16, 0, time.Local)
	for i, worker := range status.Workers {
		// All repositories deliberately have the same ticket ID.
		log := openWatchTestLog(t, root, worker.Name, at.Add(time.Duration(i)*time.Second))
		if _, err := io.WriteString(log, `{"type":"item.completed","item":{"type":"agent_message","text":"Implemented the change.\nSecond line."}}`+"\ninvalid\n"); err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := watch.poll(at); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(at); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"first-repo:alice", "second-repo:bob", "first-repo:fallback"} {
		for _, text := range []string{"Implemented the change.", "Second line.", "[Codex log event skipped:"} {
			if !strings.Contains(out.String(), " ["+label+"] "+text) {
				t.Errorf("missing %s %s: %q", label, text, out.String())
			}
		}
	}
	for _, row := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		if len(row) < 8 {
			t.Fatalf("missing observation time: %q", row)
		}
		if _, err := time.Parse("15:04:05", row[:8]); err != nil {
			t.Fatalf("invalid observation time: %q", row)
		}
	}
	if strings.Contains(out.String(), "[api]") || strings.Contains(out.String(), "[api.coder]") {
		t.Fatal(out.String())
	}
	// Presentation refresh may change an actor/name, never the follower identity.
	status.Repositories[0].Name = "renamed repo"
	status.Workers[0].TicketActor = "new-alice"
	watch.refresh(status)
	if got := watch.renderer.workerLabel(watch.workers["api"]); got != "renamed repo:new-alice" {
		t.Fatal(got)
	}
	status.Workers[0].RepositoryID = joinOtherRepositoryID
	watch.refresh(status)
	if got := watch.workers["api"].RepositoryID; got != joinTestRepositoryID {
		t.Fatalf("refresh rebound stable worker: %s", got)
	}
}

func TestUnifiedWatchDiagnosticsAndWrites(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 3, 16, 0, time.Local)
	var out bytes.Buffer
	if err := writeWatchLines(&out, at, "repo:alice", "first\nsecond\r\x1b\n"); err != nil {
		t.Fatal(err)
	}
	if out.String() != "12:03:16 [repo:alice] first\n12:03:16 [repo:alice] second \n" {
		t.Fatal(out.String())
	}
	for _, unified := range []bool{false, true} {
		err := (&consoleWatchRenderer{unified: unified}).render(shortWatchWriter{}, daemon.Event{Type: "daemon.started"}, at)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("unified=%t short write=%v", unified, err)
		}
	}
	out.Reset()
	renderer := &consoleWatchRenderer{unified: true}
	if err := renderConsoleWatchActivity(&out, renderer, "", daemon.Event{Type: "ticket.claim", RepositoryID: joinTestRepositoryID, Ticket: "20261017-99478"}, at); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "12:03:16 [11111111…:orc] ") != 2 {
		t.Fatal(out.String())
	}
	for _, code := range []string{"invalid_event", "event_too_large"} {
		out.Reset()
		err := renderWorkerObservation(&out, watchOptions{}, executionlog.Observation{Harness: "codex", ObservedAt: at, Record: harness.LogRecord{Kind: "diagnostic", Code: code, Text: "bad\nrecord\x1b"}}, "repo:alice")
		if err != nil || out.String() != "12:03:16 [repo:alice] [Codex log event skipped: bad record]\n" {
			t.Fatalf("%q, %v", out.String(), err)
		}
	}
}
