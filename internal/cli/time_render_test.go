package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func setTestLocalLocation(t *testing.T, location *time.Location) {
	t.Helper()
	previous := time.Local
	time.Local = location
	t.Cleanup(func() { time.Local = previous })
}

func TestDaemonCompactStatusRendersLocalTimeWithoutChangingJSON(t *testing.T) {
	setTestLocalLocation(t, time.FixedZone("test-local", -7*60*60))
	started := time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC)
	status := daemon.Status{StartedAt: started}
	var output bytes.Buffer
	renderDaemonCompactStatus(&output, status)
	if !strings.Contains(output.String(), "started=2026-09-27T18:30:00-07:00") {
		t.Fatalf("compact status did not render local date and offset: %q", output.String())
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"started_at":"2026-09-28T01:30:00Z"`) {
		t.Fatalf("JSON timestamp changed from UTC: %s", encoded)
	}
}

func TestConsoleDetailStatusRendersLocalRepositoryAndWorkerTimes(t *testing.T) {
	setTestLocalLocation(t, time.FixedZone("test-local", -7*60*60))
	status := daemon.Status{
		Repositories: []daemon.RepositoryStatus{{
			Key:           "project",
			LastEventAt:   time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC),
			LastRestartAt: time.Date(2026, 9, 29, 1, 30, 0, 0, time.UTC),
		}},
		Workers: []daemon.WorkerStatus{{
			Name:             "coder",
			TicketActivity:   "claimed",
			TicketActivityAt: time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC),
		}},
	}
	var output bytes.Buffer
	renderConsoleDetailStatus(&output, status)
	for _, want := range []string{
		"Last change: 2026-09-27T18:30:00-07:00",
		"Last restart: 2026-09-28T18:30:00-07:00",
		"Ticket activity: claimed at 2026-09-27T18:30:00-07:00",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("detail status missing local timestamp %q: %q", want, output.String())
		}
	}
}

func TestReviewReportWindowRendersLocalDateAndOffset(t *testing.T) {
	setTestLocalLocation(t, time.FixedZone("test-local", -7*60*60))
	report := reviewReport{
		WindowStart: time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 9, 29, 1, 30, 0, 0, time.UTC),
	}
	var output bytes.Buffer
	renderReviewReport(&output, report)
	if !strings.Contains(output.String(), "Window: 2026-09-27T18:30:00-07:00 to 2026-09-28T18:30:00-07:00") {
		t.Fatalf("review report window did not render local dates and offset: %q", output.String())
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"window_start":"2026-09-28T01:30:00Z"`) {
		t.Fatalf("JSON report timestamp changed from UTC: %s", encoded)
	}
}

func TestConsoleWatchTimeAndDateMarkerUseLocalDay(t *testing.T) {
	setTestLocalLocation(t, time.FixedZone("test-local", -7*60*60))
	var output bytes.Buffer
	renderer := &consoleWatchRenderer{}
	first := time.Date(2026, 9, 28, 6, 59, 59, 0, time.UTC)
	second := first.Add(2 * time.Second)
	renderer.render(&output, daemon.Event{Type: "daemon.started"}, first)
	renderer.render(&output, daemon.Event{Type: "daemon.stopping"}, second)
	text := output.String()
	if !strings.Contains(text, "--- 2026-09-28 ---") || !strings.Contains(text, "23:59:59") || !strings.Contains(text, "00:00:01") {
		t.Fatalf("watch did not use local time with separate day marker: %q", text)
	}
}
