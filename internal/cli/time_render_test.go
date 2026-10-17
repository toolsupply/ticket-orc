package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const testLocalTimeProcessEnv = "TICKET_ORC_TEST_LOCAL_TIME_PROCESS"

func runWithTestLocalTime(t *testing.T) bool {
	t.Helper()
	if os.Getenv(testLocalTimeProcessEnv) == "1" {
		return false
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), testLocalTimeProcessEnv+"=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("local-time subprocess failed: %v\n%s", err, output)
	}
	return true
}

func TestDaemonCompactStatusRendersLocalTimeWithoutChangingJSON(t *testing.T) {
	if runWithTestLocalTime(t) {
		return
	}
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
	if runWithTestLocalTime(t) {
		return
	}
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
	if runWithTestLocalTime(t) {
		return
	}
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
	if runWithTestLocalTime(t) {
		return
	}
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
