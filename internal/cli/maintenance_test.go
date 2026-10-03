package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestContextTelemetrySummaryShowsKnownAndStaleValues(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	telemetry := contextheadroom.Telemetry{Known: true, Used: 800, Window: 1000, Remaining: 200, ObservedAt: now.Add(-time.Hour)}
	if got := contextTelemetrySummary(telemetry, now); got != "known used=800 window=1000 remaining=200 observed_at="+telemetry.ObservedAt.Local().Format(time.RFC3339) {
		t.Fatalf("known telemetry summary = %q", got)
	}
	if got := contextTelemetrySummary(telemetry, now.Add(25*time.Hour)); !strings.HasPrefix(got, "stale used=800 window=1000 remaining=200") {
		t.Fatalf("stale telemetry summary = %q", got)
	}
	if got := contextTelemetrySummary(contextheadroom.Telemetry{}, now); got != "unknown" {
		t.Fatalf("unknown telemetry summary = %q", got)
	}
}

func TestContextTelemetrySummaryUsesLocalTimestampAcrossDateBoundary(t *testing.T) {
	setTestLocalLocation(t, time.FixedZone("test-local", -7*60*60))
	observedAt := time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC)
	telemetry := contextheadroom.Telemetry{Known: true, Used: 800, Window: 1000, Remaining: 200, ObservedAt: observedAt}
	now := observedAt.Add(time.Hour)
	if got, want := contextTelemetrySummary(telemetry, now), "known used=800 window=1000 remaining=200 observed_at=2026-09-27T18:30:00-07:00"; got != want {
		t.Fatalf("known local telemetry summary = %q, want %q", got, want)
	}
	if got := contextTelemetrySummary(telemetry, now.Add(25*time.Hour)); !strings.HasPrefix(got, "stale used=800 window=1000 remaining=200 observed_at=") {
		t.Fatalf("local timestamp change affected stale classification: %q", got)
	}
}

func TestRenderStateShowsAuthoritativeSessionContextTelemetry(t *testing.T) {
	now := time.Now().UTC()
	snapshot := state.Snapshot{Sessions: []state.Session{{
		Ticket: "20260923-telemetry", Role: "coder", Harness: "codex", ID: "thread",
		ContextTelemetry: contextheadroom.Telemetry{Known: true, Used: 75, Window: 100, Remaining: 25, ObservedAt: now},
	}}}
	var output bytes.Buffer
	if err := renderState(&output, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"context=known", "used=75", "window=100", "remaining=25"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("state output %q does not contain %q", output.String(), want)
		}
	}
}

func TestStateValueEscapesControlsAndBoundsOutput(t *testing.T) {
	value := "repository\x1b\n" + strings.Repeat("x", maxHumanStateRunes+20)
	got := stateValue(value)
	if strings.ContainsAny(got, "\x00\x1b\n\r") || !strings.Contains(got, `\u001b\u000a`) {
		t.Fatalf("stateValue(%q) = %q, want escaped control characters", value, got)
	}
	if gotRunes := []rune(got); len(gotRunes) > maxHumanStateRunes+1 || !strings.HasSuffix(got, "…") {
		t.Fatalf("stateValue length = %d runes, want <= %d with truncation marker", len(gotRunes), maxHumanStateRunes+1)
	}
}

func TestGCUsesSelectedWorkerWorkingDirectoryAndNamespace(t *testing.T) {
	root := t.TempDir()
	repoA, repoB := filepath.Join(root, "repo-a"), filepath.Join(root, "repo-b")
	if err := os.MkdirAll(repoA, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(repoB, 0o700); err != nil {
		t.Fatal(err)
	}
	ticketName := "ticket"
	if runtime.GOOS == "windows" {
		ticketName += ".exe"
	}
	ticketBin := filepath.Join(root, ticketName)
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executableBytes, err := os.ReadFile(testExecutable)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ticketBin, executableBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TICKET_ORC_TEST_FAKE_TICKET", "1")
	t.Setenv("TICKET_ORC_TEST_FAKE_TICKET_REPO_A", repoA)
	t.Setenv("TICKET_ORC_TEST_FAKE_TICKET_REPO_B", repoB)
	stateDir := filepath.Join(root, "state")
	configPath := filepath.Join(root, "config.json")
	config := fmt.Sprintf(`{"version":1,"local_dir":%q,"workers":{"worker-a":{"role":"coder","actor":"actor","working_dir":%q},"worker-b":{"role":"coder","actor":"actor","working_dir":%q}}}`, stateDir, repoA, repoB)
	writeConfigFixture(t, configPath, config)
	ctx := context.Background()
	for _, repository := range []string{joinTestRepositoryID, joinOtherRepositoryID} {
		if _, err := state.NewForRepository(stateDir, repository).IncrementBounces(ctx, "20260921-77121"); err != nil {
			t.Fatal(err)
		}
	}
	configA, _, err := parseGCConfig([]string{"--config", configPath, "--worker", "worker-a"}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := executeGC(configA, &output, io.Discard); err != nil {
		t.Fatalf("gc error=%v output=%q", err, output.String())
	}
	if snapshot, err := state.NewForRepository(stateDir, joinTestRepositoryID).Read(ctx); err != nil || len(snapshot.Bounces) != 0 {
		t.Fatalf("repository A state=%#v err=%v", snapshot, err)
	}
	if snapshot, err := state.NewForRepository(stateDir, joinOtherRepositoryID).Read(ctx); err != nil || snapshot.Bounces[joinOtherRepositoryID+"\x0020260921-77121"] != 1 {
		t.Fatalf("repository B state=%#v err=%v", snapshot, err)
	}
}
