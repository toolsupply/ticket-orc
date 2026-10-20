package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
)

// Tests use polls one minute ahead when discovering new executions. This exceeds
// the fallback rescan interval without waiting or relying on directory mtime changes.
func TestWorkerWatchStartsAtEOFAndSkipsIncompleteExistingRecord(t *testing.T) {
	root := t.TempDir()
	log := openWatchTestLog(t, root, "coder", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	if _, err := log.Write([]byte(`{"type":"item.completed","item":{"type":"agent_message","text":"old"}}`)); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	if _, err := log.Write([]byte("\n" + `{"type":"item.completed","item":{"type":"agent_message","text":"new"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); strings.Contains(got, "old") || !strings.Contains(got, "[11111111…:coder] new") {
		t.Fatalf("watch output=%q", got)
	}
}

func TestWorkerWatchFollowsNewExecutionsAndPrefixesEveryRenderedLine(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	first := openWatchTestLog(t, root, "coder", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	if _, err := first.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"first\\nline\"}}\n")); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	second := openWatchTestLog(t, root, "coder", time.Date(2026, 10, 17, 12, 0, 1, 0, time.UTC))
	if _, err := second.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"reasoning\",\"text\":\"secret\"}}\n" +
		"{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"ansi\\u001b[31mred\\u001b[0m\"}}\n")); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, want := range []string{"[11111111…:coder] first", "[11111111…:coder] line", "[11111111…:coder] ansi[31mred[0m"} {
		if !strings.Contains(got, want) {
			t.Errorf("output %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "secret") {
		t.Fatalf("reasoning leaked: %q", got)
	}
}

func TestWorkerWatchOrdersAndReadsEveryExecutionDiscoveredTogether(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// Ticket IDs sort in the opposite order from execution time.
	for _, entry := range []struct {
		ticket string
		at     time.Time
		text   string
	}{
		{"20261017-99999", base, "earlier"},
		{"20261017-00001", base.Add(time.Second), "later"},
	} {
		log, err := harness.OpenRawLog("Codex", root, entry.ticket, "coder", entry.at, "codex", "coder")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := log.Write([]byte(fmt.Sprintf("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":%q}}\n", entry.text))); err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	got := output.String()
	first, second := strings.Index(got, "[11111111…:coder] earlier"), strings.Index(got, "[11111111…:coder] later")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("execution output order/loss: %q", got)
	}
}

func TestWorkerWatchDrainsPreviousExecutionBeforeRollover(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	first := openWatchTestLog(t, root, "coder", base)
	filler := []byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"reasoning\",\"text\":\"ignored\"}}\n")
	var data bytes.Buffer
	for data.Len() <= (1 << 20) {
		data.Write(filler)
	}
	// Leave the final event unterminated so rollover must also finish it.
	data.WriteString("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"old-tail\"}}")
	if _, err := first.Write(data.Bytes()); err != nil {
		t.Fatal(err)
	}
	// Pin both directory observations to the same timestamp. A successor must
	// still be discovered by the supported fallback poll, without an mtime bump.
	logDir := filepath.Join(root, "logs")
	stamp := base
	if err := os.Chtimes(logDir, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logDir)
	if err != nil {
		t.Fatal(err)
	}
	// Use the timestamp actually represented by this filesystem.
	stamp = info.ModTime()
	// Discover the old execution before introducing its successor.
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	second := openWatchTestLog(t, root, "coder", base.Add(time.Second))
	if _, err := second.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"new-log\"}}\n")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(logDir, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(logDir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(stamp) {
		t.Fatalf("directory timestamp changed: got %v want %v", info.ModTime(), stamp)
	}
	// The first bounded read also discovers the successor via fallback.
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "" {
		t.Fatalf("first bounded read reached the tail or successor: %q", got)
	}
	if err := watch.poll(time.Now()); err != nil { // drain old tail and advance
		t.Fatal(err)
	}
	if got := output.String(); strings.Count(got, "[11111111…:coder] old-tail") != 1 || strings.Contains(got, "new-log") {
		t.Fatalf("old execution was not drained before successor read: %q", got)
	}
	if err := watch.poll(time.Now()); err != nil { // read successor
		t.Fatal(err)
	}
	if err := watch.poll(time.Now()); err != nil { // EOF must not repeat either event
		t.Fatal(err)
	}
	got := output.String()
	oldTail, newLog := strings.Index(got, "[11111111…:coder] old-tail"), strings.Index(got, "[11111111…:coder] new-log")
	if oldTail < 0 || newLog < 0 || oldTail >= newLog {
		t.Fatalf("rollover lost or reordered output: tail=%d next=%d output=%q", oldTail, newLog, got)
	}
	for _, event := range []string{"[11111111…:coder] old-tail", "[11111111…:coder] new-log"} {
		if count := strings.Count(got, event); count != 1 {
			t.Errorf("event %q rendered %d times, want once: %q", event, count, got)
		}
	}
}

func TestWorkerWatchRolloverFinishesFinalUnterminatedEvent(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	first := openWatchTestLog(t, root, "coder", base)
	if _, err := first.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"final event\"}}")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	second := openWatchTestLog(t, root, "coder", base.Add(time.Second))
	if _, err := second.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"next execution\"}}\n")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	got := output.String()
	firstIndex, nextIndex := strings.Index(got, "[11111111…:coder] final event"), strings.Index(got, "[11111111…:coder] next execution")
	if firstIndex < 0 || nextIndex < 0 || firstIndex >= nextIndex {
		t.Fatalf("rollover lost/reordered final event: %q", got)
	}
}

func TestWorkerWatchRolloverSkipsIncompleteAndPreexistingPartialRecords(t *testing.T) {
	for _, test := range []struct {
		name        string
		preexisting bool
	}{
		{name: "incomplete record written during watch"},
		{name: "preexisting partial record", preexisting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			var output bytes.Buffer
			if test.preexisting {
				old := openWatchTestLog(t, root, "coder", base)
				if _, err := old.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"must not replay\"}")); err != nil {
					t.Fatal(err)
				}
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
			}
			watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Close()
			if !test.preexisting {
				old := openWatchTestLog(t, root, "coder", base)
				if _, err := old.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"unfinished")); err != nil {
					t.Fatal(err)
				}
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			newLog := openWatchTestLog(t, root, "coder", base.Add(time.Second))
			if _, err := newLog.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"next event\"}}\n")); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			got := output.String()
			if !strings.Contains(got, "[11111111…:coder] next event") || strings.Contains(got, "must not replay") || strings.Contains(got, "unfinished") {
				t.Fatalf("rollover partial handling output=%q", got)
			}
		})
	}
}

func TestWorkerWatchAllLabelsConcurrentWorkersAndRejectsUnknownWorker(t *testing.T) {
	root := t.TempDir()
	status := daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}, {Name: "reviewer", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, status, watchOptions{selector: "all"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	for i, worker := range []string{"coder", "reviewer"} {
		log := openWatchTestLog(t, root, worker, time.Date(2026, 10, 17, 12, 0, i, 0, time.UTC))
		_, _ = log.Write([]byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"hello\"}}\n"))
	}
	for i := 0; i < 2; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"[11111111…:coder] hello", "[11111111…:reviewer] hello"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("output %q missing %q", output.String(), want)
		}
	}
	if _, err := newWorkerLogWatch(root, status, watchOptions{selector: "unknown"}, &output); err == nil || !strings.Contains(err.Error(), "unknown worker") {
		t.Fatalf("unknown worker error = %v", err)
	}
}

func TestWorkerWatchMatchesDottedRolesAndSimilarDottedWorkerNames(t *testing.T) {
	root := t.TempDir()
	status := daemon.Status{Workers: []daemon.WorkerStatus{
		{Name: "api", Role: "coder.backend", Harness: "codex", RepositoryID: joinTestRepositoryID},
		{Name: "api.coder", Role: "coder.backend", Harness: "codex", RepositoryID: joinTestRepositoryID},
		{Name: "api.codex.foo", Role: "coder.backend", Harness: "codex", RepositoryID: joinTestRepositoryID},
		{Name: "foo", Role: "coder.backend", Harness: "codex", RepositoryID: joinTestRepositoryID},
	}}
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, status, watchOptions{selector: "all"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	for i, worker := range []struct {
		name string
		text string
	}{
		{name: "api", text: "short worker"},
		{name: "api.coder", text: "dotted worker"},
		{name: "api.codex.foo", text: "codex-containing worker"},
		{name: "foo", text: "suffix worker"},
	} {
		log, err := harness.OpenRawLog("Codex", root, fmt.Sprintf("20261017-%05d", i+1), "coder.backend", base.Add(time.Duration(i)*time.Second), "codex", worker.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := log.Write([]byte(fmt.Sprintf("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":%q}}\n", worker.text))); err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	got := output.String()
	for _, want := range []string{"[11111111…:api] short worker", "[11111111…:api.coder] dotted worker", "[11111111…:api.codex.foo] codex-containing worker", "[11111111…:foo] suffix worker"} {
		if !strings.Contains(got, want) {
			t.Errorf("dotted worker watch output %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "[11111111…:api] dotted worker") || strings.Contains(got, "[11111111…:api.coder] short worker") || strings.Contains(got, "[11111111…:foo] codex-containing worker") {
		t.Fatalf("similar worker names received each other's output: %q", got)
	}
}

func TestWorkerWatchSkipsMalformedAndOversizedRecords(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	log := openWatchTestLog(t, root, "coder", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	tooLong := strings.Repeat("x", harness.MaxEventBytes+1)
	if _, err := log.Write([]byte("{malformed}\n" + tooLong + "\n" + `{"type":"item.completed","item":{"type":"agent_message","text":"after"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	got := output.String()
	if !strings.Contains(got, "[11111111…:coder] [Codex log event skipped:") || !strings.Contains(got, "event exceeds") || !strings.Contains(got, "[11111111…:coder] after") {
		t.Fatalf("malformed and oversized log handling output=%q", got)
	}
}

func TestWorkerWatchRejectsSymlinkLogDirectory(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("symlink test uses Unix permissions and semantics")
	}
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "logs")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "not private") {
		t.Fatalf("symlink log directory error=%v", err)
	}
}

func TestWorkerWatchCancellationClosesViewer(t *testing.T) {
	root := t.TempDir()
	watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWorkerWatch(ctx, watch) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("viewer returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("viewer did not stop after cancellation")
	}
}

func openWatchTestLog(t *testing.T, root, worker string, at time.Time) *harness.RawLog {
	t.Helper()
	log, err := harness.OpenRawLog("Codex", root, "20261017-00001", "coder", at, "codex", worker)
	if err != nil {
		t.Fatalf("open test raw log: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return log
}

const watchDetailEvents = `{"type":"item.started","item":{"type":"command_execution","command":"bash noisy-command"}}
{"type":"item.completed","item":{"type":"command_execution","command":"bash failed-command","exit_code":1}}
{"type":"item.completed","item":{"type":"file_change","changes":[{"kind":"update","path":"changed.go"}]}}
{"type":"item.completed","item":{"type":"reasoning","text":"private reasoning"}}
{"type":"thread.started","thread_id":"private protocol"}
{"type":"item.completed","item":{"type":"agent_message","text":"hello\nworld\u001b"}}
{"type":"error","message":"diagnostic"}
{"type":"item.completed","item":{"type":"error","message":"item diagnostic"}}
{"type":"turn.failed","error":{"message":"failed turn"}}
{"type":"turn.completed"}
`

func TestWorkerWatchTimestampsEveryMessageAndDiagnosticRow(t *testing.T) {
	var output bytes.Buffer
	observed := time.Date(2026, 10, 17, 18, 34, 56, 0, time.UTC)
	for _, line := range strings.Split(watchDetailEvents+"invalid JSON", "\n") {
		records, err := codex.InterpretEvent([]byte(line))
		if err != nil {
			records = []harness.LogRecord{{Kind: "diagnostic", Code: "invalid_event", Text: err.Error()}}
		}
		for _, record := range records {
			if err := renderWorkerObservation(&output, watchOptions{}, executionlog.Observation{Worker: "coder\x1b", Harness: "codex", ObservedAt: observed, Record: record}, "11111111…:coder"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := renderWorkerObservation(&output, watchOptions{}, executionlog.Observation{Worker: "coder", Harness: "codex", ObservedAt: observed, Record: harness.LogRecord{Kind: "diagnostic", Code: "event_too_large", Text: "event exceeds limit"}}, "11111111…:coder"); err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(rows) != 8 {
		t.Fatalf("rows=%q, want two message lines, four diagnostics/summaries and two skipped events", rows)
	}
	prefix := observed.Local().Format("15:04:05") + " [11111111…:coder] "
	for _, row := range rows {
		if !strings.HasPrefix(row, prefix) || strings.ContainsRune(row, '\x1b') {
			t.Errorf("unsanitized or missing local observation timestamp/worker: %q", row)
		}
	}
	for _, want := range []string{"hello", "world", "[codex error] diagnostic", "[codex error] item diagnostic", "[codex failed] failed turn", "[codex] turn complete", "malformed JSONL", "event exceeds"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("output missing %q: %q", want, output.String())
		}
	}
	for _, hidden := range []string{"noisy-command", "failed-command", "changed.go", "private reasoning", "private protocol"} {
		if strings.Contains(output.String(), hidden) {
			t.Errorf("quiet watch leaked %q", hidden)
		}
	}
}

func TestWorkerWatchVerbositySurvivesExecutionRollover(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("verbose=%t", verbose), func(t *testing.T) {
			root := t.TempDir()
			base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			first := openWatchTestLog(t, root, "coder", base)
			var output bytes.Buffer
			watch, err := newWorkerLogWatch(root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder", verbose: verbose}, &output)
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Close()
			// Exercise both the initial EOF reader and readers opened at rollover.
			for i := 0; i < 2; i++ {
				log := first
				if i == 1 {
					log = openWatchTestLog(t, root, "coder", base.Add(time.Second))
				}
				if _, err := log.Write([]byte(watchDetailEvents + `{"type":"item.completed","item":{"type":"command_execution","command":"final-command","exit_code":2}}`)); err != nil {
					t.Fatal(err)
				}
				if err := log.Close(); err != nil {
					t.Fatal(err)
				}
				for n := 0; n < 3; n++ {
					if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
						t.Fatal(err)
					}
				}
			}
			// A third execution finishes the second unterminated command record.
			last := openWatchTestLog(t, root, "coder", base.Add(2*time.Second))
			if err := last.Close(); err != nil {
				t.Fatal(err)
			}
			for n := 0; n < 3; n++ {
				if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			for _, detail := range []string{"$ bash noisy-command", "[exit 1] bash failed-command", "[files] update changed.go", "[exit 2] final-command"} {
				want := 0
				if verbose {
					want = 2
				}
				if got := strings.Count(output.String(), detail); got != want {
					t.Errorf("detail %q count=%d want=%d: %q", detail, got, want, output.String())
				}
			}
			if strings.Count(output.String(), "[11111111…:coder] hello") != 2 || strings.Contains(output.String(), "private") {
				t.Fatalf("lost messages or leaked protocol/reasoning: %q", output.String())
			}
		})
	}
}

func TestWorkerWatchRendersAllManagedProviders(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	status := daemon.Status{Workers: []daemon.WorkerStatus{
		{Name: "codex", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID},
		{Name: "claude", Role: "coder", Harness: "claude", RepositoryID: joinTestRepositoryID},
		{Name: "pi", Role: "coder", Harness: "pi", RepositoryID: joinOtherRepositoryID},
	}}
	watch, err := newWorkerLogWatch(root, status, watchOptions{selector: "all"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	for i, event := range []string{
		`{"type":"item.completed","item":{"type":"agent_message","text":"hello\nworld"}}`,
		`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hello\nworld"}]}}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hello\nworld"}]}}`,
	} {
		worker := status.Workers[i]
		log, err := harness.OpenRawLog(worker.Harness, root, "20261017-00001", worker.Role, time.Now(), worker.Harness, worker.Name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		if _, err := log.Write([]byte(event + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := watch.poll(time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	for _, worker := range status.Workers {
		for _, text := range []string{"hello", "world"} {
			if !strings.Contains(output.String(), "["+shortIdentity(worker.RepositoryID)+":"+worker.Name+"] "+text) {
				t.Fatalf("missing provider message: %q", output.String())
			}
		}
	}
	for _, row := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		if _, err := time.Parse("15:04:05", row[:8]); err != nil {
			t.Fatalf("missing timestamp: %q", row)
		}
	}
}

type shortWatchWriter struct{}

func (shortWatchWriter) Write([]byte) (int, error) { return 0, nil }

func TestWorkerWatchPropagatesShortWrites(t *testing.T) {
	err := renderWorkerObservation(shortWatchWriter{}, watchOptions{}, executionlog.Observation{Worker: "coder", Harness: "pi", ObservedAt: time.Now(), Record: harness.LogRecord{Kind: "message", Text: "hello"}}, "11111111…:coder")
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write = %v", err)
	}
}

func TestWorkerWatchRejectsAmbiguousInventoryBeforeSelection(t *testing.T) {
	status := daemon.Status{Workers: []daemon.WorkerStatus{
		{Name: "api.codex.foo", Role: "coder.backend", Harness: "codex", RepositoryID: joinTestRepositoryID},
		{Name: "foo", Role: "coder.backend.codex.api", Harness: "codex", RepositoryID: joinOtherRepositoryID},
	}}
	for _, selector := range []string{"all", "api.codex.foo", "foo"} {
		t.Run(selector, func(t *testing.T) {
			watch, err := newWorkerLogWatch(t.TempDir(), status, watchOptions{selector: selector}, io.Discard)
			if err == nil {
				watch.Close()
				t.Fatal("accepted ambiguous managed inventory after worker selection")
			}
			if !strings.Contains(err.Error(), "ambiguous execution-log identity") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
