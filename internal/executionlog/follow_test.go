package executionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestWorkerLogKeyOrdersNumericCollisionSuffix(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	first, err := harness.OpenRawLog("Codex", root, "20261017-00001", "coder", at, "codex", "coder")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := harness.OpenRawLog("Codex", root, "20261017-00001", "coder", at, "codex", "coder")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	const marker = ".codex.coder."
	firstName := filepath.Base(first.Path())
	firstMarker := strings.Index(firstName, marker)
	if firstMarker < 0 {
		t.Fatalf("first execution filename %q has no worker marker", firstName)
	}
	firstKey, ok := workerLogKeyFromSuffix(firstName[firstMarker+len(marker):])
	if !ok {
		t.Fatalf("could not parse first execution filename %q", firstName)
	}
	secondName := filepath.Base(second.Path())
	secondMarker := strings.Index(secondName, marker)
	if secondMarker < 0 {
		t.Fatalf("collision execution filename %q has no worker marker", secondName)
	}
	secondKey, ok := workerLogKeyFromSuffix(secondName[secondMarker+len(marker):])
	if !ok {
		t.Fatalf("could not parse collision execution filename %q", secondName)
	}
	if !firstKey.less(secondKey) || secondKey.collision != 1 {
		t.Fatalf("execution keys first=%#v second=%#v", firstKey, secondKey)
	}
}

const repositoryA = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"
const repositoryB = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"

func testWorker(name, provider, repository string) Worker {
	return Worker{Name: name, Role: "coder.backend", Harness: provider, RepositoryID: repository}
}

func testLog(t *testing.T, root string, worker Worker, at time.Time, text string) *harness.RawLog {
	t.Helper()
	log, err := harness.OpenRawLog(worker.Harness, root, "20261017-00001", worker.Role, at, worker.Harness, worker.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if _, err := log.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	return log
}

func collect(t *testing.T, root string, catalog []Worker) (*Follower, *[]Observation) {
	t.Helper()
	var observations []Observation
	follower, err := New(root, catalog, nil, func(o Observation) error { observations = append(observations, o); return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(follower.Close)
	return follower, &observations
}

func poll(t *testing.T, f *Follower, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		// Force the documented fallback scan synchronously, independent of filesystem
		// timestamp resolution. Read and rollover completion occurs before Poll returns.
		if err := f.Poll(time.Now().Add(workerWatchRescanInterval)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiProviderObservationsUseCatalogNamespaceAndPreserveText(t *testing.T) {
	root := t.TempDir()
	catalog := []Worker{testWorker("api", "codex", repositoryA), testWorker("api.codex.foo", "claude", repositoryB), testWorker("foo", "pi", repositoryA)}
	f, observations := collect(t, root, catalog)
	text := "**hello**\n\x1b[31m" + strings.Repeat("界", harness.DisplayLimit+1)
	encoded, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	events := []string{
		fmt.Sprintf(`{"type":"item.completed","item":{"type":"agent_message","text":%s}}`, encoded),
		fmt.Sprintf(`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":%s}]}}`, encoded),
		fmt.Sprintf(`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":%s}]}}`, encoded),
	}
	executions := map[string]string{}
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	for i, worker := range catalog {
		log := testLog(t, root, worker, at, events[i]+"\n")
		executions[worker.Name] = filepath.Base(log.Path())
	}
	// The catalog is a snapshot, including repository identity.
	catalog[0].RepositoryID = repositoryB
	poll(t, f, 2)
	if len(*observations) != 3 {
		t.Fatalf("observations = %#v", *observations)
	}
	for _, o := range *observations {
		worker := f.workers[o.Worker]
		if o.RepositoryID != worker.RepositoryID || o.Harness != worker.Harness || o.TicketID != "20261017-00001" || o.ExecutionID != executions[o.Worker] || filepath.IsAbs(o.ExecutionID) || o.ObservedAt.Location() != time.UTC || o.Record.Text != text {
			t.Errorf("incorrect observation: %#v", o)
		}
	}
}

func TestExactCatalogMatchingNeverFollowsUnselectedLogs(t *testing.T) {
	root := t.TempDir()
	selected := testWorker("foo", "codex", repositoryA)
	f, observations := collect(t, root, []Worker{selected})
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	event := "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"hello\"}}\n"
	for _, worker := range []Worker{
		selected, testWorker("api.codex.foo", "codex", repositoryB), testWorker("foo.extra", "codex", repositoryB),
		testWorker("foo", "claude", repositoryB), {Name: "foo", Role: "coder.backend.codex.api", Harness: "codex", RepositoryID: repositoryB},
	} {
		testLog(t, root, worker, at, event)
	}
	poll(t, f, 2)
	if len(*observations) != 1 || (*observations)[0].Worker != "foo" || (*observations)[0].RepositoryID != repositoryA {
		t.Fatalf("cross-worker output: %#v", *observations)
	}
	for _, name := range []string{
		"bad.coder.backend.codex.foo.20261017T120000.000000000Z.jsonl",
		"20261017-00001.coder.backend.codex.foo.20261017T120000.000000000Z-01.jsonl",
		"20261017-00001.coder.backend.codex.foo.20261017T120000.000000000Z-0.jsonl",
		"20261017-00001.coder.backend.codex.foo.20261017T120000.000000000Z.jsonl.bak",
	} {
		if _, _, ok := f.workerForLog(name); ok {
			t.Errorf("accepted invalid filename %q", name)
		}
	}
}

func TestCatalogFailsClosed(t *testing.T) {
	for _, worker := range []Worker{
		testWorker("foo", "future", repositoryA), testWorker("foo", "", repositoryA), testWorker("foo", "pi", ""), testWorker("foo", "pi", "invalid"),
		{Name: "foo", Harness: "pi", RepositoryID: repositoryA}, testWorker("../foo", "pi", repositoryA),
	} {
		if f, err := New(t.TempDir(), []Worker{worker}, nil, func(Observation) error { return nil }); err == nil {
			f.Close()
			t.Fatalf("accepted %#v", worker)
		}
	}
	worker := testWorker("foo", "pi", repositoryA)
	if _, err := New(t.TempDir(), []Worker{worker, worker}, nil, func(Observation) error { return nil }); err == nil {
		t.Fatal("duplicate accepted")
	}
	if _, err := New(t.TempDir(), []Worker{worker}, nil, nil); err == nil {
		t.Fatal("nil observer accepted")
	}
	if _, err := New(t.TempDir(), make([]Worker, workerWatchMaxWorkers+1), nil, func(Observation) error { return nil }); err == nil {
		t.Fatal("unbounded workers accepted")
	}
}

func TestBaselineSkipsHistoryAndPartialEventForEveryProvider(t *testing.T) {
	for _, provider := range []struct{ name, event string }{
		{"codex", `{"type":"item.completed","item":{"type":"agent_message","text":"hello"}}`},
		{"claude", `{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]}}`},
		{"pi", `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]}}`},
	} {
		t.Run(provider.name, func(t *testing.T) {
			root := t.TempDir()
			worker := testWorker("foo", provider.name, repositoryA)
			at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			testLog(t, root, worker, at.Add(-time.Second), provider.event+"\n")
			current := testLog(t, root, worker, at, provider.event+"\n"+provider.event[:10])
			f, observations := collect(t, root, []Worker{worker})
			poll(t, f, 1)
			if len(*observations) != 0 {
				t.Fatal("baseline replayed")
			}
			if _, err := current.Write([]byte(provider.event[10:] + "\n" + provider.event + "\n")); err != nil {
				t.Fatal(err)
			}
			poll(t, f, 1)
			if len(*observations) != 1 || (*observations)[0].Record.Text != "hello" {
				t.Fatalf("baseline/partial replay: %#v", *observations)
			}
		})
	}
}

func TestBurstExecutionsDrainInNumericCollisionOrder(t *testing.T) {
	root := t.TempDir()
	worker := testWorker("foo", "pi", repositoryA)
	f, observations := collect(t, root, []Worker{worker})
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		testLog(t, root, worker, at, fmt.Sprintf("{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"%d\"}]}}\n", i))
	}
	poll(t, f, 13)
	if len(*observations) != 12 {
		t.Fatalf("lost burst output: %#v", *observations)
	}
	for i, o := range *observations {
		if o.Record.Text != strconv.Itoa(i) {
			t.Fatalf("collision order: %#v", *observations)
		}
	}
}

// ReadAt uses the same file operation as the follower and reports os.ErrClosed
// on every supported OS. Windows Stat instead calls GetFileType directly and
// can report ERROR_INVALID_HANDLE. Keep the actual reader, rather than testing
// only that the follower removed its map entry: an open empty file returns EOF.
func assertReaderClosed(t *testing.T, file *os.File) {
	t.Helper()
	var b [1]byte
	if _, err := file.ReadAt(b[:], 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("reader not closed: %v", err)
	}
}

func TestSourceErrorClosesActiveReader(t *testing.T) {
	root := t.TempDir()
	worker := testWorker("foo", "pi", repositoryA)
	log := testLog(t, root, worker, time.Now(), "")
	f, _ := collect(t, root, []Worker{worker})
	file := f.readers[worker.Name].file
	var b [1]byte
	if _, err := file.ReadAt(b[:], 0); !errors.Is(err, io.EOF) {
		t.Fatalf("reader was not open before source failure: %v", err)
	}
	// Exercise the real directory-validation failure with an active reader.
	// Redirect only this follower's lookup to a regular file; no open paths
	// need to be renamed or removed to induce the source error.
	f.logDir = log.Path()
	if err := f.Poll(time.Now()); err == nil {
		t.Fatal("regular file accepted as log directory")
	}
	if !f.closed || len(f.readers) != 0 {
		t.Fatal("source failure left follower open")
	}
	assertReaderClosed(t, file)
	f.Close()
}

func TestPollingKeepsDiscoveryBoundedAndFailsOnPendingOverflow(t *testing.T) {
	root := t.TempDir()
	worker := testWorker("foo", "pi", repositoryA)
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	log := testLog(t, root, worker, at, "")
	f, _ := collect(t, root, []Worker{worker})
	file := f.readers[worker.Name].file
	// No directory change and no fallback deadline means there is no archive scan.
	f.lastMod = time.Now().Add(time.Hour)
	f.lastRescan = time.Now()
	if err := f.Poll(time.Now()); err != nil {
		t.Fatal(err)
	}
	if !f.lastMod.After(time.Now()) {
		t.Fatal("unnecessary archive rescan")
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	// Real directory entries exercise batched scanning and the aggregate bound.
	for i := 0; i <= workerWatchPendingLimit; i++ {
		name := fmt.Sprintf("20261017-00001.%s.pi.foo.%s.jsonl", worker.Role, at.Add(time.Duration(i+1)*time.Second).Format("20060102T150405.000000000Z"))
		if err := os.WriteFile(filepath.Join(root, "logs", name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err := f.Poll(time.Now().Add(workerWatchRescanInterval))
	if err == nil || !strings.Contains(err.Error(), "pending executions") {
		t.Fatalf("overflow = %v", err)
	}
	if !f.closed || len(f.readers) != 0 {
		t.Fatal("overflow left follower open")
	}
	assertReaderClosed(t, file)
}

func TestObserverFailureAndCancellationCloseReaders(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRun), func(t *testing.T) {
			root := t.TempDir()
			worker := testWorker("foo", "pi", repositoryA)
			log := testLog(t, root, worker, time.Now(), "")
			failure := errors.New("writer failed")
			f, err := New(root, []Worker{worker}, nil, func(Observation) error { return failure })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(f.Close)
			file := f.readers[worker.Name].file
			if cancelRun {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				done := make(chan error, 1)
				go func() { done <- f.Run(ctx) }()
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("cancellation did not complete")
				}
			} else {
				if _, err := log.Write([]byte("{\"type\":\"turn_end\"}\n")); err != nil {
					t.Fatal(err)
				}
				if err := f.Poll(time.Now()); !errors.Is(err, failure) {
					t.Fatalf("observer error = %v", err)
				}
			}
			if !f.closed || len(f.readers) != 0 {
				t.Fatal("follower not closed")
			}
			assertReaderClosed(t, file)
			f.Close()
		})
	}
}

func TestPrivateLogsAndDirectories(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix mode and symlink checks")
	}
	worker := testWorker("foo", "pi", repositoryA)
	for _, mode := range []os.FileMode{0o644, 0o666} {
		t.Run(mode.String(), func(t *testing.T) {
			root := t.TempDir()
			log := testLog(t, root, worker, time.Now(), "")
			if err := os.Chmod(log.Path(), mode); err != nil {
				t.Fatal(err)
			}
			if f, err := New(root, []Worker{worker}, nil, func(Observation) error { return nil }); err == nil {
				f.Close()
				t.Fatal("insecure log accepted")
			}
		})
	}
	root := t.TempDir()
	log := testLog(t, root, worker, time.Now(), "")
	if err := os.Chmod(filepath.Join(root, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if f, err := New(root, []Worker{worker}, nil, func(Observation) error { return nil }); err == nil {
		f.Close()
		t.Fatal("insecure directory accepted")
	}
	if err := os.Chmod(filepath.Join(root, "logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.jsonl")
	if err := os.Symlink(log.Path(), link); err != nil {
		t.Fatal(err)
	}
	if reader, err := openWorkerLog(link, worker, true); err == nil {
		reader.file.Close()
		t.Fatal("symlink log accepted")
	}
	f, _ := collect(t, root, []Worker{worker})
	file := f.readers[worker.Name].file
	if err := os.Rename(filepath.Join(root, "logs"), filepath.Join(root, "original")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "original"), filepath.Join(root, "logs")); err != nil {
		t.Fatal(err)
	}
	if err := f.Poll(time.Now()); err == nil {
		t.Fatal("replaced directory accepted")
	}
	assertReaderClosed(t, file)
}

func TestFramingIsBoundedAcrossEOFAndResumesAfterOversizedEvents(t *testing.T) {
	root := t.TempDir()
	worker := testWorker("foo", "pi", repositoryA)
	f, observations := collect(t, root, []Worker{worker})
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	log := testLog(t, root, worker, at, "{")
	poll(t, f, 2)
	for i := 0; i < 3; i++ {
		poll(t, f, 1)
	}
	if len(*observations) != 0 {
		t.Fatalf("incomplete EOF emitted: %#v", *observations)
	}
	if _, err := log.Write([]byte("\"type\":\"turn_end\"}\n{bad}\n" + strings.Repeat("x", harness.MaxEventBytes+1))); err != nil {
		t.Fatal(err)
	}
	poll(t, f, 12)
	if len(*observations) != 3 || (*observations)[0].Record.Code != "turn_completed" || (*observations)[1].Record.Code != "invalid_event" || (*observations)[2].Record.Code != "event_too_large" {
		t.Fatalf("framing observations: %#v", *observations)
	}
	reader := f.readers[worker.Name]
	if !reader.discardPartial {
		t.Fatal("oversized fragment not discarded")
	}
	// Repeated EOF must not replay or grow the framer.
	poll(t, f, 3)
	if len(*observations) != 3 {
		t.Fatal("EOF replayed diagnostics")
	}
	if _, err := log.Write([]byte("discard this tail\n{\"type\":\"turn_end\"}\n")); err != nil {
		t.Fatal(err)
	}
	poll(t, f, 1)
	if len(*observations) != 4 || (*observations)[3].Record.Code != "turn_completed" {
		t.Fatalf("failed to resume: %#v", *observations)
	}
}

func TestBoundedReadsDrainOldTailAndFinishBeforeNextExecution(t *testing.T) {
	root := t.TempDir()
	worker := testWorker("foo", "pi", repositoryA)
	f, observations := collect(t, root, []Worker{worker})
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// Noise occupies more than one read budget. The final valid record has no
	// newline, so rollover must first drain the old file and then finish framing.
	noise := strings.Repeat("{\"type\":\"session\"}\n", workerWatchReadLimit/19+1)
	old := testLog(t, root, worker, at, noise+`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"tail"}]}}`)
	testLog(t, root, worker, at.Add(time.Second), "{\"type\":\"turn_end\"}\n")
	poll(t, f, 1)
	reader := f.readers[worker.Name]
	file := reader.file
	poll(t, f, 1)
	if reader.offset > workerWatchReadLimit || reader.execution != filepath.Base(old.Path()) {
		t.Fatal("poll exceeded read budget")
	}
	if len(*observations) != 0 {
		t.Fatalf("advanced without draining: %#v", *observations)
	}
	poll(t, f, 3)
	if len(*observations) != 2 || (*observations)[0].Record.Text != "tail" || (*observations)[1].Record.Code != "turn_completed" {
		t.Fatalf("rollover output: %#v", *observations)
	}
	assertReaderClosed(t, file)
}

func TestFullCatalogValidationPrecedesWorkerAndRepositorySelection(t *testing.T) {
	catalog := []Worker{
		testWorker("api.codex.foo", "codex", repositoryA),
		{Name: "foo", Role: "coder.backend.codex.api", Harness: "codex", RepositoryID: repositoryB},
	}
	byRepository := []string{}
	for _, worker := range catalog {
		if worker.RepositoryID == repositoryA {
			byRepository = append(byRepository, worker.Name)
		}
	}
	for _, test := range []struct {
		name     string
		selected []string
	}{
		{name: "all"},
		{name: "first worker", selected: []string{catalog[0].Name}},
		{name: "second worker", selected: []string{catalog[1].Name}},
		{name: "repository", selected: byRepository},
		{name: "empty", selected: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, err := New(t.TempDir(), catalog, test.selected, func(Observation) error { return nil })
			if err == nil {
				f.Close()
				t.Fatal("filtered subscription accepted ambiguous full inventory")
			}
			if !strings.Contains(err.Error(), "ambiguous execution-log identity") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSelectedWorkersReadOnlyTheirCatalogNamespace(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			root := t.TempDir()
			catalog := []Worker{testWorker("api.codex.foo", "pi", repositoryA), testWorker("foo", "pi", repositoryB)}
			// A repository-filtered consumer passes selected names alongside the full
			// authoritative inventory, so selection cannot bypass catalog validation.
			selected := []string{}
			if !empty {
				selected = append(selected, catalog[0].Name)
			}
			var observations []Observation
			f, err := New(root, catalog, selected, func(o Observation) error { observations = append(observations, o); return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(f.Close)
			at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			for _, worker := range catalog {
				testLog(t, root, worker, at, "{\"type\":\"turn_end\"}\n")
			}
			poll(t, f, 2)
			if empty {
				if len(observations) != 0 || len(f.readers) != 0 {
					t.Fatalf("empty subscription followed workers: %#v", observations)
				}
			} else if len(observations) != 1 || observations[0].Worker != catalog[0].Name || observations[0].RepositoryID != repositoryA || len(f.readers) != 1 {
				t.Fatalf("selection crossed namespace: %#v", observations)
			}
			// Mutating the selection after construction cannot add another reader.
			selected = append(selected, catalog[1].Name)
			if !empty {
				selected[0] = catalog[1].Name
			}
			poll(t, f, 1)
			if len(f.readers) != len(observations) {
				t.Fatal("selection was not snapshotted")
			}
		})
	}
}

func TestUnknownWorkerSelectionFailsClosed(t *testing.T) {
	catalog := []Worker{testWorker("foo", "pi", repositoryA)}
	f, err := New(t.TempDir(), catalog, []string{"unknown"}, func(Observation) error { return nil })
	if err == nil {
		f.Close()
		t.Fatal("unknown selection accepted")
	}
	if !strings.Contains(err.Error(), "unknown worker") {
		t.Fatalf("error = %v", err)
	}
}
