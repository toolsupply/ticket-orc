package daemonclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness"
)

const logsRepoA = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"
const logsRepoB = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"
const logsInstance = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"

func startLogsDaemon(t *testing.T, root string, inventory daemon.LogInventory) (*daemon.Server, *Client, context.Context, func(daemon.LogInventory)) {
	t.Helper()
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	server, err := daemon.NewServer(daemon.Config{StateDir: root, InstanceID: logsInstance, EndpointKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Logs: &daemon.LogSource{LocalDir: root, Inventory: func() daemon.LogInventory {
		mu.Lock()
		defer mu.Unlock()
		return daemon.LogInventory{Workers: append([]executionlog.Worker(nil), inventory.Workers...), RepositoryIDs: append([]string(nil), inventory.RepositoryIDs...)}
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := NewWithEndpoint("", server.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	return server, client, ctx, func(next daemon.LogInventory) { mu.Lock(); inventory = next; mu.Unlock() }
}

func openLogs(t *testing.T, c *Client, ctx context.Context, q LogsQuery) *LogStream {
	t.Helper()
	s, err := c.Logs(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func nextLog(t *testing.T, s *LogStream) LogsMessage {
	t.Helper()
	m, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func logFixture(t *testing.T, root string, w executionlog.Worker, at time.Time, text string) *harness.RawLog {
	t.Helper()
	log, err := harness.OpenRawLog(w.Harness, root, "20261017-00001", w.Role, at, w.Harness, w.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	appendLog(t, log, text)
	return log
}
func appendLog(t *testing.T, log *harness.RawLog, text string) {
	t.Helper()
	if _, err := log.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
}
func codexMessage(text string) string {
	data, _ := json.Marshal(text)
	return `{"type":"item.completed","item":{"type":"agent_message","text":` + string(data) + `}}` + "\n"
}

func TestLogsEndToEndProvidersIdentitiesAndQuietDefaults(t *testing.T) {
	root := t.TempDir()
	workers := []executionlog.Worker{
		{Name: "api.one", Role: "coder.backend", Harness: "codex", RepositoryID: logsRepoA},
		{Name: "two", Role: "coder", Harness: "claude", RepositoryID: logsRepoB},
		{Name: "three", Role: "reviewer", Harness: "pi", RepositoryID: logsRepoA},
		{Name: "stopped", Role: "coder", Harness: "codex", RepositoryID: logsRepoB},
	}
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	old := logFixture(t, root, workers[0], at, codexMessage("history")+`{"type":"item.completed","item":{"type":"agent_message","text":"partial`)
	server, c, ctx, _ := startLogsDaemon(t, root, daemon.LogInventory{Workers: workers})
	status, err := c.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Capabilities.Logs.Stream || status.Capabilities.Logs.MaxRecordBytes != 1<<20 || status.Capabilities.Logs.ExternalSteer {
		t.Fatalf("discovery=%+v", status.Capabilities.Logs)
	}
	s := openLogs(t, c, ctx, LogsQuery{})
	ready := nextLog(t, s).Ready
	if ready == nil || ready.InstanceID != logsInstance || ready.Replay || ready.SchemaVersion != 1 || !reflect.DeepEqual(ready.Workers, []string{"api.one", "stopped", "three", "two"}) || !reflect.DeepEqual(ready.Kinds, []string{"message", "diagnostic"}) {
		t.Fatalf("ready=%+v", ready)
	}
	long := "**世界**\n" + strings.Repeat("界", harness.DisplayLimit*2) + "\nfinal"
	appendLog(t, old, " history\"}}\n"+`{"type":"item.completed","item":{"type":"reasoning","text":"private reasoning"}}`+"\n"+`{"type":"item.updated","item":{"type":"agent_message","text":"delta"}}`+"\n"+`{"type":"item.completed","item":{"type":"command_execution","command":"quiet command","exit_code":0,"aggregated_output":"dump"}}`+"\n"+codexMessage(long)+`{"type":"turn.completed"}`+"\n")
	logFixture(t, root, workers[1], at, `{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"tool_use","name":"Bash","input":{"command":"not exposed"}},{"type":"text","text":"claude\n世界"}]}}`+"\n"+`{"type":"result","subtype":"success","result":"duplicate"}`+"\n")
	logFixture(t, root, workers[2], at, `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"private delta"}}`+"\n"+`{"type":"tool_execution_end","toolName":"bash","result":{"content":[{"type":"text","text":"private dump"}]}}`+"\n"+`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"pi"}]}}`+"\n"+`{"type":"turn_end"}`+"\n")
	texts := map[string]string{}
	summaries := map[string]bool{}
	for range 6 {
		record := nextLog(t, s).Record
		if record == nil {
			t.Fatal("expected record")
		}
		if record.InstanceID != logsInstance || record.TicketID != "20261017-00001" || record.ExecutionID != filepath.Base(record.ExecutionID) || record.ObservedAt.Location() != time.UTC {
			t.Fatalf("identity=%+v", record)
		}
		var worker executionlog.Worker
		for _, w := range workers {
			if w.Name == record.Worker {
				worker = w
			}
		}
		if record.RepositoryID != worker.RepositoryID || record.Harness != worker.Harness {
			t.Fatalf("misattributed=%+v", record)
		}
		if record.Kind == "message" {
			texts[record.Worker] = record.Text
		} else if record.Kind == "diagnostic" && record.Code == "turn_completed" {
			summaries[record.Worker] = true
		} else {
			t.Fatalf("unexpected quiet record=%+v", record)
		}
	}
	if !reflect.DeepEqual(texts, map[string]string{"api.one": long, "two": "claude\n世界", "three": "pi"}) || len(summaries) != 3 {
		t.Fatalf("texts=%v summaries=%v", texts, summaries)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("shutdown Next=%v", err)
	}
}

func TestLogsEndToEndRepeatedFiltersEmptyIntersectionAndStructuredKinds(t *testing.T) {
	root := t.TempDir()
	workers := []executionlog.Worker{{Name: "one", Role: "coder", Harness: "codex", RepositoryID: logsRepoA}, {Name: "two", Role: "coder", Harness: "codex", RepositoryID: logsRepoB}}
	_, c, ctx, _ := startLogsDaemon(t, root, daemon.LogInventory{Workers: workers})
	idle := openLogs(t, c, ctx, LogsQuery{Workers: []string{"one"}, RepositoryIDs: []string{logsRepoB}})
	if ready := nextLog(t, idle).Ready; ready == nil || ready.Workers == nil || len(ready.Workers) != 0 {
		t.Fatalf("idle ready=%+v", ready)
	}
	idle.Close()
	s := openLogs(t, c, ctx, LogsQuery{Workers: []string{"two", "one", "one"}, RepositoryIDs: []string{logsRepoB, logsRepoB}, Kinds: []string{"file_change", "command", "command"}})
	ready := nextLog(t, s).Ready
	if ready == nil || !reflect.DeepEqual(ready.Workers, []string{"two"}) || !reflect.DeepEqual(ready.Kinds, []string{"command", "file_change"}) {
		t.Fatalf("filtered ready=%+v", ready)
	}
	at := time.Now()
	logFixture(t, root, workers[0], at, codexMessage("excluded repository"))
	logFixture(t, root, workers[1], at, codexMessage("excluded kind")+`{"type":"item.completed","item":{"type":"command_execution","command":"true\nprintf 世界","exit_code":0,"aggregated_output":"private dump"}}`+"\n"+`{"type":"item.completed","item":{"type":"file_change","changes":[{"kind":"update","path":"/provider/path/世界.go"}]}}`+"\n")
	command := nextLog(t, s).Record
	change := nextLog(t, s).Record
	if command == nil || command.Worker != "two" || command.Kind != "command" || command.Command != "true\nprintf 世界" || command.ExitCode == nil || *command.ExitCode != 0 || command.Phase != "completed" {
		t.Fatalf("command=%+v", command)
	}
	if change == nil || change.Worker != "two" || len(change.Changes) != 1 || change.Changes[0].Path != "/provider/path/世界.go" {
		t.Fatalf("change=%+v", change)
	}
	// Each reconnect establishes a new EOF baseline. A sentinel after reconnect
	// proves that neither prior commands nor messages are replayed.
	s.Close()
	s = openLogs(t, c, ctx, LogsQuery{Workers: []string{"two"}})
	nextLog(t, s)
	logFixture(t, root, workers[1], at.Add(time.Second), codexMessage("new connection"))
	if record := nextLog(t, s).Record; record == nil || record.Text != "new connection" {
		t.Fatalf("reconnect=%+v", record)
	}
}

func TestLogsEndToEndMalformedOversizedAndRolloverTails(t *testing.T) {
	root := t.TempDir()
	w := executionlog.Worker{Name: "one.dot", Role: "coder.backend", Harness: "codex", RepositoryID: logsRepoA}
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	old := logFixture(t, root, w, at, codexMessage("history"))
	_, c, ctx, _ := startLogsDaemon(t, root, daemon.LogInventory{Workers: []executionlog.Worker{w}})
	s := openLogs(t, c, ctx, LogsQuery{})
	nextLog(t, s)
	appendLog(t, old, `{"type":123,"secret":"raw content"}`+"\n"+codexMessage(strings.Repeat("x", daemon.MaxLogRecordBytes))+strings.Repeat("x", harness.MaxEventBytes+1)+"\n"+strings.TrimSuffix(codexMessage("old tail"), "\n"))
	newer := logFixture(t, root, w, at, codexMessage("collision next"))
	if newer.Path() == old.Path() {
		t.Fatal("expected collision suffix")
	}
	for _, want := range []string{"malformed_event", "record_too_large", "event_too_large", "old tail", "collision next"} {
		record := nextLog(t, s).Record
		if record == nil || record.Worker != w.Name || record.ExecutionID != filepath.Base(old.Path()) && record.ExecutionID != filepath.Base(newer.Path()) {
			t.Fatalf("record=%+v", record)
		}
		if record.Code != want && record.Text != want {
			t.Fatalf("record=%+v want=%s", record, want)
		}
		if strings.Contains(record.Text, "secret") {
			t.Fatalf("raw parse error=%+v", record)
		}
	}
}

func TestLogsEndToEndInventoryErrorSourceErrorAndClientCancellation(t *testing.T) {
	for _, mode := range []string{"inventory", "source", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			w := executionlog.Worker{Name: "one", Role: "coder", Harness: "codex", RepositoryID: logsRepoA}
			if mode != "source" {
				logFixture(t, root, w, time.Now(), codexMessage("history"))
			}
			_, c, ctx, setInventory := startLogsDaemon(t, root, daemon.LogInventory{Workers: []executionlog.Worker{w}})
			readCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			s := openLogs(t, c, readCtx, LogsQuery{})
			nextLog(t, s)
			switch mode {
			case "inventory":
				w.RepositoryID = logsRepoB
				setInventory(daemon.LogInventory{Workers: []executionlog.Worker{w}})
			case "source":
				// A missing logs directory is a supported idle source. Replacing it
				// with a regular file makes the next poll fail on every platform,
				// without moving a directory containing open writer/reader handles.
				if err := os.WriteFile(filepath.Join(root, "logs"), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "close":
				done := make(chan error, 1)
				go func() { _, err := s.Next(); done <- err }()
				s.Close()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("blocked read succeeded after Close")
					}
				case <-ctx.Done():
					t.Fatal("Close did not interrupt Next")
				}
				return
			}
			if mode == "cancel" {
				if _, err := s.Next(); err == nil {
					t.Fatal("canceled read succeeded")
				}
				return
			}
			failure := nextLog(t, s).Error
			code := "inventory_changed"
			if mode == "source" {
				code = "log_source_unavailable"
			}
			if failure == nil || failure.Code != code || strings.Contains(failure.Message, root) {
				t.Fatalf("failure=%+v", failure)
			}
			if mode == "source" && failure.Message != "managed log source is unavailable" {
				t.Fatalf("unsanitized source error=%+v", failure)
			}
			if _, err := s.Next(); !errors.Is(err, io.EOF) {
				t.Fatalf("terminal read=%v", err)
			}
		})
	}
}

func TestLogsEndToEndConcurrentSubscriptionLimitAndShutdown(t *testing.T) {
	server, c, ctx, _ := startLogsDaemon(t, t.TempDir(), daemon.LogInventory{})
	for range 16 {
		s := openLogs(t, c, ctx, LogsQuery{})
		if nextLog(t, s).Ready == nil {
			t.Fatal("missing ready")
		}
	}
	_, err := c.Logs(ctx, LogsQuery{})
	var failure *Error
	if !errors.As(err, &failure) || failure.StatusCode != 429 || failure.Code != "subscription_limit" {
		t.Fatalf("excess subscription=%v", err)
	}
	// Shutdown waits for every handler, proving all sixteen idle subscriptions
	// exit without relying on a poll delay or an event-broker notification.
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogsEndToEndToolNamesRemainStructured(t *testing.T) {
	for _, provider := range []struct{ name, event string }{
		{"claude", `{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Bash","input":{"command":"opaque"}}]}}`},
		{"pi", `{"type":"tool_execution_start","toolName":"Bash","args":{"command":"opaque"}}`},
	} {
		t.Run(provider.name, func(t *testing.T) {
			root := t.TempDir()
			w := executionlog.Worker{Name: "one", Role: "coder", Harness: provider.name, RepositoryID: logsRepoA}
			_, c, ctx, _ := startLogsDaemon(t, root, daemon.LogInventory{Workers: []executionlog.Worker{w}})
			s := openLogs(t, c, ctx, LogsQuery{Kinds: []string{"command"}})
			nextLog(t, s)
			logFixture(t, root, w, time.Now(), provider.event+"\n")
			r := nextLog(t, s).Record
			if r == nil || r.Kind != "command" || r.ToolName != "Bash" || r.Phase != "started" || r.Command != "" || r.Text != "" {
				t.Fatalf("tool record=%+v", r)
			}
		})
	}
}
