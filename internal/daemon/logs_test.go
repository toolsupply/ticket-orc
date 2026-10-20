package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness"
)

const logRepositoryB = "8d1268c4-6a64-4b9b-95c9-d5598a150e86"

func logTestInventory() LogInventory {
	return LogInventory{Workers: []executionlog.Worker{
		{Name: "api.one", Role: "coder.backend", Harness: "codex", RepositoryID: testRepositoryID},
		{Name: "two", Role: "reviewer", Harness: "pi", RepositoryID: logRepositoryB},
	}}
}

func TestLogsHTTPValidationAndDiscovery(t *testing.T) {
	inventory := logTestInventory()
	server, err := NewServer(Config{StateDir: t.TempDir(), InstanceID: capabilitiesTestInstanceID, EndpointKey: testEndpointKey, Logs: &LogSource{Inventory: func() LogInventory { return inventory }}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, last string
		status             int
		code               string
	}{
		{"GET", "/v1/logs", "", 404, "not_found"},
		{"GET", "/wrong/v1/logs", "", 404, "not_found"},
		{"POST", "/" + testEndpointKey + "/v1/logs", "", 405, "method_not_allowed"},
		{"GET", "/" + testEndpointKey + "/v1/logs", "cursor", 400, "resume_unsupported"},
		{"GET", "/" + testEndpointKey + "/v1/logs?path=private", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=../bad", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=%00", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=%zz", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=" + strings.Repeat("a", maxLogWorkerBytes+1), "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?kind=reasoning", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?kind=", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?repository_id=invalid", "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=unknown", "", 404, "worker_not_found"},
		{"GET", "/" + testEndpointKey + "/v1/logs?repository_id=00000000-0000-4000-8000-000000000001", "", 404, "repository_not_found"},
		{"GET", "/" + testEndpointKey + "/v1/logs?" + strings.Repeat("worker=two&", maxLogFilterValues+1), "", 400, "invalid_query"},
		{"GET", "/" + testEndpointKey + "/v1/logs?worker=" + strings.Repeat("a", maxLogQueryBytes), "", 400, "invalid_query"},
	} {
		t.Run(tc.path[:min(len(tc.path), 100)]+tc.last, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("Last-Event-ID", tc.last)
			out := httptest.NewRecorder()
			server.handler().ServeHTTP(out, req)
			if out.Code != tc.status || !strings.Contains(out.Body.String(), `"code":"`+tc.code+`"`) || out.Flushed {
				t.Fatalf("status=%d body=%s flushed=%v", out.Code, out.Body, out.Flushed)
			}
		})
	}
	out := httptest.NewRecorder()
	server.handler().ServeHTTP(out, httptest.NewRequest("GET", "/"+testEndpointKey+"/v1/status", nil))
	var status Status
	if err := json.Unmarshal(out.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(status.Capabilities.Logs)
	const want = `{"stream":true,"schema_version":1,"kinds":["message","diagnostic","command","file_change"],"default_kinds":["message","diagnostic"],"harnesses":["codex","claude","pi"],"replay":false,"max_record_bytes":1048576,"external_steer":false}`
	if string(data) != want || status.Protocol != 1 || strings.Contains(string(data), testEndpointKey) {
		t.Fatalf("capability=%s protocol=%d", data, status.Protocol)
	}
	server.config.Logs = nil
	if server.capabilities().Logs.Stream {
		t.Fatal("unwired logs advertised")
	}
	out = httptest.NewRecorder()
	server.handleLogs(out, httptest.NewRequest("GET", "/v1/logs", nil))
	if out.Code != 422 || !strings.Contains(out.Body.String(), "capability_unavailable") {
		t.Fatalf("unwired=%d %s", out.Code, out.Body)
	}
}

func TestLogFiltersRepeatedValuesAndIntersections(t *testing.T) {
	f, err := parseLogFilters("worker=two&worker=api.one&worker=two&repository_id=" + testRepositoryID + "&kind=command&kind=message&kind=command")
	if err != nil {
		t.Fatal(err)
	}
	names, defs, code := f.selectWorkers(logTestInventory())
	if code != "" || !reflect.DeepEqual(names, []string{"api.one"}) || len(defs) != 2 || !reflect.DeepEqual(f.kinds, []string{"message", "command"}) {
		t.Fatalf("names=%v defs=%v kinds=%v code=%s", names, defs, f.kinds, code)
	}
	f.workers = []string{"two"}
	names, _, code = f.selectWorkers(logTestInventory())
	if code != "" || names == nil || len(names) != 0 {
		t.Fatalf("empty intersection=%v code=%s", names, code)
	}
}

// The in-process transport reports completed flushes, rather than relying on
// elapsed time to infer that a poll/write/heartbeat has finished.
type logProbeWriter struct {
	*httptest.ResponseRecorder
	frames   chan string
	slow     bool
	deadline time.Time
}

func (w *logProbeWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}
func (w *logProbeWriter) Write(data []byte) (int, error) {
	if w.slow && strings.HasPrefix(string(data), ": heartbeat") {
		w.slow = false
		<-time.NewTimer(time.Until(w.deadline)).C
		return 0, os.ErrDeadlineExceeded
	}
	return w.ResponseRecorder.Write(data)
}
func (w *logProbeWriter) FlushError() error {
	w.ResponseRecorder.Flush()
	w.frames <- w.Body.String()
	w.Body.Reset()
	return nil
}

func TestLogsInventoryHeartbeatLimitsAndCancellation(t *testing.T) {
	for _, mode := range []string{"selected identity", "selected role", "selected harness", "all membership", "unrelated worker", "empty intersection", "ambiguity outside filter", "repository removed", "heartbeat", "slow consumer"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				inventory := logTestInventory()
				var inventoryMu sync.Mutex
				inventory.RepositoryIDs = []string{"00000000-0000-4000-8000-000000000001"}
				server, err := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, EventHeartbeat: time.Second, Logs: &LogSource{LocalDir: t.TempDir(), Inventory: func() LogInventory {
					inventoryMu.Lock()
					defer inventoryMu.Unlock()
					return LogInventory{Workers: append([]executionlog.Worker(nil), inventory.Workers...), RepositoryIDs: append([]string(nil), inventory.RepositoryIDs...)}
				}}})
				if err != nil {
					t.Fatal(err)
				}
				server.lifecycle = context.Background()
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Minute):
						t.Error("handler did not exit")
					}
				})
				query := "?worker=api.one"
				if mode == "all membership" {
					query = ""
				}
				if mode == "empty intersection" {
					query += "&repository_id=" + logRepositoryB
				}
				if mode == "repository removed" {
					query += "&repository_id=" + inventory.RepositoryIDs[0]
				}
				writer := &logProbeWriter{ResponseRecorder: httptest.NewRecorder(), frames: make(chan string, 8), slow: mode == "slow consumer"}
				go func() {
					defer close(done)
					server.handleLogs(writer, httptest.NewRequest("GET", "/v1/logs"+query, nil).WithContext(ctx))
				}()
				ready := <-writer.frames
				if !strings.HasPrefix(ready, "event: logs.ready\n") || strings.Contains(ready, "id:") {
					t.Fatalf("ready=%s", ready)
				}
				var readyDTO LogsReady
				readyJSON := strings.TrimSuffix(strings.TrimPrefix(ready, "event: logs.ready\ndata: "), "\n\n")
				if err := json.Unmarshal([]byte(readyJSON), &readyDTO); err != nil {
					t.Fatal(err)
				}
				wantWorkers := []string{"api.one"}
				if mode == "all membership" {
					wantWorkers = []string{"api.one", "two"}
				} else if mode == "empty intersection" || mode == "repository removed" {
					wantWorkers = []string{}
				}
				wantReady := LogsReady{SchemaVersion: 1, Workers: wantWorkers, Kinds: []string{"message", "diagnostic"}}
				if !reflect.DeepEqual(readyDTO, wantReady) {
					t.Fatalf("ready=%+v want=%+v", readyDTO, wantReady)
				}
				synctest.Wait()
				server.mu.Lock()
				server.logSubscriptions = maxLogSubscriptions
				server.mu.Unlock()
				out := httptest.NewRecorder()
				server.handleLogs(out, httptest.NewRequest("GET", "/v1/logs", nil))
				if out.Code != 429 {
					t.Fatalf("limit=%d %s", out.Code, out.Body)
				}
				server.mu.Lock()
				server.logSubscriptions = 1
				server.mu.Unlock()
				want := "event: logs.error\n"
				inventoryMu.Lock()
				switch mode {
				case "selected identity":
					inventory.Workers[0].RepositoryID = logRepositoryB
				case "selected role":
					inventory.Workers[0].Role = "reviewer"
				case "selected harness":
					inventory.Workers[0].Harness = "claude"
				case "all membership":
					inventory.Workers = inventory.Workers[:1]
				case "unrelated worker":
					inventory.Workers[1].Harness = "claude"
					want = ": heartbeat\n\n"
				case "ambiguity outside filter":
					inventory.Workers = append(inventory.Workers, executionlog.Worker{Name: "pi.other", Role: "reviewer", Harness: "codex", RepositoryID: logRepositoryB}, executionlog.Worker{Name: "other", Role: "reviewer.codex", Harness: "pi", RepositoryID: testRepositoryID})
				case "repository removed":
					inventory.RepositoryIDs = nil
				case "empty intersection", "heartbeat":
					want = ": heartbeat\n\n"
				case "slow consumer":
				}
				inventoryMu.Unlock()
				frame := <-writer.frames
				if !strings.HasPrefix(frame, want) {
					t.Fatalf("next=%q want %q", frame, want)
				}
				if want == "event: logs.error\n" {
					code := "inventory_changed"
					if mode == "slow consumer" {
						code = "slow_consumer"
					}
					var failure LogsError
					failureJSON := strings.TrimSuffix(strings.TrimPrefix(frame, "event: logs.error\ndata: "), "\n\n")
					if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil || failure.Code != code || failure.Message == "" {
						t.Fatalf("error=%s err=%v", frame, err)
					}
					<-done
				} else {
					cancel()
					<-done
				}
				if server.logSubscriptions != 0 {
					t.Fatalf("retained subscriptions=%d", server.logSubscriptions)
				}
			})
		})
	}
}

func TestLogJSONBoundsAndSemanticFields(t *testing.T) {
	zero := 0
	observation := executionlog.Observation{Worker: "api.one", RepositoryID: testRepositoryID, Harness: "codex", TicketID: "20261017-00001", ExecutionID: "20261017-00001.coder.codex.api.one.20261017T000000.000000000Z.jsonl", ObservedAt: time.Date(2026, 10, 17, 1, 2, 3, 456, time.FixedZone("offset", 3600)), Record: harness.LogRecord{Kind: "command", Phase: "completed", Command: "true\n世界", ExitCode: &zero}}
	data, err := marshalLogRecord("instance", observation)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"exit_code":0`, `"command":"true\n世界"`, `"observed_at":"2026-10-17T00:02:03.000000456Z"`, `"kind":"command"`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("JSON missing %s: %s", want, data)
		}
	}
	observation.Record = harness.LogRecord{Kind: "message", Text: strings.Repeat("<", MaxLogRecordBytes/6)} // JSON escaping pushes the envelope over the cap.
	data, err = marshalLogRecord("instance", observation)
	if err != nil || len(data) > MaxLogRecordBytes || !strings.Contains(string(data), `"code":"record_too_large"`) || !strings.Contains(string(data), `"execution_id":"`+observation.ExecutionID+`"`) {
		t.Fatalf("oversize=%d err=%v data=%.500s", len(data), err, data)
	}
	observation.Record = harness.LogRecord{Kind: "diagnostic", Code: "invalid_event", Text: "/private/path raw secret"}
	data, err = marshalLogRecord("instance", observation)
	if err != nil || strings.Contains(string(data), "secret") || !strings.Contains(string(data), `"code":"malformed_event"`) {
		t.Fatalf("malformed=%s err=%v", data, err)
	}
}

func TestLogSourceErrorsBeforeHeadersAreSanitized(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(root+"/logs", []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	server, _ := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, Logs: &LogSource{LocalDir: root, Inventory: logTestInventory}})
	w := &logProbeWriter{ResponseRecorder: httptest.NewRecorder(), frames: make(chan string, 1)}
	server.handleLogs(w, httptest.NewRequest("GET", "/v1/logs", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), root) || w.Flushed {
		t.Fatalf("source error=%d %s", w.Code, w.Body)
	}
	if err := executionlog.ValidateCatalog(logTestInventory().Workers); err != nil {
		t.Fatal(err)
	}
}

func TestLogFilterProtocolBoundsAndKinds(t *testing.T) {
	// Use protocol limits as test-owned expectations, including acceptance at
	// each boundary and rejection immediately above it.
	worker := strings.Repeat("a", 128)
	query := strings.Repeat("worker="+worker+"&", 120) + "worker=" + strings.Repeat("b", 57)
	if len(query) != 16384 {
		t.Fatalf("query fixture has %d bytes", len(query))
	}
	for _, tc := range []struct {
		name, query string
		valid       bool
	}{
		{"worker at limit", "worker=" + worker, true},
		{"worker above limit", "worker=" + worker + "a", false},
		{"query at limit", query, true},
		{"query above limit", query + "&", false},
		{"values at limit", strings.Repeat("worker=a&", 255) + "kind=message", true},
		{"values above limit across fields", strings.Repeat("worker=a&", 255) + "kind=message&repository_id=" + testRepositoryID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseLogFilters(tc.query)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}
		})
	}
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{"message", "diagnostic"}},
		{"kind=file_change&kind=command&kind=diagnostic&kind=message", []string{"message", "diagnostic", "command", "file_change"}},
	} {
		filters, err := parseLogFilters(tc.query)
		if err != nil || !reflect.DeepEqual(filters.kinds, tc.want) {
			t.Fatalf("query=%q kinds=%v err=%v", tc.query, filters.kinds, err)
		}
	}
}

func TestLogRecordJSONByteBoundary(t *testing.T) {
	observation := executionlog.Observation{Record: harness.LogRecord{Kind: "message", Text: "x"}}
	base, err := marshalLogRecord("instance", observation)
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []int{0, 1} {
		observation.Record.Text = strings.Repeat("x", 1048576-len(base)+1+extra)
		data, err := marshalLogRecord("instance", observation)
		if err != nil {
			t.Fatal(err)
		}
		var record LogRecord
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		if extra == 0 {
			if len(data) != 1048576 || record.Kind != "message" || record.Text != observation.Record.Text {
				t.Fatalf("record at limit: bytes=%d kind=%s", len(data), record.Kind)
			}
		} else if len(data) > 1048576 || record.Kind != "diagnostic" || record.Code != "record_too_large" {
			t.Fatalf("record above limit: bytes=%d record=%+v", len(data), record)
		}
	}
}

func TestLogsHTTPNonreadingConsumerExitsOnWriteDeadline(t *testing.T) {
	root := t.TempDir()
	worker := logTestInventory().Workers[0]
	raw, err := harness.OpenRawLog("Codex", root, "20261017-00001", worker.Role, time.Now(), worker.Harness, worker.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	server, err := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, Logs: &LogSource{LocalDir: root, Inventory: func() LogInventory { return LogInventory{Workers: []executionlog.Worker{worker}} }}})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	done := make(chan struct{})
	transport := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); server.handler().ServeHTTP(w, r) }))
	t.Cleanup(transport.Close)
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(transport.URL, "http://"), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	if tcp, ok := connection.(*net.TCPConn); ok {
		if err := tcp.SetReadBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}
	if err := connection.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(connection, "GET /%s/v1/logs HTTP/1.1\r\nHost: localhost\r\n\r\n", testEndpointKey); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("response=%v", response)
	}
	reader := bufio.NewReader(response.Body)
	ready, err := reader.ReadString('\n')
	if err != nil || ready != "event: logs.ready\n" {
		t.Fatalf("ready=%q err=%v", ready, err)
	}
	// Stop reading after readiness. More output than the TCP send/receive buffers
	// can accept must terminate this actual HTTP handler via its write deadline.
	encoded, _ := json.Marshal(strings.Repeat("x", 900<<10))
	line := []byte(`{"type":"item.completed","item":{"type":"agent_message","text":` + string(encoded) + "}}\n")
	for range 12 {
		if _, err := raw.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("nonreading consumer retained the HTTP handler")
	}
	if server.logSubscriptions != 0 {
		t.Fatalf("retained subscriptions=%d", server.logSubscriptions)
	}
}

type canceledLogWriter struct {
	*logProbeWriter
	blocked chan struct{}
	expired chan struct{}
	once    sync.Once
}

func (w *canceledLogWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		w.once.Do(func() { close(w.expired) })
	}
	return nil
}
func (w *canceledLogWriter) Write(data []byte) (int, error) {
	if strings.HasPrefix(string(data), ": heartbeat") {
		close(w.blocked)
		<-w.expired
		return 0, os.ErrDeadlineExceeded
	}
	return w.logProbeWriter.Write(data)
}

func TestLogsShutdownInterruptsBlockedWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, EventHeartbeat: time.Second, Logs: &LogSource{LocalDir: t.TempDir(), Inventory: logTestInventory}})
		lifecycle, cancel := context.WithCancel(context.Background())
		server.lifecycle = lifecycle
		done := make(chan struct{})
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Minute):
				t.Error("cancellation retained writer")
			}
		})
		writer := &canceledLogWriter{logProbeWriter: &logProbeWriter{ResponseRecorder: httptest.NewRecorder(), frames: make(chan string, 1)}, blocked: make(chan struct{}), expired: make(chan struct{})}
		go func() { defer close(done); server.handleLogs(writer, httptest.NewRequest("GET", "/v1/logs", nil)) }()
		<-writer.frames
		<-writer.blocked
		cancel()
		<-done
		if server.logSubscriptions != 0 {
			t.Fatalf("retained subscriptions=%d", server.logSubscriptions)
		}
	})
}
