package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

type countingConsoleWriter struct {
	buffer bytes.Buffer
	writes int
}

func (writer *countingConsoleWriter) Write(data []byte) (int, error) {
	writer.writes++
	return writer.buffer.Write(data)
}

func (writer *countingConsoleWriter) String() string { return writer.buffer.String() }

type consoleWatchStartWriter struct {
	bytes.Buffer
	started    chan struct{}
	stopped    chan struct{}
	prompted   chan struct{}
	detached   chan struct{}
	startMatch string
	matched    chan struct{}
	match      string
	once       sync.Once
	stopOnce   sync.Once
	detachOnce sync.Once
	matchOnce  sync.Once
}

func (writer *consoleWatchStartWriter) Write(data []byte) (int, error) {
	text := string(data)
	n, err := writer.Buffer.Write(data)
	if writer.started != nil && (strings.Contains(text, "Watching activity.") || writer.startMatch != "" && strings.Contains(text, writer.startMatch)) {
		writer.once.Do(func() { close(writer.started) })
	}
	if writer.stopped != nil && strings.Contains(text, "watch stopped") {
		writer.stopOnce.Do(func() { close(writer.stopped) })
	}
	if writer.prompted != nil && strings.Contains(text, consolePrompt) {
		select {
		case writer.prompted <- struct{}{}:
		default:
		}
	}
	if writer.detached != nil && strings.Contains(text, "detached") {
		writer.detachOnce.Do(func() { close(writer.detached) })
	}
	if writer.matched != nil && writer.match != "" && strings.Contains(text, writer.match) {
		writer.matchOnce.Do(func() { close(writer.matched) })
	}
	return n, err
}

func (writer *consoleWatchStartWriter) WriteString(text string) (int, error) {
	return writer.Write([]byte(text))
}

func waitConsoleMilestone(t *testing.T, milestone <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-milestone:
	case <-time.After(2 * time.Second):
		t.Fatalf("console did not reach %s", description)
	}
}

func nextConsoleTestEvent(t *testing.T, events <-chan daemon.Event) daemon.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for console event")
		return daemon.Event{}
	}
}

type consoleTrackedEventBody struct {
	io.ReadCloser
	closed chan struct{}
	once   sync.Once
}

func (body *consoleTrackedEventBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return body.ReadCloser.Close()
}

type consoleEventTrackingTransport struct {
	base   http.RoundTripper
	bodies chan<- *consoleTrackedEventBody
}

func (transport consoleEventTrackingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil || !strings.HasSuffix(request.URL.Path, "/v1/events") {
		return response, err
	}
	body := &consoleTrackedEventBody{ReadCloser: response.Body, closed: make(chan struct{})}
	response.Body = body
	transport.bodies <- body
	return response, nil
}

func nextTrackedConsoleBody(t *testing.T, bodies <-chan *consoleTrackedEventBody) *consoleTrackedEventBody {
	t.Helper()
	select {
	case body := <-bodies:
		return body
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event response body")
		return nil
	}
}

func newConsoleTestServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	token, err := daemon.GenerateEndpointKey()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := "/" + token
		if !strings.HasPrefix(r.URL.Path, prefix+"/") {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		handler.ServeHTTP(w, r)
	}))
	return server, writeConsoleEndpoint(t, server.URL, token)
}

func writeConsoleEndpoint(t *testing.T, serverURL, token string) string {
	t.Helper()
	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint := daemon.Endpoint{Version: 1, Protocol: 1, PID: os.Getpid(), URL: serverURL, EndpointKey: token}
	data, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "endpoint.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return stateDir
}
