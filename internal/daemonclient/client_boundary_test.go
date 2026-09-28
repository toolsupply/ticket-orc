package daemonclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const clientEndpointKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type controlRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip controlRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestClientDaemonPauseAndResumeUseCapabilityRoutes(t *testing.T) {
	client := &Client{
		endpoint: daemon.Endpoint{URL: "http://127.0.0.1:1234", EndpointKey: clientEndpointKey},
		http: &http.Client{Transport: controlRoundTripper(func(request *http.Request) (*http.Response, error) {
			mode := "paused"
			var targets string
			switch {
			case strings.HasSuffix(request.URL.Path, "/v1/resume"):
				mode = "running"
			case strings.HasSuffix(request.URL.Path, "/v1/abort"):
				mode = "aborted"
				targets = `,"targets":[{"kind":"steer","actor":"coder","outcome":"request_failed","code":"transport_unavailable"}]`
			}
			if request.Method != http.MethodPost || !strings.HasPrefix(request.URL.Path, "/"+clientEndpointKey+"/v1/") {
				t.Errorf("control request method=%s path=%s", request.Method, request.URL.Path)
			}
			body := `{"mode":"` + mode + `","mutation_applied":true` + targets + `}`
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		})},
	}
	for _, test := range []struct {
		operation func(context.Context) (daemon.DaemonControlResult, error)
		mode      string
		applied   bool
	}{
		{operation: client.Pause, mode: "paused", applied: true},
		{operation: client.Resume, mode: "running", applied: true},
		{operation: client.Abort, mode: "aborted", applied: true},
	} {
		result, err := test.operation(context.Background())
		if err != nil || result.Mode != test.mode || result.Applied != test.applied {
			t.Fatalf("control result=%#v err=%v want mode=%s", result, err, test.mode)
		}
		if test.mode == "aborted" && (len(result.Targets) != 1 || result.Targets[0].Outcome != "request_failed" || result.Targets[0].Actor != "coder") {
			t.Fatalf("typed abort target result = %#v", result.Targets)
		}
	}
}

func TestClientResumePreservesDurableModeOnReconciliationError(t *testing.T) {
	client := &Client{
		endpoint: daemon.Endpoint{URL: "http://127.0.0.1:1234", EndpointKey: clientEndpointKey},
		http: &http.Client{Transport: controlRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/v1/resume") {
				t.Errorf("request method=%s path=%s", request.Method, request.URL.Path)
			}
			body := `{"error":{"code":"daemon_reconciliation_failed","message":"daemon resumed but worker reconciliation failed","mutation_applied":true,"daemon_control":{"mode":"running","mutation_applied":true}}}`
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		})},
	}
	result, err := client.Resume(context.Background())
	var clientErr *Error
	if !errors.As(err, &clientErr) || result.Mode != "running" || !result.Applied {
		t.Fatalf("resume result=%#v err=%v", result, err)
	}
	if clientErr.DaemonControl == nil || clientErr.DaemonControl.Mode != "running" || !clientErr.DaemonControl.Applied {
		t.Fatalf("typed error lost durable daemon result: %#v", clientErr)
	}
}

func TestClientClassifiesDiscoveryProtocolAndTransportFailures(t *testing.T) {
	missingState := t.TempDir()
	if _, err := NewWithEndpoint(missingState, "", ""); !hasClientErrorKind(err, ErrorDiscovery) || !strings.Contains(err.Error(), missingState) || !strings.Contains(err.Error(), daemon.EndpointPath(missingState)) {
		t.Fatalf("missing endpoint error=%v, want discovery with selected instance and endpoint path", err)
	}

	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	endpointPath := filepath.Join(runDir, "endpoint.json")
	if err := os.WriteFile(endpointPath, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWithEndpoint(stateDir, "", ""); !hasClientErrorKind(err, ErrorDiscovery) || !strings.Contains(err.Error(), stateDir) || !strings.Contains(err.Error(), endpointPath) {
		t.Fatalf("malformed endpoint error=%v, want discovery with selected instance and endpoint path", err)
	}
	writeClientEndpoint(t, stateDir, "http://127.0.0.1:1", 2, 1, "token")
	if _, err := NewWithEndpoint(stateDir, "", ""); !hasClientErrorKind(err, ErrorProtocol) {
		t.Fatalf("incompatible endpoint error=%v, want protocol", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := NewWithEndpoint(stateDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	server.Close()
	if _, err := client.Status(context.Background()); !hasClientErrorKind(err, ErrorTransport) || !strings.Contains(err.Error(), stateDir) || !strings.Contains(err.Error(), endpointPath) {
		t.Fatalf("stale endpoint error=%v, want transport with selected instance and endpoint path", err)
	}
}

func TestClientPropagatesCanceledAndDeadlineContexts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	stateDir := t.TempDir()
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := New(stateDir)
	if err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Status(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error=%v, want context canceled", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Status(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline request error=%v, want deadline exceeded", err)
	}
}

func TestClientRejectsMalformedOversizedAndMultipleResponses(t *testing.T) {
	mode := "malformed"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch mode {
		case "malformed":
			_, _ = w.Write([]byte("not json"))
		case "oversized":
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
		case "multiple":
			_, _ = w.Write([]byte(`{"version":"one"}{"version":"two"}`))
		}
	}))
	defer server.Close()
	stateDir := t.TempDir()
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"malformed", "oversized", "multiple"} {
		mode = want
		if _, err := client.Status(context.Background()); !hasClientErrorKind(err, ErrorProtocol) {
			t.Fatalf("mode=%s error=%v, want protocol", mode, err)
		}
	}
}

func TestClientPreservesGroupPartialAndMapsReloadShutdownErrors(t *testing.T) {
	server := newClientTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/groups/backend/start":
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": "group_partial_failure", "message": "one worker failed", "mutation_applied": true},
				"group": daemon.GroupResult{Group: "backend", Results: []daemon.MutationResult{{Worker: "one", State: "running", Applied: true}, {Worker: "two", State: "stopped"}}},
			})
		case "/v1/config/reload":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":"reload_rejected","message":"candidate is invalid","mutation_applied":false}}`))
		case "/v1/shutdown":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"mutation_timeout","message":"shutdown timed out","mutation_applied":false}}`))
		case "/v1/status":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"safe failure"},"prompt":"secret prompt","token":"secret-token"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	stateDir := t.TempDir()
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	group, err := client.Group(context.Background(), "backend", "start")
	var clientErr *Error
	if !errors.As(err, &clientErr) || clientErr.Code != "group_partial_failure" || clientErr.Group == nil || group.Group != "backend" || len(group.Results) != 2 || !group.Results[0].Applied || group.Results[1].Applied {
		t.Fatalf("group=%#v err=%v (%#v)", group, err, clientErr)
	}
	if _, err := client.Reload(context.Background()); !errors.As(err, &clientErr) || clientErr.Code != "reload_rejected" || clientErr.Kind != ErrorApplication {
		t.Fatalf("reload error=%v (%#v)", err, clientErr)
	}
	if err := client.Shutdown(context.Background()); !errors.As(err, &clientErr) || clientErr.Code != "mutation_timeout" || clientErr.Kind != ErrorApplication {
		t.Fatalf("shutdown error=%v (%#v)", err, clientErr)
	}
	if _, err := client.Status(context.Background()); err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "prompt") || strings.Contains(err.Error(), "token") {
		t.Fatalf("status privacy error=%v", err)
	}
}

func hasClientErrorKind(err error, want ErrorKind) bool {
	var clientErr *Error
	return errors.As(err, &clientErr) && clientErr.Kind == want
}

func writeClientEndpoint(t *testing.T, stateDir, endpointURL string, version, protocol int, token string) {
	t.Helper()
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	_ = token
	data, err := json.Marshal(daemon.Endpoint{Version: version, Protocol: protocol, PID: os.Getpid(), URL: endpointURL, EndpointKey: clientEndpointKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "endpoint.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func newClientTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := "/" + clientEndpointKey
		if !strings.HasPrefix(r.URL.Path, prefix+"/") {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		handler.ServeHTTP(w, r)
	}))
}
