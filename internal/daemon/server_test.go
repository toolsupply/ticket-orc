package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testEndpointKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func startTestServer(t *testing.T) (*Server, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Version:     "test",
		Status: func() Status {
			return Status{Workers: []WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", State: "running"}}}
		},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdown, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdown)
		cancel()
	})
	return server, cancel
}

func TestNewServerValidatesListenConfiguration(t *testing.T) {
	for name, config := range map[string]Config{
		"hostname":     {StateDir: t.TempDir(), ListenAddress: "localhost"},
		"privileged":   {StateDir: t.TempDir(), Port: 80},
		"out-of-range": {StateDir: t.TempDir(), Port: 65536},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewServer(config); err == nil {
				t.Fatal("invalid listen configuration accepted")
			}
		})
	}
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), ListenAddress: "127.0.0.2", Port: 43123})
	if err != nil {
		t.Fatal(err)
	}
	if server.config.ListenAddress != "127.0.0.2" || server.config.Port != 43123 {
		t.Fatalf("normalized listen config = %#v", server.config)
	}
	interfaceServer, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), ListenAddress: "192.0.2.1"})
	if err != nil || interfaceServer.config.ListenAddress != "192.0.2.1" {
		t.Fatalf("interface listener config = %#v, err=%v", interfaceServer, err)
	}
	for address, want := range map[string]string{
		"0.0.0.0":     "0.0.0.0",
		"192.0.2.1":   "192.0.2.1",
		"::":          "::",
		"::1":         "::1",
		"2001:db8::1": "2001:db8::1",
	} {
		got, err := NormalizeListenAddress(address)
		if err != nil || got != want {
			t.Errorf("NormalizeListenAddress(%q) = %q, %v; want %q", address, got, err, want)
		}
	}
}

func TestClientAddressForListenerSeparatesWildcardBindFromClientURL(t *testing.T) {
	for bind, want := range map[string]string{
		"0.0.0.0":   "127.0.0.1",
		"::":        "::1",
		"127.0.0.2": "127.0.0.2",
		"::1":       "::1",
		"192.0.2.1": "192.0.2.1",
	} {
		if got := clientAddressForListener(bind); got != want {
			t.Errorf("clientAddressForListener(%q) = %q, want %q", bind, got, want)
		}
	}
}

func TestServerStartFailsOnRequestedPortCollision(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local socket unavailable: %v", err)
	}
	defer reserved.Close()
	port := reserved.Addr().(*net.TCPAddr).Port
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), ListenAddress: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Start(context.Background())
	wantAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	var listenErr *net.OpError
	if err == nil || !errors.As(err, &listenErr) || listenErr.Op != "listen" || listenErr.Addr == nil || listenErr.Addr.String() != wantAddress {
		t.Fatalf("collision error = %v", err)
	}
	if _, statErr := os.Stat(EndpointPath(server.config.StateDir)); !os.IsNotExist(statErr) {
		t.Fatalf("failed listener published endpoint: stat error=%v", statErr)
	}
}

func TestServerBindsAndPublishesExplicitPort43123(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:43123")
	if err != nil {
		t.Skipf("explicit acceptance port is unavailable: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), ListenAddress: "127.0.0.1", Port: 43123})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	endpoint, err := ReadEndpoint(server.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Port != 43123 || endpoint.URL != "http://127.0.0.1:43123" {
		t.Fatalf("published endpoint = %#v, want explicit port 43123", endpoint)
	}
}

func TestServerStartRetainsFirstFailure(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(statePath, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: statePath})
	if err != nil {
		t.Fatal(err)
	}

	firstErr := server.Start(context.Background())
	if firstErr == nil || !strings.Contains(firstErr.Error(), "create daemon runtime directory") {
		t.Fatalf("first Start error = %v, want runtime directory failure", firstErr)
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	secondErr := server.Start(context.Background())
	if secondErr != firstErr {
		t.Fatalf("second Start error = %v, want retained first error %v", secondErr, firstErr)
	}
}

func TestServerPublishesAuthenticatedStatusAndCleansUp(t *testing.T) {
	server, _ := startTestServer(t)
	endpoint := server.Endpoint()
	if endpoint.Version != 1 || endpoint.Protocol != 1 || endpoint.PID < 1 || endpoint.ListenAddress != "127.0.0.1" || endpoint.Port < 1 || len(endpoint.EndpointKey) != 43 {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	if !strings.HasPrefix(endpoint.URL, "http://127.0.0.1:") {
		t.Fatalf("endpoint URL = %q", endpoint.URL)
	}
	loaded, err := ReadEndpoint(server.config.StateDir)
	if err != nil || loaded != endpoint {
		t.Fatalf("ReadEndpoint = %#v, %v; want %#v", loaded, err, endpoint)
	}
	info, err := os.Stat(EndpointPath(server.config.StateDir))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("endpoint permissions = %o, want 600", info.Mode().Perm())
	}

	request, err := http.NewRequest(http.MethodGet, endpoint.CapabilityURL()+"/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("status response = %d headers=%v", response.StatusCode, response.Header)
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var status Status
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Version != "test" || status.Protocol != 1 || len(status.Workers) != 1 || status.Workers[0].Name != "coder" {
		t.Fatalf("status = %#v", status)
	}
	if strings.Contains(string(raw), endpoint.EndpointKey) {
		t.Fatal("status response leaked bearer token")
	}

	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(EndpointPath(server.config.StateDir)); !os.IsNotExist(err) {
		t.Fatalf("endpoint after shutdown: %v", err)
	}
}

func TestDefaultServersPublishDistinctDynamicPortsPerInstance(t *testing.T) {
	start := func(name string) *Server {
		t.Helper()
		server, err := NewServer(Config{
			EndpointKey: testEndpointKey,
			StateDir:    t.TempDir(),
			Version:     name,
			Status: func() Status {
				return Status{Workers: []WorkerStatus{{Name: name}}}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := server.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				t.Errorf("shutdown %s: %v", name, err)
			}
		})
		return server
	}
	first := start("first-instance")
	second := start("second-instance")
	firstEndpoint, err := ReadEndpoint(first.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	secondEndpoint, err := ReadEndpoint(second.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if first.config.Port != 0 || second.config.Port != 0 || firstEndpoint.Port == 0 || secondEndpoint.Port == 0 || firstEndpoint.Port == secondEndpoint.Port {
		t.Fatalf("default listeners did not publish distinct OS-selected ports: config=(%d,%d) endpoints=(%#v,%#v)", first.config.Port, second.config.Port, firstEndpoint, secondEndpoint)
	}
	for server, want := range map[*Server]string{first: "first-instance", second: "second-instance"} {
		request, err := http.NewRequest(http.MethodGet, server.Endpoint().CapabilityURL()+"/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("request to %s: %v", want, err)
		}
		var status Status
		decodeErr := json.NewDecoder(response.Body).Decode(&status)
		closeErr := response.Body.Close()
		if decodeErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || len(status.Workers) != 1 || status.Workers[0].Name != want {
			t.Fatalf("status from %s: code=%d status=%#v decode=%v close=%v", want, response.StatusCode, status, decodeErr, closeErr)
		}
	}
}

func TestServerAuthorizesCapabilityWithoutMatchingHost(t *testing.T) {
	server, _ := startTestServer(t)
	endpoint := server.Endpoint()
	for _, test := range []struct {
		name       string
		authority  string
		path       string
		withKey    bool
		wantStatus int
	}{
		{name: "missing key", authority: strings.TrimPrefix(endpoint.URL, "http://"), path: "/v1/status", wantStatus: http.StatusNotFound},
		{name: "wrong key", authority: strings.TrimPrefix(endpoint.URL, "http://"), path: "/wrong/v1/status", wantStatus: http.StatusNotFound},
		{name: "missing key with alternate host", authority: "container.internal:8443", path: "/v1/status", wantStatus: http.StatusNotFound},
		{name: "wrong key with alternate host", authority: "container.internal:8443", path: "/wrong/v1/status", wantStatus: http.StatusNotFound},
		{name: "valid key with alternate host", authority: "container.internal:8443", path: "/v1/status", withKey: true, wantStatus: http.StatusOK},
		{name: "namespace root", authority: strings.TrimPrefix(endpoint.URL, "http://"), path: "/v1", withKey: true, wantStatus: http.StatusNotFound},
		{name: "events missing key", authority: strings.TrimPrefix(endpoint.URL, "http://"), path: "/v1/events", wantStatus: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := "/v1/status"
			if test.path != "" {
				path = test.path
			}
			base := endpoint.URL
			if test.withKey {
				base = endpoint.CapabilityURL()
			}
			request, err := http.NewRequest(http.MethodGet, base+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Host = test.authority
			response, err := http.DefaultTransport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
		})
	}
}

func TestServerPublishesAuthenticatedOrderedSSEEventsAndHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), Version: "test", EventHeartbeat: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	endpoint := server.Endpoint()
	request, err := http.NewRequest(http.MethodGet, endpoint.CapabilityURL()+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("events response status=%d headers=%v", response.StatusCode, response.Header)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != ": connected\n" {
		t.Fatalf("connected line=%q err=%v", line, err)
	}
	server.PublishEvent(Event{Type: "worker.state", Worker: "coder", State: "running"})
	var eventLines []string
	for len(eventLines) < 3 {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "id: ") || strings.HasPrefix(line, "event: ") || strings.HasPrefix(line, "data: ") || strings.HasPrefix(line, ": heartbeat") {
			eventLines = append(eventLines, strings.TrimSpace(line))
		}
	}
	if !strings.Contains(strings.Join(eventLines, "\n"), "id: 1") || !strings.Contains(strings.Join(eventLines, "\n"), "worker.state") {
		t.Fatalf("SSE event lines = %#v", eventLines)
	}
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\n" {
			break
		}
	}
	server.PublishEvent(Event{Type: "worker.state", Worker: "coder", State: "running"})
	var secondLines []string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		secondLines = append(secondLines, strings.TrimSpace(line))
		if line == "\n" {
			break
		}
	}
	secondEvent := strings.Join(secondLines, "\n")
	if !strings.Contains(secondEvent, "id: 2") || !strings.Contains(secondEvent, `"state":"running"`) {
		t.Fatalf("second SSE event = %q", secondEvent)
	}
	for _, forbidden := range []string{"steer prompt", "queue target", endpoint.EndpointKey, "raw transport error"} {
		if strings.Contains(secondEvent, forbidden) {
			t.Fatalf("second SSE event leaked %q: %q", forbidden, secondEvent)
		}
	}
}

func TestServerRejectsUnknownRoutesAndMethods(t *testing.T) {
	server, _ := startTestServer(t)
	endpoint := server.Endpoint()
	for _, test := range []struct {
		path   string
		method string
		want   int
	}{
		{path: "/v1/status", method: http.MethodPost, want: http.StatusMethodNotAllowed},
		{path: "/v1/nope", method: http.MethodGet, want: http.StatusNotFound},
	} {
		request, err := http.NewRequest(test.method, endpoint.CapabilityURL()+test.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != test.want {
			t.Errorf("%s %s status = %d, want %d", test.method, test.path, response.StatusCode, test.want)
		}
	}
}

func TestDecodeJSONIsBoundedStrictAndSingleDocument(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "unknown field", body: `{"name":"x","extra":true}`, want: "unknown field"},
		{name: "trailing document", body: `{"name":"x"}{"name":"y"}`, want: "one JSON document"},
		{name: "trailing text", body: `{"name":"x"} trailing`, want: "trailing JSON"},
		{name: "malformed JSON", body: `{"name":`, want: "decode JSON request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			var value struct {
				Name string `json:"name"`
			}
			if err := DecodeJSON(httptest.NewRecorder(), request, &value); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeJSON error = %v, want %q", err, test.want)
			}
		})
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"`+strings.Repeat("x", maxRequestBody)+`"}`))
	request.Header.Set("Content-Type", "application/json")
	if err := DecodeJSON(httptest.NewRecorder(), request, &struct{}{}); err == nil {
		t.Fatal("DecodeJSON accepted oversized body")
	} else if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("oversized error = %v, want ErrRequestTooLarge", err)
	}
	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"x"}`+strings.Repeat(" ", maxRequestBody)))
	request.Header.Set("Content-Type", "application/json")
	if err := DecodeJSON(httptest.NewRecorder(), request, &struct {
		Name string `json:"name"`
	}{}); err == nil || !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("oversized trailing error = %v, want ErrRequestTooLarge", err)
	}
}

func TestDecodeJSONRejectsContentTypeAndErrorsAreBounded(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"x"}`))
	request.Header.Set("Content-Type", "text/plain")
	if err := DecodeJSON(httptest.NewRecorder(), request, &struct{}{}); err == nil || !strings.Contains(err.Error(), "content type") {
		t.Fatalf("wrong content type error = %v", err)
	}
	request.Header.Set("Content-Type", "application/json; charset=broken\x00")
	if err := DecodeJSON(httptest.NewRecorder(), request, &struct{}{}); err == nil || !strings.Contains(err.Error(), "content type") {
		t.Fatalf("malformed content type error = %v", err)
	}
	recorder := httptest.NewRecorder()
	writeError(recorder, http.StatusBadRequest, "invalid_request", strings.Repeat("x", 4096))
	if recorder.Body.Len() > 512 {
		t.Fatalf("generic error body length = %d, want bounded", recorder.Body.Len())
	}
}

func TestMutationContextIgnoresRequestCancellationHonorsDeadlineAndShutdown(t *testing.T) {
	server, _ := startTestServer(t)
	request, requestCancel := context.WithCancel(context.Background())
	requestCancel()
	mutation, cancel, err := server.MutationContext(request, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	select {
	case <-mutation.Done():
		t.Fatal("mutation canceled with request context")
	case <-time.After(5 * time.Millisecond):
	}
	select {
	case <-mutation.Done():
		if !errors.Is(mutation.Err(), context.DeadlineExceeded) {
			t.Fatalf("mutation error = %v, want deadline", mutation.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("mutation deadline did not fire")
	}

	shutdownRequest := context.Background()
	mutation, cancel, err = server.MutationContext(shutdownRequest, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mutation.Done():
	case <-time.After(time.Second):
		t.Fatal("mutation was not canceled by daemon shutdown")
	}
}

func TestServerReplacesStaleEndpoint(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(stateDir, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(EndpointPath(stateDir), []byte(`{"version":1,"protocol":1,"pid":1,"url":"http://127.0.0.1:1","token":"stale"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadEndpoint(stateDir)
	if err != nil || loaded.EndpointKey == "stale" {
		t.Fatalf("stale endpoint was not replaced: %#v, %v", loaded, err)
	}
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdown)
}

func TestServerRestartPublishesCurrentEndpoint(t *testing.T) {
	stateDir := t.TempDir()
	start := func() *Server {
		t.Helper()
		server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: stateDir})
		if err != nil {
			t.Fatal(err)
		}
		if err := server.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return server
	}
	first := start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := first.Shutdown(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if _, err := os.Stat(EndpointPath(stateDir)); !os.IsNotExist(err) {
		t.Fatalf("endpoint after first shutdown: %v", err)
	}
	second := start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := second.Shutdown(ctx); err != nil {
			t.Errorf("shutdown restarted server: %v", err)
		}
	})
	current, err := ReadEndpoint(stateDir)
	if err != nil || current != second.Endpoint() {
		t.Fatalf("endpoint after restart = %#v, err=%v; want %#v", current, err, second.Endpoint())
	}
}
