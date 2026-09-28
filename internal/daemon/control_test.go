package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestWorkerGroupReloadAndShutdownControls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	stopped := make(chan string, 2)
	partialGroup := false
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Workers: []WorkerStatus{{Name: "one", Role: "coder", Harness: "codex", State: "stopped", Groups: []string{"default"}}, {Name: "two", Role: "reviewer", Harness: "pi", State: "stopped", Groups: []string{"default"}}}}
		},
		Control: &Control{
			StartWorker: func(_ context.Context, name string) (supervisor.MutationResult, error) {
				started <- name
				return supervisor.MutationResult{Worker: name, State: "running", Applied: true}, nil
			},
			StopWorker: func(_ context.Context, name string) (supervisor.MutationResult, error) {
				stopped <- name
				return supervisor.MutationResult{Worker: name, State: "stopped", Applied: true}, nil
			},
			PauseWorker: func(_ context.Context, name string) (supervisor.MutationResult, error) {
				return supervisor.MutationResult{Worker: name, State: "paused", Applied: true}, nil
			},
			ResumeWorker: func(_ context.Context, name string) (supervisor.MutationResult, error) {
				return supervisor.MutationResult{Worker: name, State: "running", Applied: true}, nil
			},
			StartGroup: func(_ context.Context, name string) (supervisor.GroupResult, error) {
				if partialGroup {
					return supervisor.GroupResult{Group: name, Results: []supervisor.MutationResult{{Worker: "one", State: "running", Applied: true}, {Worker: "two", State: "stopped", Applied: false}}}, &supervisor.LifecycleError{Code: "group_partial_failure", Message: "one worker failed", Applied: true}
				}
				return supervisor.GroupResult{Group: name, Results: []supervisor.MutationResult{{Worker: "one", Applied: true}}}, nil
			},
			Reload: func(_ context.Context) (supervisor.ReloadResult, error) {
				return supervisor.ReloadResult{Revision: "2", Applied: true}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	endpoint := server.Endpoint()
	request := func(method, path, body string) (*http.Response, error) {
		req, err := http.NewRequest(method, endpoint.CapabilityURL()+path, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		return http.DefaultClient.Do(req)
	}
	response, err := request(http.MethodGet, "/v1/workers", "")
	if err != nil {
		t.Fatal(err)
	}
	var workers struct {
		Workers []WorkerStatus `json:"workers"`
	}
	if err := json.NewDecoder(response.Body).Decode(&workers); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(workers.Workers) != 2 {
		t.Fatalf("workers response = %#v status=%d", workers, response.StatusCode)
	}
	response, err = request(http.MethodPost, "/v1/workers/one/start", "")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("start status = %d", response.StatusCode)
	}
	select {
	case got := <-started:
		if got != "one" {
			t.Fatalf("started worker = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("start callback not called")
	}
	for _, path := range []string{"/v1/workers/one/pause", "/v1/workers/one/resume"} {
		response, err = request(http.MethodPost, path, "")
		if err != nil {
			t.Fatal(err)
		}
		var result MutationResult
		decodeErr := json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusOK || !result.Applied {
			t.Fatalf("%s response=%#v status=%d err=%v", path, result, response.StatusCode, decodeErr)
		}
	}
	response, err = request(http.MethodPost, "/v1/groups/default/start", "")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("group status = %d", response.StatusCode)
	}
	partialGroup = true
	response, err = request(http.MethodPost, "/v1/groups/default/start", "")
	if err != nil {
		t.Fatal(err)
	}
	var partial struct {
		Error map[string]any `json:"error"`
		Group GroupResult    `json:"group"`
	}
	if err := json.NewDecoder(response.Body).Decode(&partial); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || partial.Error["code"] != "group_partial_failure" || len(partial.Group.Results) != 2 || !partial.Group.Results[0].Applied || partial.Group.Results[1].Applied {
		t.Fatalf("partial group response = %#v status=%d", partial, response.StatusCode)
	}
	response, err = request(http.MethodPost, "/v1/config/reload", `{}`)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reload status = %d", response.StatusCode)
	}
	response, err = request(http.MethodPost, "/v1/workers/missing/start", "")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d", response.StatusCode)
	}
}

func TestDaemonPauseResumeRoutesAreCapabilityRouted(t *testing.T) {
	var paused, resumed bool
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Control: &Control{
			PauseDaemon: func(context.Context) (DaemonControlResult, error) {
				paused = true
				return DaemonControlResult{Mode: "paused", Applied: true}, nil
			},
			ResumeDaemon: func(context.Context) (DaemonControlResult, error) {
				resumed = true
				return DaemonControlResult{Mode: "running", Applied: true}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	for _, test := range []struct {
		path string
		mode string
	}{
		{path: "/v1/pause", mode: "paused"},
		{path: "/v1/resume", mode: "running"},
	} {
		request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+test.path, nil)
		response := httptest.NewRecorder()
		server.handler().ServeHTTP(response, request)
		var result DaemonControlResult
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusOK || result.Mode != test.mode || !result.Applied {
			t.Fatalf("%s response=%#v status=%d err=%v body=%s", test.path, result, response.Code, err, response.Body.String())
		}
	}
	if !paused || !resumed {
		t.Fatalf("control callbacks pause=%t resume=%t", paused, resumed)
	}
	request := httptest.NewRequest(http.MethodPost, "/unauthorized/v1/pause", nil)
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unauthorized capability route status=%d callback pause=%t", response.Code, paused)
	}
}

func TestDaemonAbortRouteReturnsBoundedPartialResultAndStatusMode(t *testing.T) {
	var mode = "aborted"
	abortCalls := 0
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Workers: []WorkerStatus{{Name: "coder", State: "stopped"}}}
		},
		Control: &Control{
			DaemonMode: func() string { return mode },
			AbortDaemon: func(context.Context) (DaemonControlResult, error) {
				abortCalls++
				return DaemonControlResult{
					Mode: mode, Applied: false,
					Targets: []DaemonAbortTargetResult{{Kind: "managed", Worker: "coder", Outcome: "termination_failed", Code: "process_unavailable"}},
				}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+"/v1/abort", nil)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, request)
	var result DaemonControlResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("abort response=%s status=%d err=%v", recorder.Body.String(), recorder.Code, err)
	}
	if result.Mode != "aborted" || result.Applied || len(result.Targets) != 1 || result.Targets[0].Outcome != "termination_failed" {
		t.Fatalf("abort result = %#v", result)
	}
	encoded, _ := json.Marshal(result)
	for _, forbidden := range []string{"prompt", "credential", "session content", "home directory", "argv", "environment"} {
		if strings.Contains(strings.ToLower(string(encoded)), forbidden) {
			t.Fatalf("abort result exposed forbidden detail %q: %s", forbidden, encoded)
		}
	}
	if abortCalls != 1 {
		t.Fatalf("abort callback calls=%d, want 1", abortCalls)
	}

	request = httptest.NewRequest(http.MethodPost, "/wrong/v1/abort", nil)
	recorder = httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound || abortCalls != 1 {
		t.Fatalf("unauthenticated abort status=%d callback calls=%d", recorder.Code, abortCalls)
	}

	request = httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/status", nil)
	recorder = httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, request)
	var status Status
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("status response=%s status=%d err=%v", recorder.Body.String(), recorder.Code, err)
	}
	if status.Mode != "aborted" || len(status.Workers) != 1 || status.Workers[0].State != "stopped" {
		t.Fatalf("status did not expose mode independently of worker state: %#v", status)
	}
}

func TestDaemonResumeErrorPreservesPersistedModeResult(t *testing.T) {
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Control: &Control{ResumeDaemon: func(context.Context) (DaemonControlResult, error) {
			result := DaemonControlResult{Mode: "running", Applied: true}
			return result, &ControlError{
				Code: "daemon_reconciliation_failed", Status: http.StatusServiceUnavailable,
				Message: "daemon resumed but worker reconciliation failed",
				Applied: true, Daemon: &result,
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+"/v1/resume", nil)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, request)
	var response struct {
		Error struct {
			Code   string              `json:"code"`
			Result DaemonControlResult `json:"daemon_control"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("resume response=%s status=%d err=%v", recorder.Body.String(), recorder.Code, err)
	}
	if response.Error.Code != "daemon_reconciliation_failed" || response.Error.Result.Mode != "running" || !response.Error.Result.Applied {
		t.Fatalf("resume partial response = %#v", response)
	}
}

func TestShutdownRemainsAvailableInEveryDaemonMode(t *testing.T) {
	for _, mode := range []string{"running", "paused", "aborted"} {
		t.Run(mode, func(t *testing.T) {
			shutdownCalls := 0
			server, err := NewServer(Config{
				EndpointKey: testEndpointKey,
				StateDir:    t.TempDir(),
				Control: &Control{
					DaemonMode: func() string { return mode },
					Shutdown: func(context.Context) error {
						shutdownCalls++
						return nil
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			server.lifecycle = context.Background()
			server.started = true
			server.http = &http.Server{}
			request := httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+"/v1/shutdown", nil)
			response := httptest.NewRecorder()
			server.handler().ServeHTTP(response, request)
			if response.Code != http.StatusAccepted || shutdownCalls != 1 {
				t.Fatalf("shutdown in mode %s status=%d calls=%d body=%s", mode, response.Code, shutdownCalls, response.Body.String())
			}
		})
	}
}
