package daemonclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestWorkerControlPreservesMissingResources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	starts := 0
	server, err := daemon.NewServer(daemon.Config{
		StateDir: t.TempDir(), InstanceID: logsInstance, EndpointKey: clientEndpointKey,
		Status: func() daemon.Status {
			return daemon.Status{Workers: []daemon.WorkerStatus{{Name: "known"}}}
		},
		Control: &daemon.Control{StartWorker: func(_ context.Context, name string) (supervisor.MutationResult, error) {
			starts++
			return supervisor.MutationResult{Worker: name, State: "running", Applied: true}, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := NewWithEndpoint("", server.Endpoint().CapabilityURL(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, code, message string
		call                func() error
	}{
		{"worker", "worker_not_found", "unknown worker", func() error { _, err := client.Worker(ctx, "orc-coder", "start"); return err }},
		{"group", "group_not_found", "unknown group", func() error { _, err := client.Group(ctx, "unknown", "start"); return err }},
		{"repository", "repository_not_found", "unknown repository", func() error { _, err := client.Repository(ctx, logsRepoA); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failure *Error
			if err := tc.call(); !errors.As(err, &failure) || failure.Kind != ErrorApplication || failure.Code != tc.code || failure.Message != tc.message || failure.StatusCode != http.StatusNotFound {
				t.Fatalf("error=%v (%+v)", err, failure)
			}
		})
	}
	result, err := client.Worker(ctx, "known", "start")
	if err != nil || result.Worker != "known" || result.State != "running" || !result.Applied {
		t.Fatalf("start result=%+v err=%v", result, err)
	}
	for _, key := range []string{"", strings.Repeat("B", len(clientEndpointKey))} {
		endpoint := server.Endpoint()
		endpoint.EndpointKey = key
		unauthorized := &Client{endpoint: endpoint, http: http.DefaultClient}
		for _, call := range []func() error{
			func() error { _, err := unauthorized.Worker(ctx, "known", "start"); return err },
			func() error { _, err := unauthorized.Logs(ctx, LogsQuery{}); return err },
			func() error { _, err := unauthorized.Events(ctx); return err },
		} {
			var failure *Error
			if err := call(); !errors.As(err, &failure) || failure.Kind != ErrorAuth || failure.Code != "unauthorized" || failure.StatusCode != http.StatusNotFound {
				t.Fatalf("capability error=%v (%+v)", err, failure)
			}
			if strings.Contains(failure.Error(), clientEndpointKey) || (key != "" && strings.Contains(failure.Error(), key)) || strings.Contains(failure.Error(), endpoint.URL) {
				t.Fatalf("error exposed capability: %v", failure)
			}
		}
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if starts != 1 {
		t.Fatalf("start calls=%d, want only the known worker", starts)
	}
}

func TestClientClassifiesStructuredHTTPFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		kind   ErrorKind
		code   string
	}{
		{"worker", 404, `{"error":{"code":"worker_not_found","message":"unknown worker"}}`, ErrorApplication, "worker_not_found"},
		{"group", 404, `{"error":{"code":"group_not_found","message":"unknown group"}}`, ErrorApplication, "group_not_found"},
		{"repository", 404, `{"error":{"code":"repository_not_found","message":"unknown repository"}}`, ErrorApplication, "repository_not_found"},
		{"repository endpoint", 404, `{"error":{"code":"repository_endpoint_not_found","message":"repository endpoint not found"}}`, ErrorApplication, "repository_endpoint_not_found"},
		{"generic missing", 404, `{"error":{"code":"not_found","message":"http://orc.invalid/` + clientEndpointKey + `"}}`, ErrorAuth, "unauthorized"},
		{"unauthorized missing", 404, `{"error":{"code":"unauthorized","message":"` + clientEndpointKey + `"}}`, ErrorAuth, "unauthorized"},
		{"unauthorized", 401, `{"error":{"code":"worker_not_found","message":"` + clientEndpointKey + `"}}`, ErrorAuth, "unauthorized"},
		{"unauthorized malformed", 401, "not JSON", ErrorAuth, "unauthorized"},
		{"malformed", 404, "not JSON", ErrorProtocol, ""},
		{"wrong field type", 404, `{"error":{"code":4}}`, ErrorProtocol, ""},
		{"multiple", 404, `{"error":{"code":"worker_not_found"}}{}`, ErrorProtocol, ""},
		{"oversized", 404, strings.Repeat("x", maxResponseBytes+100), ErrorProtocol, ""},
	} {
		for _, path := range []string{"worker", "logs"} {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				body := strings.NewReader(tc.body)
				calls := 0
				client := &Client{
					endpoint: daemon.Endpoint{URL: "http://orc.invalid", EndpointKey: clientEndpointKey},
					http: &http.Client{Transport: controlRoundTripper(func(request *http.Request) (*http.Response, error) {
						calls++
						return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(body), Request: request}, nil
					})},
				}
				var err error
				if path == "worker" {
					_, err = client.Worker(context.Background(), "missing", "start")
				} else {
					_, err = client.Logs(context.Background(), LogsQuery{Workers: []string{"missing"}})
				}
				var failure *Error
				if !errors.As(err, &failure) || failure.Kind != tc.kind || failure.Code != tc.code {
					t.Fatalf("error=%v (%+v), want %s/%s", err, failure, tc.kind, tc.code)
				}
				if tc.kind != ErrorProtocol && failure.StatusCode != tc.status {
					t.Fatalf("status=%d, want %d", failure.StatusCode, tc.status)
				}
				if strings.Contains(err.Error(), clientEndpointKey) || strings.Contains(err.Error(), "http://orc.invalid") {
					t.Fatalf("error exposed capability: %v", err)
				}
				if calls != 1 || len(tc.body)-body.Len() > maxResponseBytes+1 {
					t.Fatalf("calls=%d bytes read=%d", calls, len(tc.body)-body.Len())
				}
			})
		}
	}
}

func TestWorkerNotFoundPreservesMutationDetails(t *testing.T) {
	client := &Client{
		endpoint: daemon.Endpoint{URL: "http://orc.invalid", EndpointKey: clientEndpointKey},
		http: &http.Client{Transport: controlRoundTripper(func(request *http.Request) (*http.Response, error) {
			body := `{"error":{"code":"worker_not_found","message":"unknown worker","mutation_applied":true,"state":"stopped","daemon_control":{"mode":"paused","mutation_applied":true}},"group":{"group":"backend","results":[{"worker":"one","state":"stopped","mutation_applied":true}]}}`
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		})},
	}
	result, err := client.Worker(context.Background(), "missing", "start")
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != ErrorApplication || failure.Code != "worker_not_found" || failure.StatusCode != http.StatusNotFound || !failure.Applied {
		t.Fatalf("error=%v (%+v)", err, failure)
	}
	if result.Worker != "missing" || result.State != "stopped" || !result.Applied || failure.Result == nil || failure.Result.State != "stopped" || !failure.Result.Applied {
		t.Fatalf("mutation result=%+v error=%+v", result, failure)
	}
	if failure.Group == nil || failure.Group.Group != "backend" || len(failure.Group.Results) != 1 || !failure.Group.Results[0].Applied {
		t.Fatalf("group details=%+v", failure.Group)
	}
	if failure.DaemonControl == nil || failure.DaemonControl.Mode != "paused" || !failure.DaemonControl.Applied {
		t.Fatalf("daemon details=%+v", failure.DaemonControl)
	}
}
