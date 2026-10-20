package daemonclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const logsReadyFrame = "event: logs.ready\ndata: {\"schema_version\":1,\"instance_id\":\"test\",\"workers\":[],\"kinds\":[\"message\",\"diagnostic\"],\"replay\":false}\n\n"

func logFramesClient(t *testing.T, frames string, check func(*http.Request)) *Client {
	t.Helper()
	return &Client{endpoint: daemon.Endpoint{URL: "http://orc.invalid", EndpointKey: "test-key"}, http: &http.Client{Transport: eventStreamRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/test-key/v1/logs" {
			t.Errorf("request path=%s", r.URL.Path)
		}
		if r.Header.Get("Last-Event-ID") != "" {
			t.Error("client invented resume header")
		}
		if check != nil {
			check(r)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(frames))}, nil
	})}}
}

func TestLogsClientRepeatedURLEncodingAndTerminalFrame(t *testing.T) {
	c := logFramesClient(t, logsReadyFrame+": heartbeat\n\n"+"event: log\ndata: {\"worker\":\"one\",\"kind\":\"message\",\"text\":\"**世界**\\nnext\"}\n\n"+"event: logs.error\ndata: {\"code\":\"inventory_changed\",\"message\":\"refresh\"}\n\n", func(r *http.Request) {
		want := url.Values{"worker": {"one", "two +&世界"}, "repository_id": {logsRepoA, logsRepoB}, "kind": {"message", "command"}}
		if !reflect.DeepEqual(r.URL.Query(), want) {
			t.Errorf("query=%v want=%v", r.URL.Query(), want)
		}
	})
	s, err := c.Logs(context.Background(), LogsQuery{Workers: []string{"one", "two +&世界"}, RepositoryIDs: []string{logsRepoA, logsRepoB}, Kinds: []string{"message", "command"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if nextLog(t, s).Ready == nil {
		t.Fatal("missing ready")
	}
	if r := nextLog(t, s).Record; r == nil || r.Text != "**世界**\nnext" {
		t.Fatalf("record=%+v", r)
	}
	if e := nextLog(t, s).Error; e == nil || e.Code != "inventory_changed" {
		t.Fatalf("error=%+v", e)
	}
	if _, err := s.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal=%v", err)
	}
}

func TestLogsClientBoundsFramesAndEnforcesReadyOrdering(t *testing.T) {
	for _, tc := range []struct {
		name, frames string
		ready        bool
	}{
		{"oversize line", strings.Repeat("x", maxLogFrameBytes+1), false},
		{"oversize multi-line frame", strings.Repeat(": comment\n", maxLogFrameBytes/8), false},
		{"oversize JSON payload", logsReadyFrame + "event: log\ndata: {\"text\":\"" + strings.Repeat("x", daemon.MaxLogRecordBytes) + "\"}\n\n", true},
		{"record before ready", "event: log\ndata: {}\n\n", false},
		{"error before ready", "event: logs.error\ndata: {\"code\":\"failed\"}\n\n", false},
		{"duplicate ready", logsReadyFrame + logsReadyFrame, true},
		{"unsupported schema", "event: logs.ready\ndata: {\"schema_version\":2}\n\n", false},
		{"replay claim", "event: logs.ready\ndata: {\"schema_version\":1,\"replay\":true}\n\n", false},
		{"cursor", "id: 1\n" + logsReadyFrame, false},
		{"bad ready JSON", "event: logs.ready\ndata: {bad}\n\n", false},
		{"bad record JSON", logsReadyFrame + "event: log\ndata: {bad}\n\n", true},
		{"empty error code", logsReadyFrame + "event: logs.error\ndata: {}\n\n", true},
		{"unknown event", logsReadyFrame + "event: other\ndata: {}\n\n", true},
		{"partial frame", strings.TrimSuffix(logsReadyFrame, "\n"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := logFramesClient(t, tc.frames, nil).Logs(context.Background(), LogsQuery{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if tc.ready {
				if nextLog(t, s).Ready == nil {
					t.Fatal("missing ready")
				}
			}
			if _, err := s.Next(); !hasClientErrorKind(err, ErrorProtocol) {
				t.Fatalf("error=%v", err)
			}
			if _, err := s.Next(); !errors.Is(err, io.EOF) {
				t.Fatalf("after error=%v", err)
			}
		})
	}
}

func TestLogsClientBoundsIndividualFramesWithoutLimitingLifetime(t *testing.T) {
	const frame = "event: log\ndata: {\"kind\":\"message\",\"text\":\"hello\"}\n\n"
	count := maxLogFrameBytes/len(frame) + 1
	s, err := logFramesClient(t, logsReadyFrame+strings.Repeat(frame, count), nil).Logs(context.Background(), LogsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	nextLog(t, s)
	for range count {
		if r := nextLog(t, s).Record; r == nil || r.Text != "hello" {
			t.Fatalf("record=%+v", r)
		}
	}
	if _, err := s.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end=%v", err)
	}
}

func TestLogsClientPreservesHTTPFilterErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{400, "invalid_query"}, {404, "worker_not_found"}, {422, "capability_unavailable"}, {429, "subscription_limit"}} {
		c := &Client{endpoint: daemon.Endpoint{URL: "http://orc.invalid"}, http: &http.Client{Transport: eventStreamRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"` + tc.code + `","message":"failed"}}`))}, nil
		})}}
		_, err := c.Logs(context.Background(), LogsQuery{})
		var result *Error
		if !errors.As(err, &result) || result.Kind != ErrorApplication || result.Code != tc.code || result.StatusCode != tc.status {
			t.Fatalf("HTTP error=%+v", err)
		}
	}
}
