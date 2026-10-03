package daemon

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func groupInventoryHandler(t *testing.T, control *Control) http.Handler {
	t.Helper()
	server, err := NewServer(Config{
		StateDir:    t.TempDir(),
		EndpointKey: testEndpointKey,
		Control:     control,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server.handler()
}

func TestGroupInventoryReturnsBoundedLiteralProjection(t *testing.T) {
	tests := []struct {
		name   string
		groups []string
		want   string
	}{
		{name: "zero", groups: []string{}, want: "{\"groups\":[]}\n"},
		{name: "one", groups: []string{"default"}, want: "{\"groups\":[{\"name\":\"default\"}]}\n"},
		{name: "multiple", groups: []string{"backend", "default", "reviewers"}, want: "{\"groups\":[{\"name\":\"backend\"},{\"name\":\"default\"},{\"name\":\"reviewers\"}]}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := groupInventoryHandler(t, &Control{Groups: func() []string { return test.groups }})
			request := httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/groups", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK || response.Body.String() != test.want {
				t.Fatalf("groups response status=%d body=%q, want status=%d body=%q", response.Code, response.Body.String(), http.StatusOK, test.want)
			}
		})
	}
}

func TestGroupInventoryWithoutCallbackIsEmpty(t *testing.T) {
	for _, control := range []*Control{nil, &Control{}} {
		handler := groupInventoryHandler(t, control)
		request := httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/groups", nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "{\"groups\":[]}\n" {
			t.Fatalf("missing callback response status=%d body=%q, want 200 empty inventory", response.Code, response.Body.String())
		}
	}
}

func TestGroupInventoryRequiresCapabilityKeyAndGet(t *testing.T) {
	handler := groupInventoryHandler(t, &Control{Groups: func() []string { return []string{"default"} }})
	for _, test := range []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{name: "missing key", method: http.MethodGet, path: "/v1/groups", want: http.StatusNotFound},
		{name: "wrong method", method: http.MethodPost, path: "/" + testEndpointKey + "/v1/groups", want: http.StatusMethodNotAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("response status=%d body=%q, want %d", response.Code, response.Body.String(), test.want)
			}
		})
	}
}

func TestGroupInventoryRejectsOversizedOrUnsafeNames(t *testing.T) {
	tooMany := make([]string, maxGroupInventoryEntries+1)
	for i := range tooMany {
		tooMany[i] = "group"
	}
	for _, test := range []struct {
		name   string
		groups []string
		secret string
		want   string
	}{
		{name: "too many", groups: tooMany, want: `"message":"group inventory exceeds the response bound"`},
		{name: "unsafe path name", groups: []string{"/private/repository/path"}, secret: "/private/repository/path", want: `"message":"group inventory contains an invalid name"`},
		{name: "oversized name", groups: []string{strings.Repeat("g", maxPublicGroupNameBytes+1)}, secret: strings.Repeat("g", maxPublicGroupNameBytes+1), want: `"message":"group inventory contains an invalid name"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := groupInventoryHandler(t, &Control{Groups: func() []string { return test.groups }})
			request := httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/groups", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			leaked := test.secret != "" && strings.Contains(response.Body.String(), test.secret)
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), test.want) || leaked || len(response.Body.Bytes()) > 256 {
				t.Fatalf("invalid inventory response status=%d body=%q", response.Code, response.Body.String())
			}
		})
	}
}
