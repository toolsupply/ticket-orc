package daemonclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

type discoveryRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper discoveryRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

func clientWithEndpointDocument(t *testing.T, endpointJSON string, transport discoveryRoundTripper) *Client {
	t.Helper()
	stateDir := t.TempDir()
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "endpoint.json"), []byte(endpointJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := NewWithHTTPClient(stateDir, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func discoveryResponse(request *http.Request, body string) (*http.Response, error) {
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func TestStatusReadsMatchingInstanceAndCapabilitiesWithUnknownFields(t *testing.T) {
	const instanceID = "11111111-1111-4111-8111-111111111111"
	endpointJSON := `{"version":1,"protocol":1,"instance_id":"` + instanceID + `","pid":123,"url":"http://127.0.0.1:1234","endpoint_key":"` + clientEndpointKey + `","future_endpoint_field":true}`
	client := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/"+clientEndpointKey+"/v1/status" {
			t.Errorf("status request method=%s path=%q", request.Method, request.URL.Path)
		}
		body := `{"version":"test","protocol":1,"instance_id":"` + instanceID + `","workers":[],"capabilities":{"events":true,"daemon_actions":[],"worker_actions":["start","stop","pause","resume","restart"],"group_actions":[],"repositories":{"inventory":true,"ticket_list":false,"max_page_size":256,"max_search_bytes":256,"future_capability":true},"future_status_capability":true},"future_status_field":true}`
		return discoveryResponse(request, body)
	})
	status, err := client.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.InstanceID != instanceID || status.Protocol != 1 || !status.Capabilities.Events || !status.Capabilities.Repositories.Inventory || status.Capabilities.Repositories.MaxPageSize != 256 || len(status.Capabilities.WorkerActions) != 5 {
		t.Fatalf("decoded additive status = %#v", status)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), clientEndpointKey) || strings.Contains(string(encoded), "endpoint_key") {
		t.Fatalf("status exposed endpoint capability: %s", encoded)
	}
}

func TestStatusRejectsEndpointInstanceMismatch(t *testing.T) {
	endpointID := "11111111-1111-4111-8111-111111111111"
	statusID := "22222222-2222-4222-8222-222222222222"
	endpointJSON := `{"version":1,"protocol":1,"instance_id":"` + endpointID + `","pid":123,"url":"http://127.0.0.1:1234","endpoint_key":"` + clientEndpointKey + `"}`
	client := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
		body := `{"protocol":1,"instance_id":"` + statusID + `","workers":[]}`
		return discoveryResponse(request, body)
	})
	status, err := client.Status(context.Background())
	if !reflect.DeepEqual(status, daemon.Status{}) || !hasClientErrorKind(err, ErrorProtocol) {
		t.Fatalf("mismatched status=%#v err=%v, want protocol error", status, err)
	}
	if strings.Contains(err.Error(), clientEndpointKey) || strings.Contains(err.Error(), endpointID) || strings.Contains(err.Error(), statusID) {
		t.Fatalf("instance mismatch error exposed endpoint material or IDs: %v", err)
	}
}

func TestStatusAcceptsOlderEndpointWithoutInstanceID(t *testing.T) {
	endpointJSON := `{"version":1,"protocol":1,"pid":123,"url":"http://127.0.0.1:1234","endpoint_key":"` + clientEndpointKey + `"}`
	client := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
		return discoveryResponse(request, `{"protocol":1,"instance_id":"11111111-1111-4111-8111-111111111111","workers":[]}`)
	})
	status, err := client.Status(context.Background())
	if err != nil || status.InstanceID == "" {
		t.Fatalf("legacy endpoint status=%#v err=%v", status, err)
	}
}

func TestGroupsReadsZeroOneAndMultipleLiteralResponses(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want []daemon.GroupStatus
	}{
		{name: "zero", body: `{"groups":[],"future":true}`, want: []daemon.GroupStatus{}},
		{name: "one", body: `{"groups":[{"name":"default"}],"future":true}`, want: []daemon.GroupStatus{{Name: "default"}}},
		{name: "multiple", body: `{"groups":[{"name":"backend"},{"name":"default"},{"name":"reviewers"}],"future":true}`, want: []daemon.GroupStatus{{Name: "backend"}, {Name: "default"}, {Name: "reviewers"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpointJSON := `{"version":1,"protocol":1,"pid":123,"url":"http://127.0.0.1:1234","endpoint_key":"` + clientEndpointKey + `"}`
			client := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet || request.URL.Path != "/"+clientEndpointKey+"/v1/groups" {
					t.Errorf("groups request method=%s path=%q", request.Method, request.URL.Path)
				}
				return discoveryResponse(request, test.body)
			})
			groups, err := client.Groups(context.Background())
			if err != nil || !reflect.DeepEqual(groups, test.want) {
				t.Fatalf("groups=%#v err=%v, want %#v", groups, err, test.want)
			}
		})
	}
}

func TestGroupsNormalizesMissingListAndPreservesAuthenticationErrors(t *testing.T) {
	endpointJSON := `{"version":1,"protocol":1,"pid":123,"url":"http://127.0.0.1:1234","endpoint_key":"` + clientEndpointKey + `"}`
	client := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/"+clientEndpointKey+"/v1/groups" {
			t.Errorf("groups request path=%q", request.URL.Path)
		}
		return discoveryResponse(request, `{"future":true}`)
	})
	groups, err := client.Groups(context.Background())
	if err != nil || groups == nil || len(groups) != 0 {
		t.Fatalf("missing groups list=%#v err=%v, want non-nil empty slice", groups, err)
	}

	unauthorized := clientWithEndpointDocument(t, endpointJSON, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/"+clientEndpointKey+"/v1/groups" {
			t.Errorf("groups request path=%q", request.URL.Path)
		}
		response, _ := discoveryResponse(request, `{"error":{"code":"not_found","message":"not found"}}`)
		response.StatusCode = http.StatusNotFound
		return response, nil
	})
	_, err = unauthorized.Groups(context.Background())
	var clientError *Error
	if !errors.As(err, &clientError) || clientError.Kind != ErrorAuth || clientError.StatusCode != http.StatusNotFound {
		t.Fatalf("unauthorized groups error=%v", err)
	}
}
