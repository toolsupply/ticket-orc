package daemonclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const testRepositoryID = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"

type repositoryRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip repositoryRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func newRepositoryTestClient(roundTrip repositoryRoundTripper) *Client {
	return &Client{
		endpoint: daemon.Endpoint{URL: "http://daemon.invalid", EndpointKey: "capability"},
		http:     &http.Client{Transport: roundTrip},
	}
}

func repositoryResponse(request *http.Request, status int, contentType, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     http.Header{"Content-Type": []string{contentType}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func TestRepositoryClientUsesInventoryIDAndTypedComposedFilters(t *testing.T) {
	var paths []string
	client := newRepositoryTestClient(func(request *http.Request) (*http.Response, error) {
		paths = append(paths, request.URL.EscapedPath())
		switch request.URL.Path {
		case "/capability/v1/repositories":
			return repositoryResponse(request, http.StatusOK, "application/json", `{"repositories":[{"id":"`+testRepositoryID+`","key":"repo-42","name":"tickets","state":"healthy"}]}`), nil
		case "/capability/v1/repositories/" + testRepositoryID:
			return repositoryResponse(request, http.StatusOK, "application/json", `{"id":"`+testRepositoryID+`","key":"repo-42","name":"tickets","state":"healthy"}`), nil
		case "/capability/v1/repositories/" + testRepositoryID + "/tickets":
			query := request.URL.Query()
			priorityValues := query["priority"]
			if query.Get("q") != "parent parser" || !reflect.DeepEqual(query["state"], []string{"open", "review"}) || !reflect.DeepEqual(priorityValues, []string{"1"}) || query.Get("assignee") != "coder" || !reflect.DeepEqual(query["tag"], []string{"ui", "api"}) || query.Get("limit") != "17" || query.Get("offset") != "6" {
				t.Errorf("repository ticket query=%#v raw=%q", query, request.URL.RawQuery)
			}
			return repositoryResponse(request, http.StatusOK, "application/json", `{"repository_id":"`+testRepositoryID+`","repository_key":"repo-42","items":[{"id":"20260925-00001","title":"matches","state":"open","priority":1,"assignee":"coder","parent":"20260924-00002","tags":["ui"]}],"more":true}`), nil
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL)
			return repositoryResponse(request, http.StatusNotFound, "application/json", `{"error":{"code":"not_found","message":"unexpected path"}}`), nil
		}
	})

	ctx := context.Background()
	inventory, err := client.Repositories(ctx)
	if err != nil || len(inventory) != 1 || inventory[0].ID != testRepositoryID || inventory[0].Key != "repo-42" {
		t.Fatalf("inventory=%#v err=%v", inventory, err)
	}
	repository, err := client.Repository(ctx, inventory[0].ID)
	if err != nil || repository.Key != inventory[0].Key {
		t.Fatalf("repository=%#v err=%v", repository, err)
	}
	priority := 1
	result, err := client.RepositoryTickets(ctx, repository.ID, daemon.RepositoryTicketQuery{
		Search: "parent parser", States: []string{"open", "review"}, Priority: &priority,
		Assignee: "coder", Tags: []string{"ui", "api"}, Limit: 17, Offset: 6,
	})
	if err != nil || len(result.Items) != 1 || result.Items[0].Parent != "20260924-00002" || !result.More {
		t.Fatalf("repository tickets=%#v err=%v", result, err)
	}
	if !reflect.DeepEqual(paths, []string{"/capability/v1/repositories", "/capability/v1/repositories/" + testRepositoryID, "/capability/v1/repositories/" + testRepositoryID + "/tickets"}) {
		t.Fatalf("request paths=%#v", paths)
	}
}

func TestRepositoryClientAllStatePaginationAndDetailProjection(t *testing.T) {
	var requests int
	client := newRepositoryTestClient(func(request *http.Request) (*http.Response, error) {
		requests++
		switch request.URL.Path {
		case "/capability/v1/repositories/" + testRepositoryID + "/tickets":
			query := request.URL.Query()
			if !reflect.DeepEqual(query["state"], []string{"all"}) || query.Get("limit") != "10" || query.Get("offset") != "20" {
				t.Errorf("all-state pagination query=%#v", query)
			}
			return repositoryResponse(request, http.StatusOK, "application/json", `{"repository_id":"`+testRepositoryID+`","repository_key":"repo-42","items":[{"id":"20260925-00001","state":"closed","parent":"20260924-00002"}],"more":true}`), nil
		case "/capability/v1/repositories/" + testRepositoryID + "/tickets/20260925-00001":
			return repositoryResponse(request, http.StatusOK, "application/json", `{"repository_id":"`+testRepositoryID+`","repository_key":"repo-42","id":"20260925-00001","title":"details","state":"open","readiness":{"ready":false,"blockers":[{"code":"dependency_open","id":"20260924-00002","message":"dependency is open"}]},"created":"2026-09-25","modified":"2026-09-25T12:00:00Z","body":"complete body","body_truncated":true}`), nil
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL)
			return repositoryResponse(request, http.StatusNotFound, "application/json", `{"error":{"code":"not_found","message":"unexpected path"}}`), nil
		}
	})

	list, err := client.RepositoryTickets(context.Background(), testRepositoryID, daemon.RepositoryTicketQuery{States: []string{"all"}, Limit: 10, Offset: 20})
	if err != nil || list.RepositoryID != testRepositoryID || list.RepositoryKey != "repo-42" || len(list.Items) != 1 || list.Items[0].Parent != "20260924-00002" || !list.More {
		t.Fatalf("all-state list=%#v err=%v", list, err)
	}
	detail, err := client.RepositoryTicket(context.Background(), testRepositoryID, "20260925-00001")
	if err != nil || detail.RepositoryID != testRepositoryID || detail.RepositoryKey != "repo-42" || detail.Readiness == nil || detail.Readiness.Ready || len(detail.Readiness.Blockers) != 1 || detail.Created != "2026-09-25" || detail.Modified != "2026-09-25T12:00:00Z" || detail.Body == nil || *detail.Body != "complete body" || detail.BodyTruncated == nil || !*detail.BodyTruncated {
		t.Fatalf("ticket detail=%#v err=%v", detail, err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want 2", requests)
	}
}

func TestRepositoryClientAcceptsWorstCaseEscapedBoundedDetail(t *testing.T) {
	const bodyBudget = 256 << 10
	const selectedSectionBudget = 16 << 10
	body := strings.Repeat("<", bodyBudget)
	section := strings.Repeat("&", selectedSectionBudget)
	sections := map[string]any{
		"objective":  map[string]any{"text": section},
		"acceptance": map[string]any{"text": section},
		"handoff":    map[string]any{"text": section},
		"work_log":   map[string]any{"text": section},
	}
	data, err := json.Marshal(map[string]any{
		"id": "20260925-00001", "state": "open", "body": body, "body_truncated": false,
		"sections": sections, "readiness": map[string]any{"ready": true},
		"created": "2026-09-25", "modified": "2026-09-25T12:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 1<<20 || len(data) > maxResponseBytes {
		t.Fatalf("fixture size=%d, want >1 MiB and <=%d bytes", len(data), maxResponseBytes)
	}

	client := newRepositoryTestClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/capability/v1/repositories/"+testRepositoryID+"/tickets/20260925-00001" {
			t.Errorf("unexpected path %q", request.URL.Path)
		}
		return repositoryResponse(request, http.StatusOK, "application/json", string(data)), nil
	})
	detail, err := client.RepositoryTicket(context.Background(), testRepositoryID, "20260925-00001")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Body == nil || len(*detail.Body) != bodyBudget || detail.Sections["objective"].Text != section || detail.BodyTruncated == nil || *detail.BodyTruncated {
		t.Fatalf("bounded detail body=%d sections=%d truncation=%v", len(valueOrEmpty(detail.Body)), len(detail.Sections), detail.BodyTruncated)
	}
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func TestRepositoryClientTypedMutationsAndEventRepositoryCorrelation(t *testing.T) {
	var mutationCount int
	client := newRepositoryTestClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/capability/v1/events" {
			request.Header.Set("Accept", "text/event-stream")
			return repositoryResponse(request, http.StatusOK, "text/event-stream", "id: 1\nevent: ticket.repository_changed\ndata: {\"seq\":1,\"type\":\"ticket.repository_changed\",\"repository_id\":\""+testRepositoryID+"\",\"repository_key\":\"repo-42\",\"code\":\"submitted\"}\n\n"), nil
		}
		mutationCount++
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode mutation body: %v", err)
		}
		if payload["actor"] != "ui-user" {
			t.Errorf("mutation actor=%#v", payload["actor"])
		}
		if request.Method != http.MethodPost || !strings.HasPrefix(request.URL.Path, "/capability/v1/repositories/"+testRepositoryID+"/") {
			t.Errorf("unexpected mutation request: %s %s", request.Method, request.URL)
		}
		operation := strings.TrimPrefix(request.URL.Path, "/capability/v1/repositories/"+testRepositoryID+"/tickets/20260925-00001/")
		if request.URL.Path == "/capability/v1/repositories/"+testRepositoryID+"/tickets" {
			operation = "create"
		} else if strings.HasSuffix(request.URL.Path, "/tickets/20260925-00001/update") {
			operation = "update"
		}
		return repositoryResponse(request, http.StatusOK, "application/json", fmt.Sprintf(`{"repository_id":%q,"repository_key":"repo-42","operation":%q,"changed":true,"ticket":{"id":"20260925-00001","state":"open"}}`, testRepositoryID, operation)), nil
	})

	ctx := context.Background()
	_, err := client.CreateRepositoryTicket(ctx, testRepositoryID, daemon.RepositoryTicketCreateRequest{Actor: "ui-user", Title: "Create"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateRepositoryTicket(ctx, testRepositoryID, "20260925-00001", daemon.RepositoryTicketUpdateRequest{Actor: "ui-user", Set: map[string]any{"title": "Update"}})
	if err != nil {
		t.Fatal(err)
	}
	operations := []string{"claim", "release", "open", "hold", "submit", "review", "approve", "close", "reject", "bump"}
	for _, operation := range operations {
		result, err := client.MutateRepositoryTicket(ctx, testRepositoryID, "20260925-00001", operation, daemon.RepositoryTicketMutationRequest{Actor: "ui-user"})
		if err != nil || result.RepositoryID != testRepositoryID || result.RepositoryKey != "repo-42" || result.Operation != operation || !result.Changed {
			t.Fatalf("operation=%s result=%#v err=%v", operation, result, err)
		}
	}
	if _, err := client.MutateRepositoryTicket(ctx, testRepositoryID, "20260925-00001", "arbitrary", daemon.RepositoryTicketMutationRequest{Actor: "ui-user"}); err == nil {
		t.Fatal("accepted arbitrary workflow operation")
	}
	if mutationCount != 12 {
		t.Fatalf("mutation count=%d, want create + update + 10 workflow operations", mutationCount)
	}
	stream, err := client.Events(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next()
	if err != nil || event.Type != "ticket.repository_changed" || event.RepositoryID != testRepositoryID || event.RepositoryKey != "repo-42" {
		t.Fatalf("repository event=%#v err=%v", event, err)
	}
}
