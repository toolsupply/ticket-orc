package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

type uiAPIRoundTripper func(*http.Request) (*http.Response, error)

const uiAPIEndpointKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
const uiAPIRepositoryID = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"

func (roundTrip uiAPIRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestTypedTicketUIAPIContract(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("TICKET_ORC_ENDPOINT", "")
	runDir := filepath.Join(stateDir, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint, err := json.Marshal(daemon.Endpoint{Version: 1, Protocol: 1, PID: os.Getpid(), URL: "http://127.0.0.1:43123", EndpointKey: uiAPIEndpointKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "endpoint.json"), endpoint, 0o600); err != nil {
		t.Fatal(err)
	}

	var mutations int
	httpClient := &http.Client{Transport: uiAPIRoundTripper(func(request *http.Request) (*http.Response, error) {
		respond := func(status int, contentType, body string) (*http.Response, error) {
			return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		}
		path := strings.TrimPrefix(request.URL.Path, "/"+uiAPIEndpointKey)
		switch path {
		case "/v1/repositories":
			return respond(http.StatusOK, "application/json", `{"repositories":[{"id":"`+uiAPIRepositoryID+`","key":"tickets-42","name":"Tickets","state":"healthy"}]}`)
		case "/v1/repositories/" + uiAPIRepositoryID + "":
			return respond(http.StatusOK, "application/json", `{"id":"`+uiAPIRepositoryID+`","key":"tickets-42","name":"Tickets","state":"healthy"}`)
		case "/v1/repositories/" + uiAPIRepositoryID + "/tickets":
			if request.Method == http.MethodPost {
				mutations++
				var payload map[string]any
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Errorf("decode create request: %v", err)
				}
				if payload["actor"] != "ui-user" {
					t.Errorf("create actor=%#v", payload["actor"])
				}
				return respond(http.StatusOK, "application/json", `{"repository_id":"`+uiAPIRepositoryID+`","repository_key":"tickets-42","operation":"create","changed":true,"ticket":{"id":"20260925-00003","state":"open"}}`)
			}
			query := request.URL.Query()
			if query.Get("q") == "parent parser" {
				if !reflect.DeepEqual(query["state"], []string{"open", "review"}) || query.Get("priority") != "1" || query.Get("assignee") != "coder" || !reflect.DeepEqual(query["tag"], []string{"ticket-ui", "api"}) || query.Get("limit") != "5" || query.Get("offset") != "2" {
					t.Errorf("combined search query=%#v", query)
				}
				return respond(http.StatusOK, "application/json", `{"repository_id":"`+uiAPIRepositoryID+`","repository_key":"tickets-42","items":[{"id":"20260925-00001","state":"open","parent":"20260924-00001"}],"more":true}`)
			}
			if !reflect.DeepEqual(query["state"], []string{"all"}) || query.Get("limit") != "10" || query.Get("offset") != "20" {
				t.Errorf("all-state pagination query=%#v", query)
			}
			return respond(http.StatusOK, "application/json", `{"repository_id":"`+uiAPIRepositoryID+`","repository_key":"tickets-42","items":[{"id":"20260925-00002","state":"closed","parent":"20260924-00002"}],"more":true}`)
		case "/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001":
			return respond(http.StatusOK, "application/json", `{"repository_id":"`+uiAPIRepositoryID+`","repository_key":"tickets-42","id":"20260925-00001","title":"UI ticket","state":"open","readiness":{"ready":false,"blockers":[{"code":"dependency_open","id":"20260924-00001","message":"dependency is open"}]},"created":"2026-09-25","modified":"2026-09-25T12:00:00Z","body":"complete display body","body_truncated":false}`)
		case "/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/update",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/claim",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/release",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/open",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/hold",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/submit",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/review",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/approve",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/close",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/reject",
			"/v1/repositories/" + uiAPIRepositoryID + "/tickets/20260925-00001/bump":
			mutations++
			if request.Method != http.MethodPost {
				t.Errorf("mutation method=%s, want POST", request.Method)
			}
			var payload map[string]any
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode mutation request: %v", err)
			}
			if payload["actor"] != "ui-user" {
				t.Errorf("mutation actor=%#v", payload["actor"])
			}
			operation := strings.TrimPrefix(path, "/v1/repositories/"+uiAPIRepositoryID+"/tickets/20260925-00001/")
			return respond(http.StatusOK, "application/json", fmt.Sprintf(`{"repository_id":%q,"repository_key":"tickets-42","operation":%q,"changed":true,"ticket":{"id":"20260925-00001","state":"open"}}`, uiAPIRepositoryID, operation))
		case "/v1/events":
			return respond(http.StatusOK, "text/event-stream", "id: 1\nevent: ticket.repository_changed\ndata: {\"seq\":1,\"type\":\"ticket.repository_changed\",\"repository_id\":\""+uiAPIRepositoryID+"\",\"repository_key\":\"tickets-42\",\"code\":\"submitted\"}\n\n")
		}
		return respond(http.StatusNotFound, "application/json", `{"error":{"code":"not_found","message":"unexpected path"}}`)
	})}
	client, err := daemonclient.NewWithHTTPClient(stateDir, httpClient)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	inventory, err := client.Repositories(ctx)
	if err != nil || len(inventory) != 1 || inventory[0].ID != uiAPIRepositoryID || inventory[0].Key != "tickets-42" {
		t.Fatalf("inventory=%#v err=%v", inventory, err)
	}
	repository, err := client.Repository(ctx, inventory[0].ID)
	if err != nil || repository.Key != inventory[0].Key {
		t.Fatalf("repository=%#v err=%v", repository, err)
	}
	priority := 1
	list, err := client.RepositoryTickets(ctx, repository.ID, daemon.RepositoryTicketQuery{
		Search: "parent parser", States: []string{"open", "review"}, Priority: &priority,
		Assignee: "coder", Tags: []string{"ticket-ui", "api"}, Limit: 5, Offset: 2,
	})
	if err != nil || list.RepositoryID != repository.ID || list.RepositoryKey != repository.Key || len(list.Items) != 1 || list.Items[0].Parent != "20260924-00001" || !list.More {
		t.Fatalf("filtered list=%#v err=%v", list, err)
	}
	all, err := client.RepositoryTickets(ctx, repository.ID, daemon.RepositoryTicketQuery{States: []string{"all"}, Limit: 10, Offset: 20})
	if err != nil || all.RepositoryID != repository.ID || all.RepositoryKey != repository.Key || len(all.Items) != 1 || all.Items[0].Parent != "20260924-00002" || !all.More {
		t.Fatalf("all-state list=%#v err=%v", all, err)
	}
	detail, err := client.RepositoryTicket(ctx, repository.ID, "20260925-00001")
	if err != nil || detail.RepositoryID != repository.ID || detail.RepositoryKey != repository.Key || detail.Readiness == nil || detail.Readiness.Ready || detail.Created != "2026-09-25" || detail.Modified != "2026-09-25T12:00:00Z" || detail.Body == nil || *detail.Body != "complete display body" || detail.BodyTruncated == nil || *detail.BodyTruncated {
		t.Fatalf("ticket detail=%#v err=%v", detail, err)
	}
	created, err := client.CreateRepositoryTicket(ctx, repository.ID, daemon.RepositoryTicketCreateRequest{Actor: "ui-user", Title: "New"})
	if err != nil {
		t.Fatal(err)
	}
	if created.RepositoryID != repository.ID || created.RepositoryKey != repository.Key {
		t.Fatalf("created repository identity=%#v, want ID/key %q/%q", created, repository.ID, repository.Key)
	}
	updated, err := client.UpdateRepositoryTicket(ctx, repository.ID, "20260925-00001", daemon.RepositoryTicketUpdateRequest{Actor: "ui-user", Set: map[string]any{"title": "Updated"}})
	if err != nil {
		t.Fatal(err)
	}
	if updated.RepositoryID != repository.ID || updated.RepositoryKey != repository.Key {
		t.Fatalf("updated repository identity=%#v, want ID/key %q/%q", updated, repository.ID, repository.Key)
	}
	for _, operation := range []string{"claim", "release", "open", "hold", "submit", "review", "approve", "close", "reject", "bump"} {
		result, err := client.MutateRepositoryTicket(ctx, repository.ID, "20260925-00001", operation, daemon.RepositoryTicketMutationRequest{Actor: "ui-user"})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		if result.RepositoryID != repository.ID || result.RepositoryKey != repository.Key {
			t.Fatalf("%s repository identity=%#v, want ID/key %q/%q", operation, result, repository.ID, repository.Key)
		}
	}
	if mutations != 12 {
		t.Fatalf("mutation count=%d, want create + update + 10 workflow operations", mutations)
	}
	stream, err := client.Events(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next()
	if err != nil || event.Type != "ticket.repository_changed" || event.RepositoryID != repository.ID || event.RepositoryKey != repository.Key {
		t.Fatalf("repository event=%#v err=%v", event, err)
	}
}
