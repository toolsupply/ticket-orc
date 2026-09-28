package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testRepositoryID = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"

func TestHandleRepositoriesUsesConfiguredKeysAndHealth(t *testing.T) {
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Repositories: []RepositoryStatus{{ID: testRepositoryID, Key: "orc", Name: "Ticket Orc", State: "healthy"}}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/repositories", nil)
	recorder := httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Repositories []RepositoryStatus `json:"repositories"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Repositories) != 1 || response.Repositories[0].ID != testRepositoryID || response.Repositories[0].Key != "orc" || response.Repositories[0].State != "healthy" {
		t.Fatalf("repositories=%#v", response.Repositories)
	}
}

func TestRepositoryRoutesUseStableTicketIDAcrossConfigKeyRename(t *testing.T) {
	repository := RepositoryStatus{ID: testRepositoryID, Key: "project", Name: "Ticket project", State: "healthy"}
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Repositories: []RepositoryStatus{repository}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID, nil)
	recorder := httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"id":"`+testRepositoryID+`"`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"key":"project"`)) {
		t.Fatalf("UUID route status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/repositories/project", nil)
	recorder = httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusNotFound || !bytes.Contains(recorder.Body.Bytes(), []byte(`"code":"repository_not_found"`)) {
		t.Fatalf("key route status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	repository.Key = "backend"
	request = httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID, nil)
	recorder = httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"id":"`+testRepositoryID+`"`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"key":"backend"`)) {
		t.Fatalf("renamed UUID route status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestDynamicRepositoryHTTPContractUsesCapabilityAndStableID(t *testing.T) {
	var gotRepositoryID string
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		Status: func() Status {
			return Status{Repositories: []RepositoryStatus{{ID: testRepositoryID, Key: "dynamic:" + testRepositoryID, Name: "Ticket project", State: "healthy"}}}
		},
		RepositoryGateway: RepositoryGateway{ListTickets: func(_ context.Context, repositoryID string, _ RepositoryTicketQuery) (RepositoryTicketList, error) {
			gotRepositoryID = repositoryID
			return RepositoryTicketList{RepositoryID: repositoryID, Items: []RepositoryTicket{{ID: "20260926-00001", State: "open"}}}, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	handler := server.handler()

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/repositories", nil))
	if unauthorized.Code != http.StatusNotFound {
		t.Fatalf("unauthenticated repository list status=%d body=%q", unauthorized.Code, unauthorized.Body.String())
	}

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/repositories", nil))
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"id":"`+testRepositoryID+`"`)) {
		t.Fatalf("dynamic repository list status=%d body=%q", list.Code, list.Body.String())
	}

	tickets := httptest.NewRecorder()
	handler.ServeHTTP(tickets, httptest.NewRequest(http.MethodGet, "/"+testEndpointKey+"/v1/repositories/"+testRepositoryID+"/tickets", nil))
	if tickets.Code != http.StatusOK || gotRepositoryID != testRepositoryID || !bytes.Contains(tickets.Body.Bytes(), []byte(`"id":"20260926-00001"`)) {
		t.Fatalf("dynamic Ticket gateway status=%d repository=%q body=%q", tickets.Code, gotRepositoryID, tickets.Body.String())
	}

	for _, route := range []string{"join", "leave"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/"+testEndpointKey+"/v1/"+route, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("bootstrap route /v1/%s status=%d body=%q", route, response.Code, response.Body.String())
		}
	}
}

func TestParseRepositoryTicketQueryBoundsAndStates(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?state=open&state=closed&limit=12&offset=3", nil)
	query, err := parseRepositoryTicketQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if query.Search != "" || query.Limit != 12 || query.Offset != 3 || len(query.States) != 2 || query.States[0] != "open" || query.States[1] != "closed" {
		t.Fatalf("query=%#v", query)
	}
	searchRequest := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?q=parser&limit=12&offset=3", nil)
	search, err := parseRepositoryTicketQuery(searchRequest)
	if err != nil || search.Search != "parser" || len(search.States) != 0 {
		t.Fatalf("search query=%#v err=%v", search, err)
	}
	for _, raw := range []string{"limit=0", "limit=257", "offset=-1", "state=unknown"} {
		request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?"+raw, nil)
		if _, err := parseRepositoryTicketQuery(request); err == nil {
			t.Fatalf("accepted invalid query %q", raw)
		}
	}
}

func TestParseRepositoryTicketQueryCombinesExactFiltersAndSearch(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?state=open&state=review&priority=1&assignee=coder&tag=api&tag=ticket-ui&q=parent%20filter&limit=7&offset=3", nil)
	query, err := parseRepositoryTicketQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if query.Search != "parent filter" || len(query.States) != 2 || query.States[0] != "open" || query.States[1] != "review" || query.Priority == nil || *query.Priority != 1 || query.Assignee != "coder" || len(query.Tags) != 2 || query.Tags[0] != "api" || query.Tags[1] != "ticket-ui" || query.Limit != 7 || query.Offset != 3 {
		t.Fatalf("query=%#v", query)
	}

	for _, raw := range []string{"priority=abc", "priority=-1", "priority=5", "priority=1&priority=2", "assignee=a&assignee=b", "state=all&state=open"} {
		request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?"+raw, nil)
		if _, err := parseRepositoryTicketQuery(request); err == nil {
			t.Errorf("accepted invalid query %q", raw)
		}
	}
}

func TestParseRepositoryTicketQueryAcceptsEachFilterIndependently(t *testing.T) {
	tests := []struct {
		name  string
		query string
		check func(RepositoryTicketQuery) bool
	}{
		{name: "search", query: "q=parser", check: func(q RepositoryTicketQuery) bool { return q.Search == "parser" }},
		{name: "state", query: "state=open", check: func(q RepositoryTicketQuery) bool { return len(q.States) == 1 && q.States[0] == "open" }},
		{name: "priority", query: "priority=0", check: func(q RepositoryTicketQuery) bool { return q.Priority != nil && *q.Priority == 0 }},
		{name: "assignee", query: "assignee=coder", check: func(q RepositoryTicketQuery) bool { return q.Assignee == "coder" }},
		{name: "tag", query: "tag=api", check: func(q RepositoryTicketQuery) bool { return len(q.Tags) == 1 && q.Tags[0] == "api" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/repositories/"+testRepositoryID+"/tickets?"+test.query, nil)
			query, err := parseRepositoryTicketQuery(request)
			if err != nil || !test.check(query) {
				t.Fatalf("query=%#v err=%v", query, err)
			}
		})
	}
}

func TestRepositoryGatewaySetterOnlyBeforeStart(t *testing.T) {
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.SetRepositoryGateway(RepositoryGateway{ListTickets: func(context.Context, string, RepositoryTicketQuery) (RepositoryTicketList, error) {
		return RepositoryTicketList{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	server.started = true
	if err := server.SetRepositoryGateway(RepositoryGateway{}); err == nil {
		t.Fatal("gateway changed after server start")
	}
}

func TestRepositoryMutationRoutesUseTypedActorAndOperation(t *testing.T) {
	var gotKey, gotID, gotOperation, gotActor string
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		RepositoryGateway: RepositoryGateway{
			MutateTicket: func(_ context.Context, key, id, operation string, request RepositoryTicketMutationRequest) (RepositoryTicketMutation, error) {
				gotKey, gotID, gotOperation, gotActor = key, id, operation, request.Actor
				return RepositoryTicketMutation{RepositoryID: key, RepositoryKey: "project", Operation: operation, Changed: true, Ticket: RepositoryTicket{ID: id, State: "closed"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/"+testRepositoryID+"/tickets/20260922-00001/close", bytes.NewBufferString(`{"actor":"ui-user","outcome":"accepted"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusOK || gotKey != testRepositoryID || gotID != "20260922-00001" || gotOperation != "close" || gotActor != "ui-user" {
		t.Fatalf("status=%d key=%q id=%q operation=%q actor=%q body=%q", recorder.Code, gotKey, gotID, gotOperation, gotActor, recorder.Body.String())
	}
}

func TestRepositoryMutationRejectsUnknownInputAndMissingActor(t *testing.T) {
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		RepositoryGateway: RepositoryGateway{MutateTicket: func(context.Context, string, string, string, RepositoryTicketMutationRequest) (RepositoryTicketMutation, error) {
			return RepositoryTicketMutation{}, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	for _, body := range []string{`{"actor":""}`, `{"actor":"ui","extra":true}`} {
		request := httptest.NewRequest(http.MethodPost, "/v1/repositories/"+testRepositoryID+"/tickets/20260922-00001/claim", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		server.handleRepositories(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d response=%q", body, recorder.Code, recorder.Body.String())
		}
	}
}

func TestRepositoryUpdateAliasUsesTypedUpdateGateway(t *testing.T) {
	var gotKey, gotID, gotActor string
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		RepositoryGateway: RepositoryGateway{UpdateTicket: func(_ context.Context, key, id string, request RepositoryTicketUpdateRequest) (RepositoryTicketMutation, error) {
			gotKey, gotID, gotActor = key, id, request.Actor
			return RepositoryTicketMutation{RepositoryID: key, RepositoryKey: "project", Operation: "update", Changed: true, Ticket: RepositoryTicket{ID: id, State: "open"}}, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/"+testRepositoryID+"/tickets/20260922-00001/update", bytes.NewBufferString(`{"actor":"ui","set":{"title":"new"}}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusOK || gotKey != testRepositoryID || gotID != "20260922-00001" || gotActor != "ui" {
		t.Fatalf("status=%d key=%q id=%q actor=%q body=%q", recorder.Code, gotKey, gotID, gotActor, recorder.Body.String())
	}
}

func TestRepositoryMutationRejectsUnknownOperationBeforeGateway(t *testing.T) {
	called := false
	server, err := NewServer(Config{
		EndpointKey: testEndpointKey,
		StateDir:    t.TempDir(),
		RepositoryGateway: RepositoryGateway{MutateTicket: func(context.Context, string, string, string, RepositoryTicketMutationRequest) (RepositoryTicketMutation, error) {
			called = true
			return RepositoryTicketMutation{}, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.lifecycle = context.Background()
	server.started = true
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/"+testRepositoryID+"/tickets/20260922-00001/exec", bytes.NewBufferString(`{"actor":"ui"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.handleRepositories(recorder, request)
	if recorder.Code != http.StatusNotFound || called {
		t.Fatalf("status=%d called=%t body=%q", recorder.Code, called, recorder.Body.String())
	}
}
