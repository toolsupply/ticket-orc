package cli

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

type repositoryTicketReaderStub struct {
	candidates ticketclient.ListResult
	listed     ticketclient.ListResult
	search     string
	query      ticketclient.ListQuery
	listCalls  int
}

type repositoryTicketDetailReaderStub struct {
	detail ticketclient.TicketDetail
	err    error
}

func (stub repositoryTicketDetailReaderStub) ShowDetail(context.Context, string) (ticketclient.TicketDetail, error) {
	return stub.detail, stub.err
}

func (stub *repositoryTicketReaderStub) SearchTicketCandidates(_ context.Context, expression string) (ticketclient.ListResult, error) {
	stub.search = expression
	return stub.candidates, nil
}

func (stub *repositoryTicketReaderStub) ListTickets(_ context.Context, query ticketclient.ListQuery) (ticketclient.ListResult, error) {
	stub.listCalls++
	stub.query = query
	return stub.listed, nil
}

func TestRepositoryGatewayLookupUsesTicketIDOnly(t *testing.T) {
	repository := supervisor.ConfiguredRepository{
		Key: "project", ID: joinTestRepositoryID,
		Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/repo/project"},
	}
	manager := &workerManager{repositoriesByID: repositoryRegistryByID(supervisor.RepositoryRegistry{"project": repository})}
	resolved, err := manager.repositoryTarget(joinTestRepositoryID)
	if err != nil || resolved.Key != "project" || resolved.Target.Repository != "/repo/project" {
		t.Fatalf("ID lookup resolved=%#v err=%v", resolved, err)
	}
	if _, err := manager.repositoryTarget("project"); err == nil {
		t.Fatal("repository key was accepted as an API identity")
	}
}

func TestListRepositoryTicketsCombinesSearchFiltersBeforePagination(t *testing.T) {
	priority := 2
	reader := &repositoryTicketReaderStub{
		candidates: ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260925-00001"}, {ID: "20260925-00002"}}},
		listed:     ticketclient.ListResult{Items: []ticketclient.Ticket{{ID: "20260925-00002", Title: "match", State: "open", Priority: 2, Assignee: "coder", Parent: "20260924-00003", Tags: []string{"ui"}}}, More: true},
	}
	query := daemon.RepositoryTicketQuery{
		Search: "parent filter", States: []string{"open", "review"}, Priority: &priority,
		Assignee: "coder", Tags: []string{"ui", "api"}, Limit: 1, Offset: 4,
	}
	result, err := listRepositoryTicketsFromClient(context.Background(), joinTestRepositoryID, "repo", query, reader, query.Limit)
	if err != nil {
		t.Fatal(err)
	}
	if reader.search != query.Search || reader.listCalls != 1 {
		t.Fatalf("search=%q listCalls=%d", reader.search, reader.listCalls)
	}
	if got := reader.query; !reflect.DeepEqual(got, ticketclient.ListQuery{
		IDs: []string{"20260925-00001", "20260925-00002"}, States: query.States, Priority: &priority,
		Assignee: "coder", Tags: query.Tags, Limit: 1, Offset: 4,
	}) {
		t.Fatalf("Ticket query=%#v", got)
	}
	if result.RepositoryID != joinTestRepositoryID || result.RepositoryKey != "repo" || len(result.Items) != 1 || result.Items[0].ID != "20260925-00002" || result.Items[0].Parent != "20260924-00003" || !result.More {
		t.Fatalf("repository result=%#v", result)
	}
}

func TestListRepositoryTicketsSkipsListWhenSearchHasNoCandidates(t *testing.T) {
	reader := &repositoryTicketReaderStub{}
	result, err := listRepositoryTicketsFromClient(context.Background(), joinTestRepositoryID, "repo", daemon.RepositoryTicketQuery{Search: "no match"}, reader, 50)
	if err != nil {
		t.Fatal(err)
	}
	if reader.listCalls != 0 || result.RepositoryID != joinTestRepositoryID || result.RepositoryKey != "repo" || len(result.Items) != 0 || result.Items == nil || result.More {
		t.Fatalf("listCalls=%d result=%#v", reader.listCalls, result)
	}
}

func TestRepositoryTicketDetailViewMapsAuthoritativeDisplayFields(t *testing.T) {
	view := repositoryTicketDetailView(ticketclient.TicketDetail{
		ID: "20260925-00001", State: "open", Created: "2026-09-25", Modified: "2026-09-25T12:00:00Z",
		Readiness: &ticketclient.TicketReadiness{Ready: false, Blockers: []ticketclient.TicketReadinessBlocker{{Code: "dependency_open", ID: "20260924-00001", Message: "dependency is open"}}},
		Body:      "display body", BodyTruncated: true, Truncated: true,
	})
	if view.Readiness == nil || view.Readiness.Ready || len(view.Readiness.Blockers) != 1 || view.Readiness.Blockers[0].ID != "20260924-00001" || view.Created != "2026-09-25" || view.Modified != "2026-09-25T12:00:00Z" || view.Body == nil || *view.Body != "display body" || view.BodyTruncated == nil || !*view.BodyTruncated || !view.Truncated {
		t.Fatalf("detail view=%#v", view)
	}
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	if strings.Contains(encoded, "path") || strings.Contains(encoded, "raw command") || !strings.Contains(encoded, `"body_truncated":true`) {
		t.Fatalf("unsafe or incomplete detail response: %s", encoded)
	}
}

func TestRepositoryTicketListViewOmitsDetailOnlyFields(t *testing.T) {
	data, err := json.Marshal(repositoryTicketView(ticketclient.Ticket{ID: "20260925-00001", State: "open"}))
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	for _, field := range []string{"readiness", "created", "modified", "body", "body_truncated"} {
		if strings.Contains(encoded, `"`+field+`"`) {
			t.Fatalf("list view includes detail-only field %q: %s", field, encoded)
		}
	}
}

func TestRepositoryMutationResponseCarriesRepositoryIDAndKey(t *testing.T) {
	reader := repositoryTicketDetailReaderStub{detail: ticketclient.TicketDetail{ID: "20260925-00001", State: "closed"}}
	result, err := rereadRepositoryMutation(context.Background(), reader, joinTestRepositoryID, "project", "close", "20260925-00001", true)
	if err != nil {
		t.Fatal(err)
	}
	if result.RepositoryID != joinTestRepositoryID || result.RepositoryKey != "project" || result.Operation != "close" || !result.Changed || result.Ticket.ID != "20260925-00001" {
		t.Fatalf("repository mutation response=%#v", result)
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"repository":`) || !strings.Contains(string(data), `"repository_id":"`+joinTestRepositoryID+`"`) || !strings.Contains(string(data), `"repository_key":"project"`) {
		t.Fatalf("repository mutation JSON=%s", data)
	}
}

func TestRepositoryTicketDetailFailureIsSafeAndHasNoPartialResult(t *testing.T) {
	result, err := repositoryTicketDetailFromClient(context.Background(), "20260925-00001", repositoryTicketDetailReaderStub{err: errors.New("private raw command output at /private/TASK.md")})
	var readError *daemon.RepositoryReadError
	if !errors.As(err, &readError) || readError.Code != "repository_read_failed" || result.ID != "" {
		t.Fatalf("result=%#v err=%#v", result, err)
	}
	if strings.Contains(err.Error(), "private raw command output") || strings.Contains(err.Error(), "/private/TASK.md") {
		t.Fatalf("error leaked Ticket output: %v", err)
	}
}
