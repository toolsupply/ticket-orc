package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRepositoryDeleteAuthenticationAndAvailability(t *testing.T) {
	for _, available := range []bool{false, true} {
		calls := 0
		gateway := RepositoryGateway{}
		if available {
			gateway.DeleteTicket = func(_ context.Context, repository, id string, request RepositoryTicketDeleteRequest) (RepositoryTicketDeletion, error) {
				calls++
				if repository != testRepositoryID || id != "20261017-00001" || request.Actor != "ui" {
					t.Errorf("delete target=%s/%s request=%#v", repository, id, request)
				}
				return RepositoryTicketDeletion{RepositoryID: repository, ID: id, Operation: "delete", Changed: true}, nil
			}
		}
		server, err := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, RepositoryGateway: gateway})
		if err != nil {
			t.Fatal(err)
		}
		server.lifecycle = context.Background()
		server.started = true
		handler := server.handler()
		path := "/v1/repositories/" + testRepositoryID + "/tickets/20261017-00001/delete"
		for _, authenticated := range []bool{false, true} {
			url := path
			if authenticated {
				url = "/" + testEndpointKey + path
			}
			response := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(`{"actor":"ui"}`))
			req.Header.Set("Content-Type", "application/json")
			handler.ServeHTTP(response, req)
			want := http.StatusNotFound
			if authenticated {
				want = http.StatusUnprocessableEntity
				if available {
					want = http.StatusOK
				}
			}
			if response.Code != want {
				t.Fatalf("available=%v authenticated=%v status=%d body=%s", available, authenticated, response.Code, response.Body.String())
			}
		}
		if (calls == 1) != available {
			t.Fatalf("available=%v calls=%d", available, calls)
		}
		capabilities := server.capabilities().Repositories
		if capabilities.TicketDelete != available || (len(capabilities.TicketActions) == 1) != available {
			t.Fatalf("capabilities=%#v", capabilities)
		}
	}
}
