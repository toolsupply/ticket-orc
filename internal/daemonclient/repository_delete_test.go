package daemonclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestDeleteRepositoryTicketRequiresReceiptAndNeverRetries(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		transport, valid bool
	}{
		{"changed", `{"repository_id":"` + testRepositoryID + `","id":"20261017-00001","operation":"delete","changed":true}`, false, true},
		{"unchanged", `{"repository_id":"` + testRepositoryID + `","id":"20261017-00001","operation":"delete","changed":false}`, false, true},
		{"missing outcome", `{"repository_id":"` + testRepositoryID + `","id":"20261017-00001","operation":"delete"}`, false, false},
		{"missing receipt", `{}`, false, false},
		{"wrong target", `{"repository_id":"other","id":"20261017-00001","operation":"delete","changed":true}`, false, false},
		{"transport", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := newRepositoryTestClient(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.Method != http.MethodPost || req.URL.Path != "/capability/v1/repositories/"+testRepositoryID+"/tickets/20261017-00001/delete" {
					t.Errorf("request=%s %s", req.Method, req.URL)
				}
				data, err := io.ReadAll(req.Body)
				if err != nil || string(data) != `{"actor":"ui"}` {
					t.Errorf("request body=%s err=%v", data, err)
				}
				if tc.transport {
					return nil, errors.New("lost response")
				}
				return repositoryResponse(req, 200, "application/json", tc.body), nil
			})
			result, err := client.DeleteRepositoryTicket(context.Background(), testRepositoryID, "20261017-00001", daemon.RepositoryTicketDeleteRequest{Actor: "ui"})
			if calls != 1 || (err == nil) != tc.valid {
				t.Fatalf("calls=%d result=%#v err=%v", calls, result, err)
			}
			if !tc.valid {
				var failure *Error
				if !errors.As(err, &failure) || failure.AppliedKnown || failure.Applied {
					t.Fatalf("uncertain outcome=%#v", err)
				}
			}
		})
	}
}
