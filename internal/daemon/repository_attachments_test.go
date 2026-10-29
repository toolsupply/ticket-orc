package daemon

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestAttachmentCapabilitiesReflectWiredOperations(t *testing.T) {
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if server.capabilities().Repositories.TicketAttachments != nil {
		t.Fatal("absent service advertised")
	}
	server.config.RepositoryGateway = RepositoryGateway{ListAttachments: func(context.Context, string, string, RepositoryAttachmentQuery) (attachments.Listing, error) {
		return attachments.Listing{}, nil
	}}
	c := server.capabilities().Repositories.TicketAttachments
	if c == nil || !c.List || c.Download || c.Upload || c.Mkdir || c.Move || c.Delete || c.RecursiveDelete {
		t.Fatalf("partial capabilities: %+v", c)
	}
	l := attachments.V1Limits()
	raw, _ := json.Marshal(c)
	var fields map[string]any
	json.Unmarshal(raw, &fields)
	if c.MaxFileBytes != l.MaxFileBytes || c.MaxJSONBodyBytes != l.MaxJSONBodyBytes || c.MaxCursorBytes != l.MaxCursorBytes || c.TransferTimeoutSeconds != int64(l.TransferTimeout.Seconds()) || len(fields) != 24 {
		t.Fatalf("capability limits: %s", raw)
	}
}

func TestAttachmentRouteRejectsAmbiguousInputs(t *testing.T) {
	unexpected := func() { t.Error("invalid request reached gateway") }
	g := RepositoryGateway{
		ListAttachments: func(context.Context, string, string, RepositoryAttachmentQuery) (attachments.Listing, error) {
			unexpected()
			return attachments.Listing{}, nil
		},
		WriteAttachment: func(context.Context, string, string, RepositoryAttachmentRequest, io.Reader, int64, attachments.CommitContext) (attachments.Receipt, error) {
			unexpected()
			return attachments.Receipt{}, nil
		},
		DeleteAttachment: func(context.Context, string, string, RepositoryAttachmentRequest) (attachments.Receipt, error) {
			unexpected()
			return attachments.Receipt{}, nil
		},
	}
	server, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), RepositoryGateway: g})
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/repositories/" + testRepositoryID + "/tickets/20261025-00001/attachments"
	for _, tc := range []struct {
		method, path, match, none, actor string
		status                           int
		code                             string
	}{
		{"GET", base + "/", "", "", "", 404, "repository_endpoint_not_found"},
		{"PATCH", base, "", "", "", 405, "method_not_allowed"},
		{"GET", strings.Replace(base, "20261025-00001", "short", 1), "", "", "", 400, "invalid_ticket"},
		{"GET", base + "?path=../private", "", "", "", 400, "invalid_path"},
		{"GET", base + "?path=%252e%252e", "", "", "", 400, "invalid_path"},
		{"GET", base + "?path=a%5Cb", "", "", "", 400, "invalid_path"},
		{"GET", base + "?path=a&path=b", "", "", "", 400, "invalid_request"},
		{"GET", base + "?limit=1&limit=2", "", "", "", 400, "invalid_request"},
		{"GET", base + "?limit=", "", "", "", 400, "invalid_request"},
		{"GET", base + "?x=y", "", "", "", 400, "invalid_request"},
		{"GET", base + "?path=%zz", "", "", "", 400, "invalid_request"},
		{"GET", strings.Replace(base, testRepositoryID, "repo", 1), "", "", "", 404, "repository_not_found"},
		{"PUT", base + "/content?path=a", "", "", "ui-user", 428, "precondition_required"},
		{"PUT", base + "/content?path=a", "*", "", "ui-user", 400, "invalid_precondition"},
		{"PUT", base + "/content?path=a", "", "*", "bad/path", 400, "invalid_actor"},
		{"PUT", base + "/content?path=a", "", "*", "ui-user", 415, "unsupported_media_type"},
		{"PUT", base + "/content?path=a", "bad", "*", "ui-user", 400, "invalid_precondition"},
		{"DELETE", base + "?path=a&recursive=false", "", "", "ui-user", 422, "capability_unavailable"},
		{"DELETE", base + "?path=a", "", "*", "ui-user", 400, "invalid_precondition"},
	} {
		// Unsupported mutation platforms reject the operation before parsing
		// write-specific headers. Still exercise every request and verify that
		// none reaches the gateway; supported platforms check detailed errors.
		if !attachments.MutationsAvailable() && (tc.method == "PUT" || tc.method == "DELETE") {
			tc.status, tc.code = 422, "capability_unavailable"
		}
		t.Run(tc.method+tc.path+tc.code, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.match != "" {
				req.Header.Set("If-Match", tc.match)
			}
			if tc.none != "" {
				req.Header.Set("If-None-Match", tc.none)
			}
			if tc.actor != "" {
				req.Header.Set("X-Ticket-Actor", tc.actor)
			}
			out := httptest.NewRecorder()
			server.handleRepositories(out, req)
			if out.Code != tc.status || !strings.Contains(out.Body.String(), `"code":"`+tc.code+`"`) {
				t.Fatalf("%d %s", out.Code, out.Body.String())
			}
			if tc.method == "PUT" || tc.method == "DELETE" {
				if !strings.Contains(out.Body.String(), `"mutation_applied":false`) {
					t.Fatalf("rejected mutation lost certainty: %s", out.Body.String())
				}
			}
		})
	}
}

func TestAttachmentInvalidationSSEPreservesSafeAttribution(t *testing.T) {
	for _, actor := range []string{"ui-user", "private/path", strings.Repeat("a", 129)} {
		event := RuntimeEventDTO(supervisor.RuntimeEvent{Type: "ticket.repository_changed", RepositoryID: testRepositoryID, Ticket: "20261025-00001", Actor: actor, Code: "attachments_changed", Applied: true})
		data, err := encodeSSEEvent(event)
		if err != nil {
			t.Fatal(err)
		}
		if actor == "ui-user" {
			if !strings.Contains(string(data), `"actor":"ui-user"`) || !strings.Contains(string(data), `"repository_id":"`+testRepositoryID+`"`) || !strings.Contains(string(data), `"mutation_applied":true`) {
				t.Fatalf("SSE=%s", data)
			}
		} else if strings.Contains(string(data), `"actor"`) {
			t.Fatalf("unsafe attribution: %s", data)
		}
	}
}

func TestAttachmentConditionsAndCertainty(t *testing.T) {
	revision := "a1_" + strings.Repeat("A", 43)
	for _, tc := range []struct {
		match, none             []string
		createOnly, replaceOnly bool
		code                    string
	}{
		{nil, nil, false, false, "precondition_required"},
		{[]string{`"` + revision + `"`}, nil, false, false, ""},
		{nil, []string{"*"}, true, false, ""},
		{[]string{`W/"` + revision + `"`}, nil, false, false, "invalid_precondition"},
		{[]string{`"` + revision + `", "` + revision + `"`}, nil, false, false, "invalid_precondition"},
		{[]string{`"` + revision + `"`}, nil, true, false, "invalid_precondition"},
		{[]string{`"` + revision + `"`, `"` + revision + `"`}, nil, false, false, "invalid_precondition"},
		{nil, []string{"*", "*"}, false, false, "invalid_precondition"},
		{nil, []string{"*"}, false, true, "invalid_precondition"},
	} {
		req := httptest.NewRequest("PUT", "/", nil)
		req.Header["If-Match"] = tc.match
		req.Header["If-None-Match"] = tc.none
		_, err := attachmentCondition(req, tc.createOnly, tc.replaceOnly)
		if tc.code == "" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil || err.(*attachments.Error).Code != tc.code {
			t.Fatalf("condition error=%v want %s", err, tc.code)
		}
	}
	server := &Server{}
	for _, tc := range []struct {
		err       *RepositoryMutationError
		certainty string
	}{
		{&RepositoryMutationError{Status: 503, Code: "mutation_outcome_unknown"}, ""},
		{&RepositoryMutationError{Status: 502, Code: "attachment_storage_failed", AppliedKnown: true, Applied: true}, `"mutation_applied":true`},
		{&RepositoryMutationError{Status: 412, Code: "revision_conflict", AppliedKnown: true}, `"mutation_applied":false`},
	} {
		out := httptest.NewRecorder()
		server.writeAttachmentReceipt(out, attachments.Receipt{}, tc.err, false)
		if tc.certainty == "" {
			if strings.Contains(out.Body.String(), "mutation_applied") {
				t.Fatal(out.Body.String())
			}
		} else if !strings.Contains(out.Body.String(), tc.certainty) {
			t.Fatal(out.Body.String())
		}
	}
}
