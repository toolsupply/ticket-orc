package daemonclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
)

const attachmentStatus = `{"capabilities":{"repositories":{"ticket_attachments":{"list":true,"download":true,"upload":true,"mkdir":true,"move":true,"delete":true,"revision_scope":"ticket"}}}}`

func TestAttachmentMissingAndUnknownCapabilitiesDoNotMutate(t *testing.T) {
	for _, status := range []string{`{}`, `{"capabilities":{"repositories":{"ticket_attachments":{"upload":true,"revision_scope":"unknown"}}}}`} {
		calls := 0
		c := newRepositoryTestClient(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.URL.Path != "/capability/v1/status" {
				t.Fatal("operation sent")
			}
			return repositoryResponse(r, 200, "application/json", status), nil
		})
		body := &attachmentCloseProbe{Reader: strings.NewReader("data")}
		_, err := c.WriteRepositoryAttachment(t.Context(), testRepositoryID, "20261025-00001", daemon.RepositoryAttachmentRequest{}, body, 4)
		var e *Error
		if !errors.As(err, &e) || e.Code != "capability_unavailable" || calls != 1 || body.closes.Load() != 0 {
			t.Fatalf("err=%v calls=%d closes=%d", err, calls, body.closes.Load())
		}
	}
}

type attachmentCloseProbe struct {
	io.Reader
	closes atomic.Int32
}

func (b *attachmentCloseProbe) Close() error { b.closes.Add(1); return nil }
func TestAttachmentWriteOwnershipAndUnknownOutcome(t *testing.T) {
	for _, mode := range []string{"transport", "unknown", "applied", "rejected", "redirect", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			c := newRepositoryTestClient(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/capability/v1/status" {
					return repositoryResponse(r, 200, "application/json", attachmentStatus), nil
				}
				calls++
				if r.Method != "PUT" || r.GetBody != nil || r.Header.Get("X-Ticket-Actor") != "ui-user" || r.Header.Get("If-None-Match") != "*" || r.URL.Query().Get("path") != "x y.zip" {
					t.Errorf("request=%+v", r)
				}
				if r.Body != nil {
					defer r.Body.Close()
				}
				switch mode {
				case "transport":
					return nil, io.ErrUnexpectedEOF
				case "unknown":
					return repositoryResponse(r, 503, "application/json", `{"error":{"code":"mutation_outcome_unknown"}}`), nil
				case "applied":
					return repositoryResponse(r, 502, "application/json", `{"error":{"code":"attachment_storage_failed","mutation_applied":true}}`), nil
				case "rejected":
					return repositoryResponse(r, 412, "application/json", `{"error":{"code":"revision_conflict","mutation_applied":false}}`), nil
				case "redirect":
					res := repositoryResponse(r, 307, "application/json", `{}`)
					res.Header.Set("Location", "/replay")
					return res, nil
				default:
					return repositoryResponse(r, 200, "application/json", strings.Repeat("x", maxResponseBytes+1)), nil
				}
			})
			b := &attachmentCloseProbe{Reader: strings.NewReader("zip")}
			_, err := c.WriteRepositoryAttachment(t.Context(), testRepositoryID, "20261025-00001", daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "x y.zip", Condition: attachments.Condition{Create: true}}, b, 3)
			var e *Error
			if !errors.As(err, &e) || calls != 1 || b.closes.Load() != 1 {
				t.Fatalf("err=%v calls=%d closes=%d", err, calls, b.closes.Load())
			}
			if e.AppliedKnown != (mode == "applied" || mode == "rejected") || e.Applied != (mode == "applied") {
				t.Fatalf("certainty=%+v", e)
			}
		})
	}
}

func TestAttachmentDownloadCancellationAndClose(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			finished := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					io.WriteString(w, attachmentStatus)
					return
				}
				defer close(finished)
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", "1000000")
				w.Header().Set("ETag", `"a1_`+strings.Repeat("A", 43)+`"`)
				w.Header().Set("Content-Disposition", `attachment; filename="bytes.zip"`)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() { cancel(); server.Close() })
			c := &Client{endpoint: daemon.Endpoint{URL: server.URL, EndpointKey: "key"}, http: server.Client()}
			d, err := c.ReadRepositoryAttachment(ctx, testRepositoryID, "20261025-00001", "bytes.zip")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { d.Content.Close() })
			done := make(chan error, 1)
			exited := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				d.Content.Close()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("download reader did not exit")
				}
			})
			go func() { defer close(exited); _, e := io.ReadAll(d.Content); done <- e }()
			if mode == "cancel" {
				cancel()
			} else {
				d.Content.Close()
			}
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("truncated transfer succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("body read did not unblock")
			}
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not observe cancellation")
			}
		})
	}
}

func TestAttachmentGapReconcilesWithTypedList(t *testing.T) {
	c := newRepositoryTestClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			return repositoryResponse(r, 200, "application/json", attachmentStatus), nil
		case strings.HasSuffix(r.URL.Path, "/events"):
			return repositoryResponse(r, 200, "text/event-stream", "id: 1\ndata: {\"seq\":1,\"type\":\"stream.sync\"}\n\nid: 3\ndata: {\"seq\":3,\"type\":\"ticket.repository_changed\",\"code\":\"attachments_changed\"}\n\n"), nil
		default:
			return repositoryResponse(r, 200, "application/json", `{"repository_id":"`+testRepositoryID+`","ticket_id":"20261025-00001","directory":"","revision":"a1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","entries":[]}`), nil
		}
	})
	stream, err := c.Events(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next()
	if !errors.Is(err, ErrEventGap) || event.Code != "attachments_changed" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	listing, err := c.ListRepositoryAttachments(t.Context(), testRepositoryID, "20261025-00001", daemon.RepositoryAttachmentQuery{})
	if err != nil || listing.RepositoryID != testRepositoryID || len(listing.Entries) != 0 {
		t.Fatalf("reconciliation=%+v err=%v", listing, err)
	}
}

func TestAttachmentListingResponseBudgetAndClosure(t *testing.T) {
	// Verify the larger success budget does not widen error/receipt decoding, and
	// that oversized responses stop at one sentinel byte and close their body.
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			budget := maxResponseBytes
			if status == http.StatusOK {
				budget = attachmentListingResponseBytes()
			}
			reader := strings.NewReader(strings.Repeat(" ", budget+2))
			body := &attachmentCloseProbe{Reader: reader}
			c := newRepositoryTestClient(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					return repositoryResponse(r, 200, "application/json", attachmentStatus), nil
				}
				res := repositoryResponse(r, status, "application/json", "")
				res.Body = body
				return res, nil
			})
			_, err := c.ListRepositoryAttachments(t.Context(), testRepositoryID, "20261025-00001", daemon.RepositoryAttachmentQuery{})
			var ce *Error
			if !errors.As(err, &ce) || ce.Kind != ErrorProtocol || !strings.Contains(ce.Message, "too large") || body.closes.Load() != 1 || reader.Len() != 1 {
				t.Fatalf("error=%v closes=%d remaining=%d", err, body.closes.Load(), reader.Len())
			}
		})
	}
}
