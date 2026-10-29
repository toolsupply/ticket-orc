package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// Route components are decoded once, independently of attachment-relative query
// paths. Encoded route separators cannot acquire routing meaning.
func (s *Server) routeAttachments(w http.ResponseWriter, r *http.Request) bool {
	parts := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/repositories/"), "/")
	if len(parts) < 4 {
		return false
	}
	operation, err := url.PathUnescape(parts[3])
	if err != nil || operation != "attachments" {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	fail := func(status int, code string) { attachmentHTTPError(w, status, code, r.Method != http.MethodGet) }
	if len(parts) > 5 || parts[1] != "tickets" {
		fail(400, "invalid_request")
		return true
	}
	repo, e1 := url.PathUnescape(parts[0])
	id, e2 := url.PathUnescape(parts[2])
	if e1 != nil || strings.ContainsAny(repo, "/\\%") || !ticketclient.ValidRepositoryID(repo) {
		fail(404, "repository_not_found")
		return true
	}
	if e2 != nil || ticketclient.ValidateFullID(id) != nil {
		fail(400, "invalid_ticket")
		return true
	}
	suffix := ""
	if len(parts) == 5 {
		suffix = parts[4]
		if suffix == "" {
			fail(404, "repository_endpoint_not_found")
			return true
		}
	}
	allowed := suffix == "" && (r.Method == "GET" || r.Method == "DELETE") || suffix == "content" && (r.Method == "GET" || r.Method == "PUT") || (suffix == "folders" || suffix == "move") && r.Method == "POST"
	if !allowed {
		fail(405, "method_not_allowed")
		return true
	}
	g := s.config.RepositoryGateway
	available := suffix == "" && r.Method == "GET" && g.ListAttachments != nil || suffix == "" && r.Method == "DELETE" && g.DeleteAttachment != nil || suffix == "content" && r.Method == "GET" && g.ReadAttachment != nil || suffix == "content" && r.Method == "PUT" && g.WriteAttachment != nil || suffix == "folders" && g.CreateAttachmentDirectory != nil || suffix == "move" && g.MoveAttachment != nil
	if !available || r.Method != "GET" && !attachments.MutationsAvailable() {
		fail(422, "capability_unavailable")
		return true
	}
	if len(r.Header.Values("Content-Encoding")) != 0 {
		fail(415, "unsupported_media_type")
		return true
	}
	for _, h := range []string{"Range", "If-Range", "Content-Range", "If-Modified-Since", "If-Unmodified-Since"} {
		if len(r.Header.Values(h)) != 0 {
			fail(400, "invalid_request")
			return true
		}
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fail(400, "invalid_request")
		return true
	}
	if _, exists := q["recursive"]; exists && r.Method == "DELETE" {
		fail(422, "capability_unavailable")
		return true
	}
	for key, values := range q {
		if len(values) != 1 || !(key == "path" && (suffix == "" || suffix == "content") || suffix == "" && r.Method == "GET" && (key == "limit" || key == "cursor")) {
			fail(400, "invalid_request")
			return true
		}
	}
	p := q.Get("path")
	if suffix == "" || suffix == "content" {
		if err := attachments.ValidatePath(p, suffix == "" && r.Method == "GET"); err != nil {
			writeAttachmentDomainError(w, err, r.Method != "GET")
			return true
		}
	}
	if r.Method == "GET" || r.Method == "DELETE" {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			fail(400, "invalid_request")
			return true
		}
	}
	if r.Method == "GET" {
		for _, h := range []string{"If-Match", "If-None-Match", "X-Ticket-Actor"} {
			if len(r.Header.Values(h)) != 0 {
				fail(400, "invalid_request")
				return true
			}
		}
		if suffix == "content" {
			s.downloadAttachment(w, r, repo, id, p)
			return true
		}
		limits := attachments.V1Limits()
		limit := limits.DefaultPageEntries
		if values, ok := q["limit"]; ok {
			limit, err = strconv.Atoi(values[0])
			if err != nil || limit < 1 || limit > limits.MaxPageEntries {
				fail(400, "invalid_request")
				return true
			}
		}
		if len(q.Get("cursor")) > limits.MaxCursorBytes {
			fail(400, "invalid_cursor")
			return true
		}
		ctx, cancel, err := s.attachmentTransferContext(r.Context(), limits.CommitTimeout)
		if err != nil {
			fail(503, "repository_unavailable")
			return true
		}
		defer cancel()
		result, err := g.ListAttachments(ctx, repo, id, RepositoryAttachmentQuery{Path: p, Limit: limit, Cursor: q.Get("cursor")})
		if err != nil {
			writeRepositoryReadError(w, err)
		} else {
			writeJSON(w, 200, result)
		}
		return true
	}
	condition, err := attachmentCondition(r, suffix == "folders" || suffix == "move", r.Method == "DELETE")
	if err != nil {
		writeAttachmentDomainError(w, err, true)
		return true
	}
	req := RepositoryAttachmentRequest{Path: p, Condition: condition}
	if suffix == "content" || r.Method == "DELETE" {
		actors := r.Header.Values("X-Ticket-Actor")
		if len(actors) != 1 || ticketclient.ValidateActor(actors[0]) != nil {
			fail(400, "invalid_actor")
			return true
		}
		req.Actor = actors[0]
	} else if len(r.Header.Values("X-Ticket-Actor")) != 0 {
		fail(400, "invalid_actor")
		return true
	}
	if suffix == "content" {
		if !attachmentMediaType(r, "application/octet-stream") {
			fail(415, "unsupported_media_type")
			return true
		}
		if r.ContentLength > attachments.V1Limits().MaxFileBytes {
			fail(413, "limit_exceeded")
			return true
		}
		ctx, cancel, err := s.attachmentTransferContext(r.Context(), attachments.V1Limits().TransferTimeout)
		if err != nil {
			fail(503, "repository_unavailable")
			return true
		}
		defer cancel()
		stop, err := attachmentDeadlines(w, r, ctx, true)
		if err != nil {
			fail(422, "capability_unavailable")
			return true
		}
		defer stop()
		result, err := g.WriteAttachment(ctx, repo, id, req, r.Body, r.ContentLength, func() (context.Context, context.CancelFunc, error) {
			return s.MutationContext(r.Context(), attachments.V1Limits().CommitTimeout)
		})
		s.writeAttachmentReceipt(w, result, err, false)
		return true
	}
	var move RepositoryAttachmentMoveRequest
	if suffix == "folders" || suffix == "move" {
		bodyCtx, bodyCancel, bodyErr := s.attachmentTransferContext(r.Context(), attachments.V1Limits().CommitTimeout)
		if bodyErr != nil {
			fail(503, "repository_unavailable")
			return true
		}
		bodyStop, bodyErr := attachmentDeadlines(w, r, bodyCtx, true)
		if bodyErr != nil {
			bodyCancel()
			fail(422, "capability_unavailable")
			return true
		}
		fields, code, status := decodeAttachmentObject(w, r)
		if bodyCtx.Err() != nil {
			code, status = "invalid_request", 400
		}
		bodyStop()
		bodyCancel()
		if code != "" {
			fail(status, code)
			return true
		}
		keys := map[string]bool{"actor": true, "path": suffix == "folders", "from": suffix == "move", "to": suffix == "move", "revision": suffix == "move"}
		for key := range fields {
			if !keys[key] {
				fail(400, "invalid_request")
				return true
			}
		}
		req.Actor, req.Path = fields["actor"], fields["path"]
		if ticketclient.ValidateActor(req.Actor) != nil {
			fail(400, "invalid_actor")
			return true
		}
		if suffix == "folders" {
			err = attachments.ValidatePath(req.Path, false)
		} else {
			move = RepositoryAttachmentMoveRequest{Actor: req.Actor, From: fields["from"], To: fields["to"], Revision: fields["revision"], Condition: condition}
			err = attachments.ValidatePath(move.From, false)
			if err == nil {
				err = attachments.ValidatePath(move.To, false)
			}
			if err == nil {
				err = (attachments.Condition{Revision: move.Revision}).Validate(false)
			}
		}
		if err != nil {
			writeAttachmentDomainError(w, err, true)
			return true
		}
	}
	if err := r.Context().Err(); err != nil {
		fail(400, "invalid_request")
		return true
	}
	ctx, cancel, err := s.MutationContext(r.Context(), attachments.V1Limits().CommitTimeout)
	if err != nil {
		fail(503, "repository_unavailable")
		return true
	}
	defer cancel()
	var result attachments.Receipt
	switch suffix {
	case "folders":
		result, err = g.CreateAttachmentDirectory(ctx, repo, id, req)
	case "move":
		result, err = g.MoveAttachment(ctx, repo, id, move)
	default:
		result, err = g.DeleteAttachment(ctx, repo, id, req)
	}
	s.writeAttachmentReceipt(w, result, err, r.Method == "DELETE")
	return true
}

func attachmentMediaType(r *http.Request, want string) bool {
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mt, params, err := mime.ParseMediaType(values[0])
	return err == nil && mt == want && len(params) == 0
}

func attachmentCondition(r *http.Request, createOnly, replaceOnly bool) (attachments.Condition, error) {
	match, none := r.Header.Values("If-Match"), r.Header.Values("If-None-Match")
	c := attachments.Condition{}
	if len(match) == 0 && len(none) == 0 {
		return c, &attachments.Error{Code: "precondition_required"}
	}
	if len(match)+len(none) != 1 || len(match) > 1 || len(none) > 1 {
		return c, &attachments.Error{Code: "invalid_precondition"}
	}
	if len(none) == 1 {
		if none[0] != "*" || replaceOnly {
			return c, &attachments.Error{Code: "invalid_precondition"}
		}
		c.Create = true
	} else {
		v := match[0]
		if len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
			return c, &attachments.Error{Code: "invalid_precondition"}
		}
		c.Revision = v[1 : len(v)-1]
	}
	return c, c.Validate(createOnly)
}

// Token decoding rejects duplicate and case-aliased keys as well as unknown keys;
// every value is a literal string, never a JSON coercion or nested object.
func decodeAttachmentObject(w http.ResponseWriter, r *http.Request) (map[string]string, string, int) {
	if !attachmentMediaType(r, "application/json") {
		return nil, "unsupported_media_type", 415
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(attachments.V1Limits().MaxJSONBodyBytes)))
	var large *http.MaxBytesError
	if errors.As(err, &large) {
		return nil, "limit_exceeded", 413
	}
	if err != nil || !utf8.Valid(raw) {
		return nil, "invalid_request", 400
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, "invalid_request", 400
	}
	fields := map[string]string{}
	for dec.More() {
		token, err = dec.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, "invalid_request", 400
		}
		if _, exists := fields[key]; exists {
			return nil, "invalid_request", 400
		}
		var value any
		if dec.Decode(&value) != nil {
			return nil, "invalid_request", 400
		}
		str, ok := value.(string)
		if !ok {
			return nil, "invalid_request", 400
		}
		fields[key] = str
	}
	if _, err = dec.Token(); err != nil {
		return nil, "invalid_request", 400
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, "invalid_request", 400
	}
	return fields, "", 0
}

// Admission uses the existing server lifecycle without detaching the transfer
// from its caller. Only a fully staged upload may request a separate commit.
func (s *Server) attachmentTransferContext(request context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	ctx, cancel, err := s.MutationContext(request, timeout)
	if err != nil {
		return nil, nil, err
	}
	stop := context.AfterFunc(request, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// Deadlines interrupt blocked network IO on timeout/shutdown. No body-reader
// goroutine is detached. Wait for the cancellation callback before clearing a
// deadline so it cannot race with a subsequent keep-alive request.
func attachmentDeadlines(w http.ResponseWriter, r *http.Request, ctx context.Context, upload bool) (func(), error) {
	ctl := http.NewResponseController(w)
	set := ctl.SetWriteDeadline
	if upload {
		set = ctl.SetReadDeadline
	}
	deadline, _ := ctx.Deadline()
	if err := set(deadline); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = set(time.Now()); close(done) })
	return func() {
		if !stop() {
			<-done
		}
		_ = set(time.Time{})
	}, nil
}

func (s *Server) downloadAttachment(w http.ResponseWriter, r *http.Request, repo, id, p string) {
	ctx, cancel, err := s.attachmentTransferContext(r.Context(), attachments.V1Limits().TransferTimeout)
	if err != nil {
		attachmentHTTPError(w, 503, "repository_unavailable", false)
		return
	}
	defer cancel()
	stop, err := attachmentDeadlines(w, r, ctx, false)
	if err != nil {
		attachmentHTTPError(w, 422, "capability_unavailable", false)
		return
	}
	defer stop()
	result, err := s.config.RepositoryGateway.ReadAttachment(ctx, repo, id, p)
	if result.Content != nil {
		defer result.Content.Close()
	}
	if err != nil {
		writeRepositoryReadError(w, err)
		return
	}
	if result.Content == nil || result.Entry.SizeBytes == nil || *result.Entry.SizeBytes < 0 {
		attachmentHTTPError(w, 502, "attachment_storage_failed", false)
		return
	}
	h := w.Header()
	h.Set("ETag", strconv.Quote(result.Entry.Revision))
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.FormatInt(*result.Entry.SizeBytes, 10))
	h.Set("X-Content-Type-Options", "nosniff")
	// A fixed ASCII fallback and RFC 5987 UTF-8 name avoid header/control injection.
	h.Set("Content-Disposition", "attachment; filename=\"attachment\"; filename*=UTF-8''"+strings.ReplaceAll(url.QueryEscape(result.Entry.Name), "+", "%20"))
	w.WriteHeader(200)
	if _, err = io.CopyN(w, result.Content, *result.Entry.SizeBytes); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) writeAttachmentReceipt(w http.ResponseWriter, result attachments.Receipt, err error, deleted bool) {
	if err != nil {
		writeRepositoryMutationError(w, err)
		return
	}
	if deleted {
		w.Header().Set("X-Ticket-Attachments-Revision", result.RootRevision)
		w.WriteHeader(204)
		return
	}
	if result.Entry != nil {
		w.Header().Set("ETag", strconv.Quote(result.Entry.Revision))
	}
	status := 200
	if result.Created {
		status = 201
	}
	writeJSON(w, status, result)
}
func attachmentHTTPError(w http.ResponseWriter, status int, code string, mutation bool) {
	body := map[string]any{"code": code, "message": "Attachment request failed"}
	if mutation {
		body["mutation_applied"] = false
	}
	writeJSON(w, status, map[string]any{"error": body})
}
func writeAttachmentDomainError(w http.ResponseWriter, err error, mutation bool) {
	var domain *attachments.Error
	code, status := "invalid_request", 400
	if errors.As(err, &domain) {
		code = domain.Code
		switch code {
		case "limit_exceeded":
			status = 422
		case "precondition_required":
			status = 428
		}
	}
	attachmentHTTPError(w, status, code, mutation)
}
