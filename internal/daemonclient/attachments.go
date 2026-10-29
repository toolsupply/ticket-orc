package daemonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
)

// AttachmentDownload owns the response body. Close it even after a partial read;
// closing or cancelling the request context interrupts a blocked network read.
// ETag is quoted for HTTP forwarding; Entry.Revision is the unquoted token.
type AttachmentDownload struct {
	Content io.ReadCloser
	Entry   attachments.Entry
	ETag    string
	Length  int64
}

func attachmentPath(repo, id, suffix string, q url.Values) string {
	p := "/v1/repositories/" + url.PathEscape(repo) + "/tickets/" + url.PathEscape(id) + "/attachments" + suffix
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	return p
}

// AttachmentCapabilities returns nil for older daemons without the extension.
// Every attachment call checks current capabilities before sending its operation.
func (c *Client) AttachmentCapabilities(ctx context.Context) (*daemon.AttachmentCapabilities, error) {
	s, err := c.Status(ctx)
	if err != nil {
		return nil, err
	}
	return s.Capabilities.Repositories.TicketAttachments, nil
}

func (c *Client) requireAttachment(ctx context.Context, op string) error {
	caps, err := c.AttachmentCapabilities(ctx)
	if err != nil {
		return err
	}
	supported := false
	if caps != nil && caps.RevisionScope == "ticket" {
		switch op {
		case "list":
			supported = caps.List
		case "download":
			supported = caps.Download
		case "upload":
			supported = caps.Upload
		case "mkdir":
			supported = caps.Mkdir
		case "move":
			supported = caps.Move
		case "delete":
			supported = caps.Delete
		}
	}
	if !supported {
		return &Error{Kind: ErrorApplication, Code: "capability_unavailable", Message: "attachment operation is unavailable", AppliedKnown: true}
	}
	return nil
}

// attachmentResponse deliberately has no retry or redirect path. In particular
// a PUT body cannot be replayed by net/http: GetBody is never populated.
func (c *Client) attachmentResponse(ctx context.Context, method, path string, body io.Reader, length int64, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint.CapabilityURL()+path, body)
	if err != nil {
		return nil, &Error{Kind: ErrorProtocol, Message: "build attachment request", Cause: err}
	}
	req.ContentLength = length
	req.Header = headers
	client := *c.http
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := client.Do(req)
	if err != nil {
		return nil, &Error{Kind: ErrorTransport, Message: "attachment transport failed; reconcile before another mutation", Cause: err}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		defer res.Body.Close()
		data, err := readAttachmentJSON(res.Body, maxResponseBytes)
		if err != nil {
			return nil, err
		}
		return nil, c.applicationError(res.StatusCode, data)
	}
	return res, nil
}

func readAttachmentJSON(r io.Reader, maxBytes int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(maxBytes)+1))
	if err != nil {
		return nil, &Error{Kind: ErrorTransport, Message: "read attachment response", Cause: err}
	}
	if len(data) > maxBytes {
		return nil, &Error{Kind: ErrorProtocol, Message: "attachment metadata response is too large"}
	}
	return data, nil
}

// attachmentListingResponseBytes bounds the complete compact Listing schema,
// including Encoder's trailing newline. Legal UTF-8 name/path bytes can expand
// sixfold under Go's JSON HTML escaping (e.g. & becomes \u0026). UUID/full-ID
// fields are restricted ASCII (36/14 bytes); strong revisions are 46 bytes and
// cursors are bounded base64url ASCII. Use the longer directory kind together
// with the largest int64 size for a conservative per-entry schema overhead.
// This budget applies only to successful listings, never receipts or errors.
func attachmentListingResponseBytes() int {
	limits := attachments.V1Limits()
	empty := ""
	size := int64(1<<63 - 1)
	entry, _ := json.Marshal(attachments.Entry{Kind: "directory", SizeBytes: &size})
	listing, _ := json.Marshal(attachments.Listing{Entries: []attachments.Entry{}, NextCursor: &empty})
	return len(listing) + 36 + 14 + 46 + 6*limits.MaxPathBytes + limits.MaxCursorBytes +
		limits.MaxPageEntries*(len(entry)+46+6*(limits.MaxNameBytes+limits.MaxPathBytes)+1) + 1
}

// ListRepositoryAttachments returns one bounded immediate-child page. After an
// event gap or unknown mutation outcome, relist and read bytes as needed; matching
// names alone do not establish which writer committed.
func (c *Client) ListRepositoryAttachments(ctx context.Context, repo, id string, query daemon.RepositoryAttachmentQuery) (attachments.Listing, error) {
	var result attachments.Listing
	if err := c.requireAttachment(ctx, "list"); err != nil {
		return result, err
	}
	q := url.Values{"path": {query.Path}}
	if query.Limit != 0 {
		q.Set("limit", strconv.Itoa(query.Limit))
	}
	if query.Cursor != "" {
		q.Set("cursor", query.Cursor)
	}
	res, err := c.attachmentResponse(ctx, "GET", attachmentPath(repo, id, "", q), nil, 0, http.Header{})
	if err != nil {
		return result, err
	}
	defer res.Body.Close()
	data, err := readAttachmentJSON(res.Body, attachmentListingResponseBytes())
	if err != nil {
		return result, err
	}
	if res.StatusCode != http.StatusOK || decodeOne(data, &result) != nil || result.RepositoryID != repo || result.TicketID != id || result.Directory != query.Path {
		return attachments.Listing{}, attachmentProtocolError()
	}
	if (attachments.Condition{Revision: result.Revision}).Validate(false) != nil || result.Entries == nil || result.NextCursor != nil && len(*result.NextCursor) > attachments.V1Limits().MaxCursorBytes {
		return attachments.Listing{}, attachmentProtocolError()
	}
	for _, entry := range result.Entries {
		parent := ""
		if i := strings.LastIndex(entry.Path, "/"); i >= 0 {
			parent = entry.Path[:i]
		}
		if !validAttachmentEntry(entry) || parent != query.Path {
			return attachments.Listing{}, attachmentProtocolError()
		}
	}
	return result, nil
}

func validAttachmentEntry(entry attachments.Entry) bool {
	if attachments.ValidatePath(entry.Path, false) != nil || attachments.ValidatePath(entry.Name, false) != nil || strings.Contains(entry.Name, "/") || (attachments.Condition{Revision: entry.Revision}).Validate(false) != nil {
		return false
	}
	if entry.Path != entry.Name && !strings.HasSuffix(entry.Path, "/"+entry.Name) {
		return false
	}
	switch entry.Kind {
	case "file":
		return entry.SizeBytes != nil && *entry.SizeBytes >= 0
	case "directory":
		return entry.SizeBytes == nil
	default:
		return false
	}
}

func (c *Client) ReadRepositoryAttachment(ctx context.Context, repo, id, path string) (AttachmentDownload, error) {
	if err := c.requireAttachment(ctx, "download"); err != nil {
		return AttachmentDownload{}, err
	}
	res, err := c.attachmentResponse(ctx, "GET", attachmentPath(repo, id, "/content", url.Values{"path": {path}}), nil, 0, http.Header{})
	if err != nil {
		return AttachmentDownload{}, err
	}
	fail := func() (AttachmentDownload, error) {
		res.Body.Close()
		return AttachmentDownload{}, attachmentProtocolError()
	}
	mt, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode != http.StatusOK || err != nil || mt != "application/octet-stream" || res.ContentLength < 0 || res.Header.Get("Content-Encoding") != "" {
		return fail()
	}
	etag := res.Header.Get("ETag")
	if len(etag) < 2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return fail()
	}
	revision := etag[1 : len(etag)-1]
	if (attachments.Condition{Revision: revision}).Validate(false) != nil {
		return fail()
	}
	disposition, params, err := mime.ParseMediaType(res.Header.Get("Content-Disposition"))
	name := params["filename"]
	if err != nil || disposition != "attachment" || name == "" || strings.Contains(name, "/") || attachments.ValidatePath(name, false) != nil {
		return fail()
	}
	size := res.ContentLength
	return AttachmentDownload{Content: res.Body, ETag: etag, Length: size, Entry: attachments.Entry{Name: name, Path: path, Kind: "file", SizeBytes: &size, Revision: revision}}, nil
}

func attachmentProtocolError() error {
	return &Error{Kind: ErrorProtocol, Message: "invalid attachment response; reconcile mutations before retrying"}
}
func conditionHeaders(c attachments.Condition) http.Header {
	h := http.Header{}
	if c.Create {
		h.Set("If-None-Match", "*")
	}
	if c.Revision != "" {
		h.Set("If-Match", `"`+c.Revision+`"`)
	}
	return h
}

// uploadBody hides concrete reader types (and hence GetBody/replay). If the
// supplied reader is also a Closer, ownership transfers after capability checks:
// Close is called exactly once, including cancellation and early HTTP failures.
// Close must unblock Read. A plain Reader must finish or observe cancellation
// itself; no goroutine is created to hide an uninterruptible user Read.
type uploadBody struct {
	io.Reader
	once sync.Once
}

func (b *uploadBody) Close() error {
	var err error
	b.once.Do(func() {
		if c, ok := b.Reader.(io.Closer); ok {
			err = c.Close()
		}
	})
	return err
}

// WriteRepositoryAttachment streams bytes without buffering or retry. length=-1
// selects chunked transport; a nonnegative value is the exact declared length.
func (c *Client) WriteRepositoryAttachment(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest, body io.Reader, length int64) (attachments.Receipt, error) {
	if err := c.requireAttachment(ctx, "upload"); err != nil {
		return attachments.Receipt{}, err
	}
	b := &uploadBody{Reader: body}
	defer b.Close()
	// net/http may wait for its writer before closing the request body. Close
	// the source explicitly on cancellation so an io.Pipe Read cannot trap it.
	stop := context.AfterFunc(ctx, func() { _ = b.Close() })
	defer stop()
	if body == nil || length < -1 {
		return attachments.Receipt{}, &Error{Kind: ErrorProtocol, Message: "invalid attachment body or length", AppliedKnown: true}
	}
	h := conditionHeaders(req.Condition)
	h.Set("X-Ticket-Actor", req.Actor)
	h.Set("Content-Type", "application/octet-stream")
	return c.attachmentReceipt(ctx, "PUT", attachmentPath(repo, id, "/content", url.Values{"path": {req.Path}}), b, length, h, repo, id, req.Actor, "upload")
}

func (c *Client) CreateRepositoryAttachmentDirectory(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest) (attachments.Receipt, error) {
	if err := c.requireAttachment(ctx, "mkdir"); err != nil {
		return attachments.Receipt{}, err
	}
	data, _ := json.Marshal(map[string]string{"actor": req.Actor, "path": req.Path})
	h := conditionHeaders(req.Condition)
	h.Set("Content-Type", "application/json")
	return c.attachmentReceipt(ctx, "POST", attachmentPath(repo, id, "/folders", nil), bytes.NewBuffer(data), int64(len(data)), h, repo, id, req.Actor, "mkdir")
}
func (c *Client) MoveRepositoryAttachment(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentMoveRequest) (attachments.Receipt, error) {
	if err := c.requireAttachment(ctx, "move"); err != nil {
		return attachments.Receipt{}, err
	}
	data, _ := json.Marshal(map[string]string{"actor": req.Actor, "from": req.From, "to": req.To, "revision": req.Revision})
	h := conditionHeaders(req.Condition)
	h.Set("Content-Type", "application/json")
	return c.attachmentReceipt(ctx, "POST", attachmentPath(repo, id, "/move", nil), bytes.NewBuffer(data), int64(len(data)), h, repo, id, req.Actor, "move")
}
func (c *Client) DeleteRepositoryAttachment(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest) (attachments.Receipt, error) {
	if err := c.requireAttachment(ctx, "delete"); err != nil {
		return attachments.Receipt{}, err
	}
	h := conditionHeaders(req.Condition)
	h.Set("X-Ticket-Actor", req.Actor)
	return c.attachmentReceipt(ctx, "DELETE", attachmentPath(repo, id, "", url.Values{"path": {req.Path}}), nil, 0, h, repo, id, req.Actor, "delete")
}
func (c *Client) attachmentReceipt(ctx context.Context, method, path string, body io.Reader, length int64, h http.Header, repo, id, actor, operation string) (attachments.Receipt, error) {
	res, err := c.attachmentResponse(ctx, method, path, body, length, h)
	if err != nil {
		return attachments.Receipt{}, err
	}
	defer res.Body.Close()
	if operation == "delete" {
		revision := res.Header.Get("X-Ticket-Attachments-Revision")
		if res.StatusCode != 204 || (attachments.Condition{Revision: revision}).Validate(false) != nil {
			return attachments.Receipt{}, attachmentProtocolError()
		}
		return attachments.Receipt{RepositoryID: repo, TicketID: id, Actor: actor, Operation: operation, MutationApplied: true, RootRevision: revision}, nil
	}
	data, err := readAttachmentJSON(res.Body, maxResponseBytes)
	if err != nil {
		return attachments.Receipt{}, err
	}
	var receipt attachments.Receipt
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated || decodeOne(data, &receipt) != nil || receipt.RepositoryID != repo || receipt.TicketID != id || receipt.Actor != actor || receipt.Operation != operation || !receipt.MutationApplied || receipt.Entry == nil || !validAttachmentEntry(*receipt.Entry) || (attachments.Condition{Revision: receipt.RootRevision}).Validate(false) != nil || res.Header.Get("ETag") != `"`+receipt.Entry.Revision+`"` {
		return attachments.Receipt{}, attachmentProtocolError()
	}
	receipt.Created = res.StatusCode == 201
	return receipt, nil
}
