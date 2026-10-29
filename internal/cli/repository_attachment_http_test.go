package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func TestAttachmentHTTPRealGateway(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	_, g, _, _ := attachmentGatewayFixture(t)
	key := testDeleteKey(t)
	s, err := daemon.NewServer(daemon.Config{EndpointKey: key, StateDir: t.TempDir(), RepositoryGateway: g})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); waitForTestDaemonStopped(t, s) })
	base := s.Endpoint().URL + "/" + key + "/v1/repositories/" + joinTestRepositoryID + "/tickets/" + attachmentTestID + "/attachments"
	call := func(method, path string, body io.Reader, headers map[string]string, status int) ([]byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest(method, path, body)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != status {
			t.Fatalf("%s %s: status=%d body=%s", method, path, response.StatusCode, data)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store")
		}
		return data, response.Header
	}
	call("GET", strings.Replace(base, "/"+key+"/", "/invalid/", 1), nil, nil, 404)
	list := func(path string) attachments.Listing {
		t.Helper()
		data, _ := call("GET", path, nil, nil, 200)
		var l attachments.Listing
		if err := json.Unmarshal(data, &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	if len(list(base).Entries) != 0 {
		t.Fatal("initial listing")
	}
	headers := map[string]string{"Content-Type": "application/octet-stream", "X-Ticket-Actor": "ui-user", "If-None-Match": "*"}
	payload := bytes.Repeat([]byte{0, 1, 255, 10}, (3<<20)/4)
	data, h := call("PUT", base+"/content?path="+url.QueryEscape("café.bin"), bytes.NewReader(payload), headers, 201)
	var receipt attachments.Receipt
	json.Unmarshal(data, &receipt)
	if !receipt.MutationApplied || receipt.Actor != "ui-user" || receipt.Entry == nil || h.Get("ETag") != `"`+receipt.Entry.Revision+`"` {
		t.Fatalf("receipt=%s headers=%v", data, h)
	}
	data, h = call("GET", base+"/content?path="+url.QueryEscape("café.bin"), nil, nil, 200)
	if !bytes.Equal(data, payload) || h.Get("Content-Length") != "3145728" || h.Get("Content-Type") != "application/octet-stream" || h.Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(h.Get("Content-Disposition"), "filename*=UTF-8''caf%C3%A9.bin") {
		t.Fatalf("download size=%d headers=%v", len(data), h)
	}
	call("GET", base+"/content?path="+url.QueryEscape("café.bin"), nil, map[string]string{"Range": "bytes=0-1"}, 400)
	call("PUT", base+"/content?path="+url.QueryEscape("café.bin"), strings.NewReader("collision"), headers, 412)
	headers = map[string]string{"Content-Type": "application/octet-stream", "X-Ticket-Actor": "ui-user", "If-Match": h.Get("ETag")}
	call("PUT", base+"/content?path="+url.QueryEscape("café.bin"), strings.NewReader("replacement"), headers, 200)
	call("PUT", base+"/content?path="+url.QueryEscape("café.bin"), strings.NewReader("stale"), headers, 412)
	jsonHeaders := map[string]string{"Content-Type": "application/json", "If-None-Match": "*"}
	call("POST", base+"/folders", strings.NewReader(`{"actor":"ui-user","path":"folder"}`), jsonHeaders, 201)
	call("POST", base+"/folders", strings.NewReader(`{"actor":"ui-user","path":"folder"}`), jsonHeaders, 412)
	for _, body := range []string{`{"actor":"ui-user","path":"x","path":"y"}`, `{"actor":"ui-user","Path":"x"}`, `{"actor":"ui-user","path":null}`, `{"actor":"ui-user","path":"x"} {}`, `{"actor":"ui-user","path":"x","other":"y"}`} {
		call("POST", base+"/folders", strings.NewReader(body), jsonHeaders, 400)
	}
	call("POST", base+"/folders", strings.NewReader(strings.Repeat(" ", attachments.V1Limits().MaxJSONBodyBytes+1)), jsonHeaders, 413)
	call("POST", base+"/folders", strings.NewReader(`{"actor":"ui-user","path":"missing/child"}`), jsonHeaders, 404)
	var source attachments.Entry
	for _, e := range list(base).Entries {
		if e.Kind == "file" {
			source = e
		}
	}
	body, _ := json.Marshal(map[string]string{"actor": "ui-user", "from": source.Path, "to": "folder/moved.bin", "revision": source.Revision})
	call("POST", base+"/move", bytes.NewReader(body), jsonHeaders, 200)
	l := list(base)
	var directory attachments.Entry
	for _, e := range l.Entries {
		if e.Kind == "directory" {
			directory = e
		}
	}
	deleteHeaders := map[string]string{"X-Ticket-Actor": "ui-user", "If-Match": `"` + directory.Revision + `"`}
	call("DELETE", base+"?path=folder", nil, deleteHeaders, 409)
	file := list(base + "?path=folder").Entries[0]
	deleteHeaders["If-Match"] = `"` + file.Revision + `"`
	_, h = call("DELETE", base+"?path=folder/moved.bin", nil, deleteHeaders, 204)
	if h.Get("X-Ticket-Attachments-Revision") == "" {
		t.Fatal("missing delete revision")
	}
	directory = list(base).Entries[0]
	deleteHeaders["If-Match"] = `"` + directory.Revision + `"`
	call("DELETE", base+"?path=folder", nil, deleteHeaders, 204)
	if len(list(base).Entries) != 0 {
		t.Fatal("delete failed")
	}
	other := strings.Replace(base, joinTestRepositoryID, attachmentOtherRepo, 1)
	if len(list(other).Entries) != 0 {
		t.Fatal("cross repository state")
	}
	call("PUT", other+"/content?path=other", strings.NewReader("other bytes"), map[string]string{"Content-Type": "application/octet-stream", "X-Ticket-Actor": "ui-user", "If-None-Match": "*"}, 201)
	if len(list(base).Entries) != 0 || len(list(other).Entries) != 1 {
		t.Fatal("repository isolation")
	}
}

// The reader barrier proves the real gateway has resolved and locked storage,
// created staging, and reached network input before shutdown/cancellation.
type attachmentHTTPReadProbe struct {
	io.Reader
	entered chan struct{}
	once    sync.Once
}

func (r *attachmentHTTPReadProbe) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.Reader.Read(p)
}

func TestAttachmentClientTransferLifetime(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	for _, phase := range []string{"shutdown during body", "disconnect during body", "disconnect after acceptance"} {
		t.Run(phase, func(t *testing.T) {
			_, g, dir, _ := attachmentGatewayFixture(t)
			original := g.WriteAttachment
			entered := make(chan struct{})
			finished := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			g.WriteAttachment = func(ctx context.Context, repo, id string, req daemon.RepositoryAttachmentRequest, body io.Reader, length int64, begin attachments.CommitContext) (attachments.Receipt, error) {
				defer close(finished)
				if phase == "disconnect after acceptance" {
					return original(ctx, repo, id, req, body, length, func() (context.Context, context.CancelFunc, error) {
						commit, cancel, err := begin()
						close(entered)
						<-ctx.Done()
						<-release
						return commit, cancel, err
					})
				}
				return original(ctx, repo, id, req, &attachmentHTTPReadProbe{Reader: body, entered: entered}, length, begin)
			}
			s, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir(), RepositoryGateway: g})
			if err != nil {
				t.Fatal(err)
			}
			serverCtx, serverCancel := context.WithCancel(context.Background())
			if err = s.Start(serverCtx); err != nil {
				serverCancel()
				t.Fatal(err)
			}
			requestCtx, requestCancel := context.WithCancel(context.Background())
			reader, writer := io.Pipe()
			responseDone := make(chan struct{})
			responseError := make(chan error, 1)
			t.Cleanup(func() {
				requestCancel()
				writer.Close()
				reader.Close()
				unblock()
				_ = s.Close()
				serverCancel()
				waitForTestDaemonStopped(t, s)
				select {
				case <-responseDone:
				case <-time.After(5 * time.Second):
					t.Error("client stuck")
				}
			})
			var body io.Reader = reader
			if phase == "disconnect after acceptance" {
				body = strings.NewReader("complete bytes")
			}
			client, err := daemonclient.NewWithEndpoint("", s.Endpoint().CapabilityURL(), "")
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				defer close(responseDone)
				_, clientErr := client.WriteRepositoryAttachment(requestCtx, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "phase", Condition: attachments.Condition{Create: true}}, body, -1)
				responseError <- clientErr
			}()
			select {
			case <-entered:
			case <-responseDone:
				t.Fatalf("upload exited before transfer barrier: %v", <-responseError)
			case <-time.After(10 * time.Second):
				t.Fatal("gateway did not reach transfer barrier")
			}
			if phase == "shutdown during body" {
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				requestCancel()
			}
			unblock()
			select {
			case <-finished:
			case <-time.After(10 * time.Second):
				t.Fatal("gateway did not finish")
			}
			if phase != "shutdown during body" {
				select {
				case err := <-responseError:
					var clientErr *daemonclient.Error
					if !errors.As(err, &clientErr) || clientErr.Kind != daemonclient.ErrorTransport || clientErr.AppliedKnown {
						t.Fatalf("cancelled client must retain uncertainty: %+v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("cancelled client did not return")
				}
			}
			root := filepath.Join(dir, "first", "fixture", "attachments")
			data, err := os.ReadFile(filepath.Join(root, "phase"))
			if phase == "disconnect after acceptance" {
				if err != nil || string(data) != "complete bytes" {
					t.Fatalf("accepted commit lost: %q %v", data, err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("incomplete body published: %q %v", data, err)
			}
			if _, err = os.Stat(filepath.Join(root, ".ticket-orc", "upload.tmp")); !os.IsNotExist(err) {
				t.Fatalf("stage remains: %v", err)
			}
		})
	}
}

// An endless source exercises the actual byte limit on an unknown-length body
// without allocating a file-sized buffer in the test.
type attachmentZeroReader struct{}

func (attachmentZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestAttachmentHTTPInputBounds(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	_, g, dir, _ := attachmentGatewayFixture(t)
	s, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir(), RepositoryGateway: g})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); waitForTestDaemonStopped(t, s) })
	endpoint := s.Endpoint()
	route := "/" + endpoint.EndpointKey + "/v1/repositories/" + joinTestRepositoryID + "/tickets/" + attachmentTestID + "/attachments/content?path=bounded"
	for _, tc := range []struct {
		name   string
		length int64
		body   io.Reader
		status int
	}{
		{"declared oversize", attachments.V1Limits().MaxFileBytes + 1, io.LimitReader(attachmentZeroReader{}, attachments.V1Limits().MaxFileBytes+1), 413},
		{"unknown oversize", -1, io.LimitReader(attachmentZeroReader{}, attachments.V1Limits().MaxFileBytes+1), 413},
		{"empty", 0, strings.NewReader(""), 201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("PUT", endpoint.URL+route, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			req.ContentLength = tc.length
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("X-Ticket-Actor", "ui-user")
			req.Header.Set("If-None-Match", "*")
			// Expect avoids the HTTP client's local length check for a rejected body.
			if tc.length > 0 {
				req.Header.Set("Expect", "100-continue")
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != tc.status {
				t.Fatalf("%d %s", response.StatusCode, data)
			}
			if tc.status != 201 {
				if _, err = os.Stat(filepath.Join(dir, "first", "fixture", "attachments", "bounded")); !os.IsNotExist(err) {
					t.Fatalf("oversize published: %v", err)
				}
			}
		})
	}
	// A real truncated Content-Length, followed by TCP half-close, must not publish.
	address := strings.TrimPrefix(endpoint.URL, "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	_, err = fmt.Fprintf(conn, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Length: 10\r\nContent-Type: application/octet-stream\r\nX-Ticket-Actor: ui-user\r\nIf-None-Match: *\r\n\r\nshort", strings.Replace(route, "path=bounded", "path=truncated", 1), address)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode < 400 {
		t.Fatalf("truncated body accepted: %d", response.StatusCode)
	}
	if _, err = os.Stat(filepath.Join(dir, "first", "fixture", "attachments", "truncated")); !os.IsNotExist(err) {
		t.Fatalf("truncated body published: %v", err)
	}
}
