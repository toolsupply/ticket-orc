package daemon

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/attachments"
)

type attachmentStreamProbe struct {
	read   func([]byte) (int, error)
	closed chan struct{}
}

func (r *attachmentStreamProbe) Read(p []byte) (int, error) { return r.read(p) }
func (r *attachmentStreamProbe) Close() error               { close(r.closed); return nil }

func TestAttachmentDownloadFailureClosesStream(t *testing.T) {
	for _, phase := range []string{"partial failure", "shutdown"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			closed := make(chan struct{})
			g := RepositoryGateway{ReadAttachment: func(ctx context.Context, repo, id, path string) (RepositoryAttachmentContent, error) {
				size := int64(100)
				calls := 0
				probe := &attachmentStreamProbe{closed: closed, read: func(p []byte) (int, error) {
					calls++
					if calls == 1 {
						return copy(p, "binary prefix"), nil
					}
					close(entered)
					if phase == "shutdown" {
						<-ctx.Done()
						return 0, ctx.Err()
					}
					return 0, errors.New("private/path must not be exposed")
				}}
				return RepositoryAttachmentContent{Entry: attachments.Entry{Name: "file", SizeBytes: &size, Revision: "opaque"}, Content: probe}, nil
			}}
			s, err := NewServer(Config{EndpointKey: testEndpointKey, StateDir: t.TempDir(), RepositoryGateway: g})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if err = s.Start(ctx); err != nil {
				cancel()
				t.Fatal(err)
			}
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				_ = s.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("client stuck")
				}
			})
			go func() {
				defer close(done)
				response, err := http.Get(s.Endpoint().URL + "/" + testEndpointKey + "/v1/repositories/" + testRepositoryID + "/tickets/20261025-00001/attachments/content?path=file")
				// net/http may abort before flushing its small buffer. Both connection abort
				// and a short declared-length body are honest transport failures.
				if err != nil {
					return
				}
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				if err == nil {
					t.Error("partial download reported complete")
				}
				if strings.Contains(string(data), "error") || strings.Contains(string(data), "private") {
					t.Errorf("error appended to binary: %s", data)
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("download did not reach barrier")
			}
			if phase == "shutdown" {
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("reader leaked")
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("client did not finish")
			}
		})
	}
}
