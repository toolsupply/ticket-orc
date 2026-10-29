package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/attachments"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// The first read proves Stage has created its temporary file. Cancellation is
// the reader's explicit completion signal, as required of transport body IO.
type attachmentCanceledBody struct {
	ctx     context.Context
	entered chan struct{}
}

func (r attachmentCanceledBody) Read([]byte) (int, error) {
	close(r.entered)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestAttachmentUploadCommitLifecycleHandoff(t *testing.T) {
	if !attachments.MutationsAvailable() {
		t.Skip("safe mutations unavailable")
	}
	for _, phase := range []string{"body cancellation", "disconnect after acceptance", "shutdown after acceptance", "commit context rejected"} {
		t.Run(phase, func(t *testing.T) {
			m, g, dir, _ := attachmentGatewayFixture(t)
			server, err := daemon.NewServer(daemon.Config{EndpointKey: testDeleteKey(t), StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if err = server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			// Register server cleanup before anything can block.
			t.Cleanup(func() { _ = server.Close(); waitForTestDaemonStopped(t, server) })
			request, cancelRequest := context.WithCancel(context.Background())
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			done := make(chan struct{})
			var receipt attachments.Receipt
			var result error
			events := make(chan supervisor.RuntimeEvent, 1)
			m.runtimeEventSink = func(e supervisor.RuntimeEvent) { events <- e }
			var body io.Reader = strings.NewReader("data")
			if phase == "body cancellation" {
				body = attachmentCanceledBody{request, entered}
			}
			commitCalls := 0
			beginCommit := func() (context.Context, context.CancelFunc, error) {
				commitCalls++
				if phase == "body cancellation" {
					return nil, nil, errors.New("body cancellation reached commit")
				}
				// This is the production daemon primitive: it ignores request disconnect
				// while retaining daemon shutdown and the bounded mutation deadline.
				commit, cancel, err := server.MutationContext(request, attachments.V1Limits().CommitTimeout)
				close(entered)
				<-release
				if phase == "commit context rejected" {
					cancel()
					return nil, nil, errors.New("server rejected commit")
				}
				return commit, cancel, err
			}
			t.Cleanup(func() {
				cancelRequest()
				unblock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("upload did not exit during cleanup")
				}
			})
			go func() {
				defer close(done)
				receipt, result = g.WriteAttachment(request, joinTestRepositoryID, attachmentTestID, daemon.RepositoryAttachmentRequest{Actor: "ui-user", Path: "file", Condition: attachments.Condition{Create: true}}, body, 4, beginCommit)
			}()
			select {
			case <-entered:
			case <-done:
				t.Fatalf("upload exited before phase barrier: receipt=%+v err=%v", receipt, result)
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not reach phase barrier")
			}
			root := filepath.Join(dir, "first", "fixture", "attachments")
			if _, err = os.Stat(filepath.Join(root, ".ticket-orc", "upload.tmp")); err != nil {
				t.Fatalf("body not staged: %v", err)
			}
			switch phase {
			case "body cancellation", "disconnect after acceptance":
				cancelRequest()
			case "shutdown after acceptance":
				// Synchronous shutdown proves lifecycle cancellation before allowing any
				// quota/generation/publication work to continue past the barrier.
				if err = server.Close(); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not finish")
			}
			if phase == "body cancellation" && commitCalls != 0 {
				t.Fatal("incomplete body reached commit lifecycle")
			}
			if phase != "body cancellation" && commitCalls != 1 {
				t.Fatalf("commit lifecycle calls=%d", commitCalls)
			}
			if phase == "disconnect after acceptance" {
				if result != nil || !receipt.MutationApplied {
					t.Fatalf("accepted commit canceled on disconnect: %+v %v", receipt, result)
				}
				data, err := os.ReadFile(filepath.Join(root, "file"))
				if err != nil || string(data) != "data" {
					t.Fatalf("published=%q err=%v", data, err)
				}
				select {
				case event := <-events:
					if !event.Applied || event.Code != "attachments_changed" {
						t.Fatalf("invalidation=%+v", event)
					}
				default:
					t.Fatal("committed upload missing invalidation")
				}
			} else {
				assertAttachmentMutationError(t, result, "attachment_storage_failed", 502, false, true)
				if receipt.MutationApplied {
					t.Fatal("canceled upload reported publication")
				}
				if _, err = os.Stat(filepath.Join(root, "file")); !os.IsNotExist(err) {
					t.Fatalf("canceled upload published: %v", err)
				}
			}
			// Completion is the barrier for negative assertions and resource cleanup.
			select {
			case event := <-events:
				t.Fatalf("extra/false commit event: %+v", event)
			default:
			}
			if _, err = os.Stat(filepath.Join(root, ".ticket-orc", "upload.tmp")); !os.IsNotExist(err) {
				t.Fatalf("stage remains: %v", err)
			}
			// All four slots can be admitted after completion, proving cleanup released
			// the upload's slot even when Stage or the commit factory failed.
			var slots []*attachments.Upload
			defer func() {
				for _, u := range slots {
					u.Close()
				}
			}()
			for i := 0; i < attachments.V1Limits().MaxActiveTransfers; i++ {
				u, err := m.attachmentStore.BeginUpload(context.Background())
				if err != nil {
					t.Fatalf("admission leaked: %v", err)
				}
				slots = append(slots, u)
			}
		})
	}
}
