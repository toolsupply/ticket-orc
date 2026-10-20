package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

// Exercise both event loops and the real follower, with the viewer baselined
// before appending logs. Output milestones prove consumption before cancellation.
func TestUnifiedWatchConsoleStandaloneParityAndCancellation(t *testing.T) {
	for _, args := range [][]string{nil, {"all"}, {"--verbose"}, {"all", "--verbose"}, {"--verbose", "all"}} {
		for _, console := range []bool{false, true} {
			t.Run(fmt.Sprintf("console=%t/%s", console, strings.Join(args, " ")), func(t *testing.T) {
				status := daemon.Status{
					Repositories: []daemon.RepositoryStatus{{ID: joinTestRepositoryID, Name: "ticket-orc-repo"}},
					Workers:      []daemon.WorkerStatus{{Name: "stable-worker", Role: "coder", Harness: "codex", TicketActor: "orc-coder", RepositoryID: joinTestRepositoryID}},
				}
				ready := make(chan struct{})
				var shutdownCalls atomic.Int32
				server, root := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/status":
						_ = json.NewEncoder(w).Encode(status)
					case "/v1/events":
						select {
						case <-ready:
						case <-r.Context().Done():
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "id: 1\ndata: {\"seq\":1,\"type\":\"ticket.claim\",\"worker\":\"stable-worker\",\"ticket\":\"20261017-99478\"}\n\n")
						w.(http.Flusher).Flush()
						<-r.Context().Done()
					case "/v1/shutdown":
						shutdownCalls.Add(1)
					default:
						http.NotFound(w, r)
					}
				}))
				t.Cleanup(server.Close)
				output := &watchParityOutput{
					consoleWatchStartWriter: consoleWatchStartWriter{started: ready, matched: make(chan struct{}), match: "[ticket-orc-repo:orc-coder] worker line", prompted: make(chan struct{}, 4)},
					eventRendered:           make(chan struct{}),
				}
				ctx, cancel := context.WithCancel(context.Background())
				reader, writer := io.Pipe()
				interrupts := make(chan os.Signal, 1)
				var errorsOut bytes.Buffer
				done := make(chan error, 1)
				exited := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					_ = writer.Close()
					_ = reader.Close()
					select {
					case <-exited:
					case <-time.After(2 * time.Second):
						t.Error("viewer did not exit during cleanup")
					}
				})
				options, err := parseWatchOptions(args)
				if err != nil {
					t.Fatal(err)
				}
				if console {
					go func() {
						done <- runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{localDir: root}, reader, output, &errorsOut, interrupts, "", false, nil)
						close(exited)
					}()
					waitConsoleMilestone(t, output.prompted, "initial console prompt")
					if _, err := io.WriteString(writer, "watch "+strings.Join(args, " ")+"\n"); err != nil {
						t.Fatal(err)
					}
				} else {
					client, err := daemonclient.New(root)
					if err != nil {
						t.Fatal(err)
					}
					watch, err := newWorkerLogWatch(root, status, options, output)
					if err != nil {
						t.Fatal(err)
					}
					go func() {
						code := runUnifiedStandaloneWatch(ctx, client, watch, status, output, &errorsOut)
						if code != 0 {
							done <- fmt.Errorf("viewer exit code %d", code)
						} else {
							done <- nil
						}
						close(exited)
					}()
					close(ready) // newWorkerLogWatch has synchronously established EOF baselines.
				}
				waitWorkerWatchSignal(t, ready, "watch baseline")
				log := openWatchTestLog(t, root, "stable-worker", time.Now())
				if _, err := io.WriteString(log, watchDetailEvents+`{"type":"item.completed","item":{"type":"agent_message","text":"worker line"}}`+"\n"); err != nil {
					t.Fatal(err)
				}
				if err := log.Close(); err != nil {
					t.Fatal(err)
				}
				waitWorkerWatchSignal(t, output.eventRendered, "Orc prose event")
				waitWorkerWatchSignal(t, output.matched, "worker output")
				if console {
					interrupts <- os.Interrupt
					waitConsoleMilestone(t, output.prompted, "prompt after viewer Ctrl-C")
					if _, err := io.WriteString(writer, "quit\n"); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("viewer failed to exit")
				}
				if shutdownCalls.Load() != 0 {
					t.Fatal("viewer cancellation stopped daemon")
				}
				got := output.String()
				if console {
					got = got[strings.Index(got, "Watching activity."):]
				}
				if strings.Contains(got, consoleWatchHeader()) || strings.Contains(got, "Time  ") {
					t.Fatalf("unified mode emitted table: %q", got)
				}
				for _, want := range []string{
					"[ticket-orc-repo:orc] coder worker stable-worker (actor orc-coder) claimed 20261017-99478",
					"[ticket-orc-repo:orc-coder] worker line",
					"[ticket-orc-repo:orc-coder] hello",
					"[ticket-orc-repo:orc-coder] world",
					"[ticket-orc-repo:orc-coder] [codex error] diagnostic",
				} {
					if !strings.Contains(got, want) {
						t.Errorf("missing %q: %q", want, got)
					}
				}
				for _, line := range strings.Split(got, "\n") {
					if strings.Contains(line, "[ticket-orc-repo:") {
						if _, err := time.Parse("15:04:05", line[:8]); err != nil {
							t.Errorf("missing observation time: %q", line)
						}
					}
				}
				for _, detail := range []string{"noisy-command", "changed.go"} {
					if strings.Contains(got, detail) != options.verbose {
						t.Errorf("verbose=%t detail=%q: %q", options.verbose, detail, got)
					}
				}
				if errorsOut.Len() != 0 {
					t.Fatal(errorsOut.String())
				}
			})
		}
	}
}

type watchParityOutput struct {
	consoleWatchStartWriter
	eventOnce     sync.Once
	eventRendered chan struct{}
}

func (out *watchParityOutput) Write(data []byte) (int, error) {
	n, err := out.consoleWatchStartWriter.Write(data)
	if strings.Contains(string(data), "claimed 20261017-99478") {
		out.eventOnce.Do(func() { close(out.eventRendered) })
	}
	return n, err
}

func (out *watchParityOutput) WriteString(text string) (int, error) { return out.Write([]byte(text)) }

func TestRemoteConsoleRequiresExplicitOrcWatch(t *testing.T) {
	server, root := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(daemon.Status{})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, ": connected\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	endpoint, err := daemon.ReadEndpoint(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	interrupts := make(chan os.Signal, 1)
	output := &consoleWatchStartWriter{started: make(chan struct{}), prompted: make(chan struct{}, 8)}
	var errorsOut bytes.Buffer
	done := make(chan error, 1)
	exited := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("remote console failed to exit")
		}
	})
	go func() {
		done <- runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{endpoint: endpoint.CapabilityURL()}, reader, output, &errorsOut, interrupts, "", false, nil)
		close(exited)
	}()
	waitConsoleMilestone(t, output.prompted, "remote console prompt")
	for _, command := range []string{"watch", "watch --verbose", "watch all"} {
		if _, err := io.WriteString(writer, command+"\n"); err != nil {
			t.Fatal(err)
		}
		waitConsoleMilestone(t, output.prompted, "unsupported worker watch returned to prompt")
	}
	if _, err := io.WriteString(writer, "watch orc\n"); err != nil {
		t.Fatal(err)
	}
	waitWorkerWatchSignal(t, output.started, "explicit remote Orc watch")
	interrupts <- os.Interrupt
	waitConsoleMilestone(t, output.prompted, "prompt after remote watch")
	if _, err := io.WriteString(writer, "quit\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote console failed to exit")
	}
	if strings.Count(errorsOut.String(), remoteWorkerWatchError) != 3 {
		t.Fatalf("missing default/all guidance: %q", errorsOut.String())
	}
	if !strings.Contains(output.String(), consoleWatchHeader()) {
		t.Fatalf("explicit Orc lost legacy header: %q", output.String())
	}
}

func TestStandaloneWatchRefreshesRepositoryForWorkerEvent(t *testing.T) {
	status := daemon.Status{Workers: []daemon.WorkerStatus{{Name: "stable-worker", Role: "coder", Harness: "codex", TicketActor: "alice", RepositoryID: joinTestRepositoryID}}}
	var statusCalls atomic.Int32
	server, root := newConsoleTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			statusCalls.Add(1)
			_ = json.NewEncoder(w).Encode(daemon.Status{Workers: status.Workers, Repositories: []daemon.RepositoryStatus{{ID: joinTestRepositoryID, Name: "Refreshed Repository"}}})
		case "/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "id: 1\ndata: {\"seq\":1,\"type\":\"ticket.claim\",\"worker\":\"stable-worker\",\"ticket\":\"20261017-99478\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := daemonclient.New(root)
	if err != nil {
		t.Fatal(err)
	}
	output := &consoleWatchStartWriter{matched: make(chan struct{}), match: "[Refreshed Repository:orc] coder worker stable-worker (actor alice) claimed 20261017-99478"}
	watch, err := newWorkerLogWatch(root, status, watchOptions{selector: "all"}, output)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	exited := make(chan struct{})
	var errorsOut bytes.Buffer
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("standalone watch failed to exit")
		}
	})
	go func() {
		done <- runUnifiedStandaloneWatch(ctx, client, watch, status, output, &errorsOut)
		close(exited)
	}()
	waitWorkerWatchSignal(t, output.matched, "repository status refresh rendered")
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code=%d errors=%q", code, errorsOut.String())
		}
	case <-time.After(time.Second):
		t.Fatal("standalone watch failed to exit")
	}
	if statusCalls.Load() != 1 {
		t.Fatalf("repository refresh calls=%d", statusCalls.Load())
	}
}
