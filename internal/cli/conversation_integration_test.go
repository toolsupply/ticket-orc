package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

// This process substitutes for the model/provider, while exercising the real
// Codex JSONL adapter, managed process containment, Ticket CLI and Orc CLI/API.
func TestConversationAgentProcess(t *testing.T) {
	if os.Getenv("ORC_CONVERSATION_TEST_CHILD") != "1" {
		return
	}
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
	cwd, err := os.Getwd()
	if err != nil {
		fail(err)
	}
	if cwd != os.Getenv("ORC_CONVERSATION_TEST_CWD") || os.Getenv("TICKET_ACTOR") != "planner-agent" || os.Getenv("TICKET_ORC_WORKER") != "" || os.Getenv(supervisedRoleEnv) != "" {
		fail(fmt.Errorf("incorrect managed environment"))
	}
	args := os.Args[slices.Index(os.Args, "--")+1:]
	for _, want := range []string{"exec", "--json", "model-fixture", `model_reasoning_effort="high"`, `service_tier="fast"`, "danger-full-access", "--add-dir", os.Getenv("ORC_CONVERSATION_TEST_WORKSPACE")} {
		if !slices.Contains(args, want) {
			fail(fmt.Errorf("missing captured argument %s", want))
		}
	}
	fmt.Println(`{"type":"thread.started","thread_id":"conversation-test-thread"}`)
	// Ticket's own actor command is the authoritative attribution check.
	actor, err := exec.Command("ticket", "actor", "-j").Output()
	if err != nil {
		fail(err)
	}
	if !bytes.Contains(actor, []byte(`"actor":"planner-agent"`)) {
		fail(fmt.Errorf("incorrect Ticket actor"))
	}
	if !slices.Contains(args, "resume") {
		create := exec.Command("ticket", "create", "Planned by operator agent", "Implement the requested fixture plan", "-j")
		output, err := create.CombinedOutput()
		if err != nil {
			fail(fmt.Errorf("fixture creation failed: %s", output))
		}
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(output, &created); err != nil || created.ID == "" {
			fail(fmt.Errorf("invalid create receipt"))
		}
		update := exec.Command("ticket", "update", created.ID, "--priority", "1", "-j")
		if output, err := update.CombinedOutput(); err != nil {
			fail(fmt.Errorf("fixture update failed: %s", output))
		}
		message, _ := json.Marshal("Created and updated Ticket " + created.ID)
		fmt.Printf("{\"type\":\"item.completed\",\"item\":{\"id\":\"plan\",\"type\":\"agent_message\",\"text\":%s}}\n", message)
	}
	if gate := os.Getenv("ORC_CONVERSATION_TEST_GATE"); gate != "" && !slices.Contains(args, "resume") {
		response, err := http.Get(gate)
		if err != nil {
			fail(err)
		}
		response.Body.Close()
	}
	var out, stderr bytes.Buffer
	for _, command := range []string{"status", "pause"} {
		if code := Run([]string{command}, &out, &stderr); code != 0 {
			fail(fmt.Errorf("Orc %s failed: %s", command, stderr.String()))
		}
	}
	switch os.Getenv("ORC_CONVERSATION_TEST_MODE") {
	case "abort", "shutdown":
		// The host's operation may kill this process before its receipt is read.
		_ = Run([]string{os.Getenv("ORC_CONVERSATION_TEST_MODE")}, &out, &stderr)
		fail(fmt.Errorf("self-interruption unexpectedly returned before stopping child"))
	}
	fmt.Println(`{"type":"item.completed","item":{"id":"diagnosis","type":"agent_message","text":"Inspected Orc and paused worker dispatch."}}`)
	fmt.Println(`{"type":"turn.completed"}`)
	os.Exit(0)
}

func TestConversationManagedAgentIntegration(t *testing.T) {
	if _, err := exec.LookPath("ticket"); err != nil {
		t.Skip("Ticket CLI is required for controlled integration")
	}
	for _, mode := range []string{"plan", "abort", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			var foreignRequests atomic.Int32
			foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				foreignRequests.Add(1)
				http.Error(w, "unexpected foreign Orc request", http.StatusBadGateway)
			}))
			t.Cleanup(foreign.Close)
			foreignEndpoint := foreign.URL + "/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			gateServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(gateServer.Close)
			t.Cleanup(unblock)
			root := t.TempDir()
			working := filepath.Join(root, "project")
			workspace := filepath.Join(root, "workspace")
			instance := filepath.Join(root, "instance")
			stateDir := filepath.Join(instance, "runtime")
			for _, dir := range []string{working, workspace, instance} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			configPath := filepath.Join(instance, "config.json")
			writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q}`, stateDir))
			// Fixture subprocesses have their own routing/actor; the worker's Ticket
			// environment is never changed in the parent or used for test mutations.
			var base []string
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if !strings.HasPrefix(key, "TICKET_") {
					base = append(base, entry)
				}
			}
			repository := filepath.Join(working, "tickets")
			fixtureEnv := ticketclient.ApplyTargetEnvironment(base, "fixture-owner", ticketclient.Target{Repository: repository})
			command := exec.Command("ticket", "init", "-j")
			command.Dir = working
			command.Env = fixtureEnv
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("fixture init: %v: %s", err, output)
			}
			command = exec.Command("ticket", "info", "-j")
			command.Dir = working
			command.Env = fixtureEnv
			output, err := command.Output()
			if err != nil {
				t.Fatal(err)
			}
			var info ticketclient.RepositoryInfo
			if err := json.Unmarshal(output, &info); err != nil {
				t.Fatal(err)
			}
			if !ticketclient.ValidRepositoryID(info.ID) {
				t.Fatalf("fixture info: %s", output)
			}
			execution := state.ConversationExecution{Harness: "codex", Actor: "planner-agent", Model: "model-fixture", Reasoning: "high", CodexSandbox: "danger-full-access", CodexServiceTier: "fast", WorkingDir: working, Workspace: workspace, MaxDuration: time.Minute, Repositories: []state.ConversationRepository{{ID: info.ID, Path: info.Path}}}
			policy, err := orc.NewConversations(state.NewConversationStore(stateDir), orc.DefaultConversationLimits())
			if err != nil {
				t.Fatal(err)
			}
			profiles := orc.NewConversationProfiles([]orc.ConversationProfile{{Name: "planning", Execution: execution}})
			created, err := profiles.Create(t.Context(), policy, orc.ConversationProfileCreate{RequestID: orc.NewConversationRequestID(time.Now()), Profile: "planning", RepositoryIDs: []string{info.ID}, Operator: "requesting-human"}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			hooks := supervisor.ConversationHarnessHooks(stateDir, func(c state.Conversation) (harness.Harness, error) {
				env := conversationEnvironment(append(base, "TICKET_ORC_ENDPOINT="+foreignEndpoint, "TICKET_ORC_WORKER=incorrect", "TICKET_ORC_ACTOR=incorrect", supervisedRoleEnv+"=1", "TICKET_SCOPE=incorrect"), configPath, c)
				env = append(env, "ORC_CONVERSATION_TEST_CHILD=1", "ORC_CONVERSATION_TEST_MODE="+mode, "ORC_CONVERSATION_TEST_CWD="+working, "ORC_CONVERSATION_TEST_WORKSPACE="+workspace)
				if mode == "plan" {
					env = append(env, "ORC_CONVERSATION_TEST_GATE="+gateServer.URL)
				}
				return codex.NewManaged(os.Args[0], env, func(ctx context.Context, req harness.ProcessRequest) (harness.ProcessResult, error) {
					req.Args = append([]string{"-test.run=^TestConversationAgentProcess$", "--"}, req.Args...)
					return runConversationProcess(ctx, req)
				})
			})
			m := supervisor.NewConversationManager(policy, hooks)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				m.Shutdown(ctx)
			})
			control := &daemon.Control{}
			gate := newDispatchGate(state.NewDaemonControlStore(stateDir), state.DaemonRunning)
			config := RunConfig{ConfigPath: configPath, StateDir: stateDir, control: control, conversations: m, dispatchGate: gate, Runtime: NewRuntimeState(nil)}
			manager := newWorkerManager(t.Context(), config, os.Args[0], io.Discard, io.Discard, func(string, supervisor.WorkerState, error) {})
			shutdown := make(chan struct{})
			var once sync.Once
			wireSupervisorControlCallbacks(&config, manager, func(supervisor.RuntimeEvent) {}, shutdown, &once)
			abortResult := make(chan daemon.DaemonControlResult, 1)
			abort := control.AbortDaemon
			control.AbortDaemon = func(ctx context.Context) (daemon.DaemonControlResult, error) {
				result, err := abort(ctx)
				abortResult <- result
				return result, err
			}
			key, err := daemon.GenerateEndpointKey()
			if err != nil {
				t.Fatal(err)
			}
			server, err := daemon.NewServer(daemon.Config{StateDir: stateDir, EndpointKey: key, Control: control, Status: func() daemon.Status { return daemon.Status{Mode: string(gate.Mode())} }})
			if err != nil {
				t.Fatal(err)
			}
			if err := server.SetConversations(&daemon.ConversationAPI{Policy: policy, Profiles: profiles, Manager: m}); err != nil {
				t.Fatal(err)
			}
			if err := server.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
			})
			// Exercise real authenticated HTTP acceptance and the existing SSE path.
			streamRequest, _ := http.NewRequestWithContext(waitCtx, "GET", server.Endpoint().CapabilityURL()+"/v1/events", nil)
			streamResponse, err := http.DefaultClient.Do(streamRequest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { streamResponse.Body.Close() })
			streamReader := bufio.NewReader(streamResponse.Body)
			readEvent := func() daemon.Event {
				t.Helper()
				for {
					line, err := streamReader.ReadString('\n')
					if err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(line, "data: ") {
						var e daemon.Event
						if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e); err != nil {
							t.Fatal(err)
						}
						return e
					}
				}
			}
			if e := readEvent(); e.Type != "stream.sync" {
				t.Fatal(e)
			}
			body, _ := json.Marshal(orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "Plan a ticket and diagnose Orc", Operator: "requesting-human"})
			turnRequest, _ := http.NewRequestWithContext(waitCtx, "POST", server.Endpoint().CapabilityURL()+"/v1/conversations/"+created.ConversationID+"/turns", bytes.NewReader(body))
			turnRequest.Header.Set("Content-Type", "application/json")
			turnResponse, err := http.DefaultClient.Do(turnRequest)
			if err != nil {
				t.Fatal(err)
			}
			var receipt state.ConversationReceipt
			if err := json.NewDecoder(turnResponse.Body).Decode(&receipt); err != nil {
				t.Fatal(err)
			}
			turnResponse.Body.Close()
			if turnResponse.StatusCode != 202 || receipt.TurnID == "" {
				t.Fatalf("turn response: %d %+v", turnResponse.StatusCode, receipt)
			}
			if mode == "plan" {
				for {
					event := readEvent()
					if event.Conversation != nil && event.Conversation.Message != nil && event.Conversation.Message.Role == "assistant" {
						break
					}
				}
				current, err := policy.Get(waitCtx, created.ConversationID)
				if err != nil || current.Turns[0].State != "running" {
					t.Fatalf("message did not arrive before exit: %+v %v", current, err)
				}
				// Recover the same persisted message via HTTP while the process is gated.
				transcript, err := http.Get(server.Endpoint().CapabilityURL() + "/v1/conversations/" + created.ConversationID + "/transcript")
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(transcript.Body)
				transcript.Body.Close()
				if err != nil || !bytes.Contains(data, []byte("Created and updated Ticket")) {
					t.Fatalf("live transcript: %s %v", data, err)
				}
				streamResponse.Body.Close()
				unblock()
			}

			if err := m.Wait(waitCtx, receipt.TurnID); err != nil {
				t.Fatal(err)
			}
			c, err := policy.Get(t.Context(), created.ConversationID)
			if err != nil {
				t.Fatal(err)
			}
			if c.Operator != "requesting-human" || c.Execution.Actor != "planner-agent" {
				t.Fatal("human and agent attribution conflated")
			}
			turn := c.Turns[len(c.Turns)-1]
			want := "completed"
			if mode != "plan" {
				want = "interrupted"
			}
			if turn.State != want {
				t.Fatalf("turn=%#v records=%#v", turn, c.Records)
			}
			data, _ := json.Marshal(c.Records)
			if bytes.Contains(data, []byte(key)) {
				t.Fatal("daemon credential leaked to transcript")
			}
			if !bytes.Contains(data, []byte("Created and updated Ticket")) {
				t.Fatalf("Ticket planning/creation missing: %s", data)
			}
			switch mode {
			case "abort":
				select {
				case result := <-abortResult:
					if result.Mode != "aborted" || len(result.Targets) != 1 || result.Targets[0].TurnID != receipt.TurnID || result.Targets[0].Outcome != "terminated" {
						t.Fatalf("abort receipt: %#v", result)
					}
				case <-waitCtx.Done():
					t.Fatal("missing abort receipt")
				}
			case "shutdown":
				select {
				case <-shutdown:
				case <-waitCtx.Done():
					t.Fatal("missing shutdown completion")
				}
			case "plan":
				if gate.Mode() != state.DaemonPaused {
					t.Fatal("child control did not pause dispatch")
				}
				next, err := m.Submit(t.Context(), c.ID, orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "Continue diagnosis", Operator: "another-human"}, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if err := m.Wait(waitCtx, next.TurnID); err != nil {
					t.Fatal(err)
				}
				c, err = policy.Get(t.Context(), c.ID)
				if err != nil || len(c.Turns) != 2 || c.Turns[1].State != "completed" || c.SessionID != "conversation-test-thread" {
					t.Fatalf("resume: %#v %v", c, err)
				}
			}
			// Child-stop and host-control completion above are the barriers for this
			// negative assertion; no later request can be issued by the conversation.
			if got := foreignRequests.Load(); got != 0 {
				t.Fatalf("conversation sent %d requests to inherited foreign endpoint", got)
			}
		})
	}
}

func TestConversationOutputFailureStopsControlledChild(t *testing.T) {
	if os.Getenv("ORC_CONVERSATION_OUTPUT_CHILD") == "1" {
		fmt.Println("output")
		// A real OS pipe stays open until the parent kills the owned group. No
		// elapsed-time condition is used as evidence of completion.
		r, w, err := os.Pipe()
		if err != nil {
			os.Exit(2)
		}
		defer r.Close()
		defer w.Close()
		_, _ = r.Read(make([]byte, 1))
		os.Exit(3)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sentinel := fmt.Errorf("controlled output failure")
	result, err := runConversationProcess(ctx, harness.ProcessRequest{Executable: os.Args[0], Args: []string{"-test.run=^TestConversationOutputFailureStopsControlledChild$"}, Env: append(os.Environ(), "ORC_CONVERSATION_OUTPUT_CHILD=1"), Stdout: conversationFailWriter{err: sentinel}})
	if err != sentinel || result.ExitCode == 0 || ctx.Err() != nil {
		t.Fatalf("output failure did not stop owned child: %#v %v ctx=%v", result, err, ctx.Err())
	}
}

type conversationFailWriter struct{ err error }

func (w conversationFailWriter) Write([]byte) (int, error) { return 0, w.err }
