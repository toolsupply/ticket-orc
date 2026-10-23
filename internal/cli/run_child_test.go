package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestAttributedWritersKeepPartialLinesSeparate(t *testing.T) {
	var output bytes.Buffer
	var mu sync.Mutex
	first := &attributedWriter{dst: &output, prefix: "[first] ", start: true, mu: &mu}
	second := &attributedWriter{dst: &output, prefix: "[second] ", start: true, mu: &mu}
	if _, err := first.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("complete\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "[second] complete\n[first] partial\n"; got != want {
		t.Fatalf("attributed output = %q, want %q", got, want)
	}
}

func TestRunChildEnvironmentBindsResolvedTicketTarget(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	base := []string{"TICKET_ACTOR=inherited", "TICKET_REPOSITORY=inherited-repository", "TICKET_CONFIG=inherited-config", "TICKET_SCOPE=inherited-scope", "TICKET_ROOT=inherited-root", "TICKET_ORC=wrong-instance", "TICKET_ORC_CONFIG=wrong-config", "TICKET_ORC_STATE_DIR=wrong-root", "TICKET_ORC_DAEMON_TOKEN=secret", "TICKET_ORC_ENDPOINT_TOKEN=secret", "TICKET_ORC_BEARER_TOKEN=secret", "OTHER=preserved"}
	value := func(environment []string, key string) string {
		for _, entry := range environment {
			if strings.HasPrefix(entry, key+"=") {
				return strings.TrimPrefix(entry, key+"=")
			}
		}
		return "<missing>"
	}
	const capabilityURL = "http://127.0.0.1:43123/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	instanceDir := filepath.Dir(configPath)
	direct := runChildEnvironment(base, configPath, supervisor.RoleConfig{Actor: "worker", Repository: "/repo", Ticket: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/repo"}}, nil)
	if value(direct, "TICKET_ACTOR") != "worker" || value(direct, "TICKET_REPOSITORY") != "/repo" || value(direct, "TICKET_CONFIG") != "<missing>" || value(direct, "TICKET_SCOPE") != "<missing>" || value(direct, "TICKET_ROOT") != "<missing>" || value(direct, "TICKET_ORC") != instanceDir || value(direct, "TICKET_ORC_CONFIG") != "<missing>" || value(direct, "TICKET_ORC_STATE_DIR") != "<missing>" || value(direct, "TICKET_ORC_DAEMON_TOKEN") != "<missing>" || value(direct, "TICKET_ORC_ENDPOINT_TOKEN") != "<missing>" || value(direct, "TICKET_ORC_BEARER_TOKEN") != "<missing>" || value(direct, "OTHER") != "preserved" {
		t.Fatalf("direct child environment = %#v", direct)
	}
	scoped := runChildEnvironment(base, configPath, supervisor.RoleConfig{Actor: "worker", Ticket: supervisor.TicketTarget{Mode: TicketTargetScoped, Config: "/config/ticket.json", Scope: "named"}}, &ticketclient.RepositoryInfo{Path: "/resolved/repository"})
	if value(scoped, "TICKET_ACTOR") != "worker" || value(scoped, "TICKET_REPOSITORY") != "/resolved/repository" || value(scoped, "TICKET_CONFIG") != "<missing>" || value(scoped, "TICKET_SCOPE") != "<missing>" || value(scoped, "TICKET_ROOT") != "<missing>" || value(scoped, "TICKET_ORC") != instanceDir || value(scoped, "TICKET_ORC_CONFIG") != "<missing>" || value(scoped, "OTHER") != "preserved" {
		t.Fatalf("scoped child environment = %#v", scoped)
	}
}

func TestRunChildEnvironmentRoutesScopedTargetThroughTicketProtocol(t *testing.T) {
	ticket, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("Ticket CLI is not installed")
	}
	root := t.TempDir()
	repository := filepath.Join(root, "tickets")
	configPath := filepath.Join(root, "ticket-config.json")
	config, err := json.Marshal(map[string]any{"scopes": map[string]any{
		"managed": map[string]any{"repository": repository},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	workingDir := root
	init := exec.Command(ticket, "--config", configPath, "--scope", "managed", "init", "-j")
	init.Dir = workingDir
	init.Env = ticketclient.ApplyTargetEnvironment(os.Environ(), "coder", ticketclient.Target{Config: configPath, Scope: "managed"})
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initialize configured scope: %v\n%s", err, output)
	}

	info, err := ticketclient.ProbeInfo(context.Background(), "coder", workingDir, ticketclient.Target{Config: configPath, Scope: "managed"})
	if err != nil {
		t.Fatalf("resolve configured scope: %v", err)
	}
	workerConfig := supervisor.RoleConfig{Actor: "coder", WorkingDir: workingDir, Ticket: supervisor.TicketTarget{Mode: TicketTargetScoped, Config: configPath, Scope: "managed"}}
	env := runChildEnvironment(os.Environ(), filepath.Join(root, "orc.json"), workerConfig, &info)
	command := exec.Command(ticket, "info", "-j")
	command.Dir = workingDir
	command.Env = env
	output, err := command.Output()
	if err != nil {
		t.Fatalf("Ticket info through managed child environment: %v", err)
	}
	var got ticketclient.RepositoryInfo
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode child Ticket info %q: %v", output, err)
	}
	if got.Path != info.Path || got.Scope != nil {
		t.Fatalf("child Ticket target = %#v, want resolved repository %q without unsupported scope/config environment", got, info.Path)
	}
}

func TestWorkerEventWriterConsumesStructuredEventsAndPreservesDiagnostics(t *testing.T) {
	var output bytes.Buffer
	var events []orc.Event
	writer := &workerEventWriter{dst: &output, sink: func(event orc.Event) { events = append(events, event) }}
	input := "ordinary diagnostic\n" + orc.EventStreamPrefix + `{"type":"steer.delivery","worker":"coder","role":"coder","phase":"accepted","code":"delivery_accepted","ticket":"20260920-12345"}` + "\npartial"
	if n, err := writer.Write([]byte(input)); err != nil || n != len(input) {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := output.String(); got != "ordinary diagnostic\npartial\n" {
		t.Fatalf("diagnostic output = %q", got)
	}
	if len(events) != 1 || events[0].Type != "steer.delivery" || events[0].Ticket != "20260920-12345" || events[0].Code != "delivery_accepted" {
		t.Fatalf("events = %#v", events)
	}
}

func TestWorkerEventWriterCapturesChildFailureEnvelope(t *testing.T) {
	var output bytes.Buffer
	var events []orc.Event
	child := &runChild{}
	writer := &workerEventWriter{dst: &output, sink: func(event orc.Event) {
		if child.captureFailure(event) {
			return
		}
		events = append(events, event)
	}}
	failureLine := orc.EventStreamPrefix + `{"type":"worker.failure","worker":"reviewer","role":"reviewer","failure":{"classification":"ticket_command_failed","phase":"worker operation","exit_code":1,"ticket_code":"ticket_not_ready"}}` + "\n"
	queueLine := orc.EventStreamPrefix + `{"type":"worker.state","worker":"reviewer","role":"reviewer","state":"quiescent"}` + "\n"
	input := failureLine + queueLine
	if n, err := writer.Write([]byte(input)); err != nil || n != len(input) {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if output.Len() != 0 || len(events) != 1 || events[0].Type != "worker.state" {
		t.Fatalf("output=%q events=%#v", output.String(), events)
	}
	failure := child.failureEnvelope()
	if failure == nil || failure.Classification != "ticket_command_failed" || failure.TicketCode != "ticket_not_ready" || failure.ExitCode != 1 {
		t.Fatalf("captured failure=%#v", failure)
	}
}

func TestStopRunChildrenReapsExitedAndForcedKillChildren(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "state")
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_FAIL", "exited")
	t.Setenv("TICKET_ORC_TEST_CHILD_IGNORE", "forced")
	oldTimeout := runChildStopTimeout
	runChildStopTimeout = 40 * time.Millisecond
	t.Cleanup(func() { runChildStopTimeout = oldTimeout })
	var output, diagnostics bytes.Buffer
	executable := os.Args[0]
	exited, err := startRunChild(context.Background(), configPath, executable, supervisorTestWorker("exited", stateDir), &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start exited child: %v", err)
	}
	stopRunChildAtTestCleanup(t, exited)
	forced, err := startRunChild(context.Background(), configPath, executable, supervisorTestWorker("forced", stateDir), &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start forced child: %v", err)
	}
	stopRunChildAtTestCleanup(t, forced)
	waitForSupervisorTestFile(t, filepath.Join(childDir, "exited.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "forced.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "forced.armed"))
	stopRunChildren([]*runChild{exited, forced})
	for _, child := range []*runChild{exited, forced} {
		select {
		case <-child.waited:
		case <-time.After(time.Second):
			t.Fatalf("child %s was not reaped", child.worker.Name)
		}
		if got := atomic.LoadInt32(child.waitCalls); got != 1 {
			t.Fatalf("child %s wait calls = %d, want one", child.worker.Name, got)
		}
	}
}

func TestStartRunChildFlushesTrailingStructuredEventFragment(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_TEST_CHILD_EVENT_FRAGMENT", "1")
	var output, diagnostics bytes.Buffer
	var events []orc.Event
	worker := supervisorTestWorker("fragment", t.TempDir())
	worker.EventSink = func(event orc.Event) { events = append(events, event) }
	child, err := startRunChild(context.Background(), configPath, os.Args[0], worker, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	stopRunChildAtTestCleanup(t, child)
	waitForSupervisorTestFile(t, filepath.Join(childDir, "fragment.ready"))
	waitForSupervisorTestFile(t, filepath.Join(childDir, "fragment.fragment-written"))
	select {
	case <-child.ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for child readiness event before stopping it")
	}
	stopRunChildren([]*runChild{child})
	if len(events) != 2 || events[0].Type != orc.WorkerReadyEventType || events[1].Type != "worker.state" || events[1].State != "running" {
		t.Fatalf("flushed events = %#v", events)
	}
	if strings.Contains(diagnostics.String(), "ticket-orc-event") {
		t.Fatalf("structured event leaked to diagnostics: %q", diagnostics.String())
	}
}

func TestStartRunChildUsesEachWorkersWorkingDirectory(t *testing.T) {
	configPath := supervisorTestConfigPath(t)
	childDir := t.TempDir()
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	t.Setenv("TICKET_ORC_TEST_CHILD", "1")
	t.Setenv("TICKET_ORC_TEST_CHILD_DIR", childDir)
	t.Setenv("TICKET_ORC_STATE_DIR", "inherited-state-root")
	var output, diagnostics bytes.Buffer
	first := supervisorTestWorker("first", t.TempDir())
	first.Config.Codex.ServiceTier = "fast"
	first.Config.WorkingDir = firstDir
	first.Config.StateDir = filepath.Join(t.TempDir(), "first-state")
	second := supervisorTestWorker("second", t.TempDir())
	second.Config.Codex.ServiceTier = "default"
	second.Config.WorkingDir = secondDir
	second.Config.StateDir = filepath.Join(t.TempDir(), "second-state")
	firstChild, err := startRunChild(context.Background(), configPath, os.Args[0], first, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		t.Fatalf("start first child: %v", err)
	}
	stopRunChildAtTestCleanup(t, firstChild)
	secondChild, err := startRunChild(context.Background(), configPath, os.Args[0], second, &output, &diagnostics, &sync.Mutex{}, &sync.Mutex{})
	if err != nil {
		stopRunChildren([]*runChild{firstChild})
		t.Fatalf("start second child: %v", err)
	}
	stopRunChildAtTestCleanup(t, secondChild)
	firstReady := waitForSupervisorTestFile(t, filepath.Join(childDir, "first.ready"))
	secondReady := waitForSupervisorTestFile(t, filepath.Join(childDir, "second.ready"))
	if !strings.Contains(firstReady, "resolved_service_tier=fast\n") || !strings.Contains(secondReady, "resolved_service_tier=default\n") {
		t.Fatalf("child service tiers missing: %q and %q", firstReady, secondReady)
	}
	firstWorkingDir, firstHasWorkingDir := supervisorTestReadyField(firstReady, "cwd")
	secondWorkingDir, secondHasWorkingDir := supervisorTestReadyField(secondReady, "cwd")
	if !firstHasWorkingDir || !secondHasWorkingDir {
		t.Fatalf("child working directories missing from readiness payloads: %q and %q", firstReady, secondReady)
	}
	firstMatches, err := sameSupervisorTestDirectory(firstWorkingDir, firstDir)
	if err != nil {
		t.Fatal(err)
	}
	secondMatches, err := sameSupervisorTestDirectory(secondWorkingDir, secondDir)
	if err != nil {
		t.Fatal(err)
	}
	if !firstMatches || !secondMatches || !strings.Contains(firstReady, "state=\n") || !strings.Contains(secondReady, "state=\n") {
		t.Fatalf("child working directories = %q and %q", firstReady, secondReady)
	}
	if sameDirectory, err := sameSupervisorTestDirectory(firstWorkingDir, secondWorkingDir); err != nil || sameDirectory {
		t.Fatalf("child working directories must remain distinct: same=%t err=%v", sameDirectory, err)
	}
	stopRunChildren([]*runChild{firstChild, secondChild})
}
