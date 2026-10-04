package integration

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestDynamicSteerContainsTwoSameTicketStalls(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local listener unavailable in this environment: %v", err)
	}
	_ = listener.Close()

	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	codexLog := filepath.Join(root, "codex.jsonl")
	ticketLog := filepath.Join(root, "ticket.log")
	ticketActorLog := filepath.Join(root, "ticket-actors.log")
	ticketStateFile := filepath.Join(root, "ticket-state")
	activeFile := filepath.Join(root, "ticket-active")
	watchEventFile := filepath.Join(root, "watch-event")
	for _, path := range []string{codexLog, ticketLog, ticketActorLog} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(ticketStateFile, []byte("open"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activeFile, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}
	const actor = "dynamic-livelock-coder"
	const thread = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"
	values := map[string]string{
		"TICKET_ORC": root, "TICKET_ORC_FAKE_INFO_PATH": root, "TICKET_ORC_FAKE_ACTOR": actor,
		"TICKET_ORC_FAKE_REPOSITORY_ID":     integrationRepositoryID(root),
		"TICKET_ORC_FAKE_TICKET_ID":         integrationTicketID,
		"TICKET_ORC_FAKE_TICKET_STATE":      "open",
		"TICKET_ORC_FAKE_TICKET_STATE_FILE": ticketStateFile,
		"TICKET_ORC_FAKE_TICKET_LOG":        ticketLog,
		"TICKET_ORC_FAKE_TICKET_ACTOR_LOG":  ticketActorLog,
		"TICKET_ORC_FAKE_CODEX_LOG":         codexLog,
		"TICKET_ORC_FAKE_WATCH_EVENT_FILE":  watchEventFile,
		"TICKET_ORC_FAKE_READY_SEQUENCE":    "1",
		"TICKET_ORC_FAKE_ACTIVE_FILE":       activeFile,
		"TICKET_ACTOR":                      actor, "CODEX_HOME": codexHome, "CODEX_THREAD_ID": thread,
	}
	env := testEnv(helperDir, values, nil)
	init := exec.Command(binary, "init")
	init.Dir = root
	init.Env = env
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("init failed: %v: %s", err, output)
	}
	configData, err := os.ReadFile(filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		ID    string `json:"id"`
		Roles map[string]struct {
			TicketQueue string `json:"ticket_queue"`
			NudgePrompt string `json:"nudge_prompt"`
			MaxBounces  int    `json:"max_bounces"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		t.Fatal(err)
	}
	coder := config.Roles["coder"]
	if config.ID == "" || coder.TicketQueue != "open" || coder.NudgePrompt == "" {
		t.Fatalf("init did not configure dynamic coder policy: id=%q coder=%#v", config.ID, coder)
	}
	// Generated configurations leave the optional livelock circuit disabled.
	// Enable the existing threshold for this focused orchestration regression.
	var editableConfig map[string]any
	if err := json.Unmarshal(configData, &editableConfig); err != nil {
		t.Fatal(err)
	}
	roles, ok := editableConfig["roles"].(map[string]any)
	if !ok {
		t.Fatalf("init roles have unexpected type: %T", editableConfig["roles"])
	}
	editableCoder, ok := roles["coder"].(map[string]any)
	if !ok {
		t.Fatalf("init coder role has unexpected type: %T", roles["coder"])
	}
	editableCoder["max_bounces"] = 6
	configData, err = json.Marshal(editableConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "config.json"), configData, 0o600); err != nil {
		t.Fatal(err)
	}

	localDir := filepath.Join(root, ".local")
	runOut, err := os.Create(filepath.Join(root, "run.out"))
	if err != nil {
		t.Fatal(err)
	}
	runErr, err := os.Create(filepath.Join(root, "run.err"))
	if err != nil {
		_ = runOut.Close()
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "run", "--port", "0")
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = runOut
	cmd.Stderr = runErr
	if err := cmd.Start(); err != nil {
		_ = runOut.Close()
		_ = runErr.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = runOut.Close()
		_ = runErr.Close()
	})
	waitForDaemonEndpoint(t, localDir)
	statusClient, err := daemonclient.NewWithEndpoint(localDir, "", "")
	if err != nil {
		t.Fatalf("create daemon status client: %v", err)
	}
	join := exec.Command(binary, "join")
	join.Dir = root
	join.Env = env
	if output, err := join.CombinedOutput(); err != nil {
		t.Fatalf("join failed: %v: %s", err, output)
	}
	resumeSteer := func() {
		t.Helper()
		resume := exec.Command(binary, "resume", "--config", filepath.Join(root, "config.json"))
		resume.Dir = root
		resume.Env = env
		if output, err := resume.CombinedOutput(); err != nil {
			t.Fatalf("resume dynamic steer reconciliation: %v: %s", err, output)
		}
	}

	diagnostics := func() string {
		return "codex=" + readText(codexLog) + "\nticket=" + readText(ticketLog) +
			"\nloop=" + readText(filepath.Join(localDir, "state.json")) +
			"\nstdout=" + readText(filepath.Join(root, "run.out")) + "\nstderr=" + readText(filepath.Join(root, "run.err"))
	}
	waitForDynamicSteerLivelock(t, 15*time.Second, diagnostics, func() bool {
		return len(readCodexCalls(t, codexLog)) >= 1
	})
	if err := os.WriteFile(watchEventFile, []byte("change"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForDynamicSteerLivelock(t, 15*time.Second, diagnostics, func() bool {
		return countDynamicSteerWorkWakes(readCodexCalls(t, codexLog), coder.NudgePrompt) == 1
	})
	setClaimActive := func(active bool) {
		t.Helper()
		value := "0"
		if active {
			value = "1"
		}
		if err := os.WriteFile(activeFile, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loopStore := state.NewForRepository(localDir, integrationRepositoryID(root))
	readLoop := func() (state.TicketLoop, bool) {
		t.Helper()
		loop, found, err := loopStore.TicketLoop(t.Context(), integrationTicketID)
		if err != nil {
			t.Fatalf("read ticket loop: %v", err)
		}
		return loop, found
	}
	setClaimActive(true)
	resumeSteer()
	waitForDynamicSteerLivelock(t, 10*time.Second, diagnostics, func() bool {
		loop, found := readLoop()
		return found && loop.ClaimState == "open" && loop.ClaimActor == actor
	})
	setClaimActive(false)
	resumeSteer()
	waitForDynamicSteerLivelock(t, 10*time.Second, diagnostics, func() bool {
		loop, found := readLoop()
		return found && loop.StallCount == 1 && countDynamicSteerWorkWakes(readCodexCalls(t, codexLog), coder.NudgePrompt) == 2
	})
	setClaimActive(true)
	resumeSteer()
	waitForDynamicSteerLivelock(t, 10*time.Second, diagnostics, func() bool {
		loop, found := readLoop()
		return found && loop.StallCount == 1 && loop.ClaimState == "open" && loop.ClaimActor == actor
	})
	setClaimActive(false)
	resumeSteer()
	waitForDynamicSteerLivelock(t, 10*time.Second, diagnostics, func() bool {
		loop, found := readLoop()
		return found && loop.Phase == state.TicketLoopHeld && loop.StallCount == 2 && loop.ClaimState == ""
	})
	if got := strings.TrimSpace(readText(ticketStateFile)); got != "hold" {
		t.Fatalf("fake Ticket state=%q, want hold after the held-state barrier; %s", got, diagnostics())
	}
	if got := countTicketRequest(ticketLog, "hold "+integrationTicketID); got != 1 {
		t.Fatalf("ordinary Ticket hold calls=%d, want one after the held-state barrier; %s", got, diagnostics())
	}

	calls := readCodexCalls(t, codexLog)
	workWakes := countDynamicSteerWorkWakes(calls, coder.NudgePrompt)
	if workWakes != 2 {
		t.Fatalf("work-bearing wakes=%d, want exactly two; %s", workWakes, diagnostics())
	}
	workBearingCalls := make([]codexCall, 0, workWakes)
	for _, call := range calls {
		if strings.Contains(call.Prompt, coder.NudgePrompt) {
			workBearingCalls = append(workBearingCalls, call)
		}
	}
	if !strings.Contains(calls[0].Prompt, "[ticket-orc] This session is registered for Orc-steered Ticket work.") {
		t.Fatalf("bootstrap wake omitted registration message: %q", calls[0].Prompt)
	}
	if strings.Contains(workBearingCalls[len(workBearingCalls)-1].Prompt, "[ticket-orc] This session is registered for Orc-steered Ticket work.") {
		t.Fatalf("follow-up work wake repeated registration bootstrap: %q", workBearingCalls[len(workBearingCalls)-1].Prompt)
	}
	if got := readText(ticketActorLog); !strings.Contains(got, "ticket-orc."+config.ID+" hold "+integrationTicketID) {
		t.Fatalf("Ticket hold was not issued as the Orc actor; actor log=%q", got)
	}
	// Replacing the session explicitly triggers dynamic-steer reconciliation.
	// Its appearance in daemon status proves the scheduler finished publishing
	// the pass caused by this registration change.
	const replacementThread = "01a0dcb8-0ad2-7013-93a9-e4d62a7d143c"
	replacementJoin := exec.Command(binary, "join")
	replacementJoin.Dir = root
	replacementJoin.Env = testEnv(helperDir, envWith(values, "CODEX_THREAD_ID", replacementThread), nil)
	if output, err := replacementJoin.CombinedOutput(); err != nil {
		t.Fatalf("replace dynamic steer session for reconciliation barrier: %v: %s", err, output)
	}
	waitForDynamicSteerLivelock(t, 10*time.Second, diagnostics, func() bool {
		status, err := statusClient.Status(t.Context())
		if err != nil {
			return false
		}
		for _, session := range status.Steer {
			if session.RepositoryID == integrationRepositoryID(root) && session.Actor == actor && session.Session == replacementThread && session.Code == "ticket_containment_pending" {
				return true
			}
		}
		return false
	})
	loop, found := readLoop()
	if !found || loop.Phase != state.TicketLoopHeld || loop.StallCount != 2 || loop.ClaimState != "" {
		t.Fatalf("held ticket loop after reconciliation=%#v found=%t; %s", loop, found, diagnostics())
	}
	if calls := countDynamicSteerWorkWakes(readCodexCalls(t, codexLog), coder.NudgePrompt); calls != 2 {
		t.Fatalf("work wakes after containment: got %d, want exactly two; %s", calls, diagnostics())
	}
	if got := countTicketRequest(ticketLog, "hold "+integrationTicketID); got != 1 {
		t.Fatalf("ordinary Ticket hold calls after reconciliation=%d, want one; %s", got, diagnostics())
	}
}

func countDynamicSteerWorkWakes(calls []codexCall, prompt string) int {
	count := 0
	for _, call := range calls {
		if strings.Contains(call.Prompt, prompt) {
			count++
		}
	}
	return count
}

func waitForDynamicSteerLivelock(t *testing.T, timeout time.Duration, diagnostics func() string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("dynamic-steer livelock condition timed out: %s", diagnostics())
}
