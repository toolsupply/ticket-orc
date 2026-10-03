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

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestDynamicSteerExecutableSendsSecondWakeAfterClaimEvidence(t *testing.T) {
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
	watchEventFile := filepath.Join(root, "watch-event")
	if err := os.WriteFile(codexLog, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ticketLog, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	const actor = "dynamic-coder"
	const thread = "01a0da4e-aa3a-78d3-87ba-b5972a10e2a5"
	values := map[string]string{
		"TICKET_ORC": root, "TICKET_ORC_FAKE_INFO_PATH": root, "TICKET_ORC_FAKE_ACTOR": actor,
		"TICKET_ORC_FAKE_REPOSITORY_ID": integrationRepositoryID(root), "TICKET_ORC_FAKE_TICKET_LOG": ticketLog,
		"TICKET_ORC_FAKE_WATCH_EVENT_FILE": watchEventFile,
		"TICKET_ORC_FAKE_CODEX_LOG":        codexLog, "TICKET_ORC_FAKE_TICKET_ID": integrationTicketID,
		"TICKET_ORC_FAKE_READY_SEQUENCE": "1,1,0", "TICKET_ORC_FAKE_ACTIVE_SEQUENCE": "0,0,1,0,0,0",
		"TICKET_ACTOR": actor, "CODEX_HOME": codexHome, "CODEX_THREAD_ID": thread,
	}
	init := exec.Command(binary, "init")
	init.Dir = root
	init.Env = testEnv(helperDir, values, nil)
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("init failed: %v: %s", err, output)
	}
	configData, err := os.ReadFile(filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		ID          string `json:"id"`
		DefaultRole string `json:"default_role"`
		Roles       map[string]struct {
			TicketQueue string `json:"ticket_queue"`
			NudgePrompt string `json:"nudge_prompt"`
		} `json:"roles"`
		Workers map[string]json.RawMessage `json:"workers"`
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		t.Fatal(err)
	}
	if config.DefaultRole != "coder" || config.Roles["coder"].TicketQueue != "open" || config.Roles["reviewer"].TicketQueue != "review" || len(config.Workers) != 0 {
		t.Fatalf("init config does not describe a zero-worker default instance: %#v", config)
	}
	var editableConfig map[string]any
	if err := json.Unmarshal(configData, &editableConfig); err != nil {
		t.Fatal(err)
	}
	roles, ok := editableConfig["roles"].(map[string]any)
	if !ok {
		t.Fatalf("init roles have unexpected type: %T", editableConfig["roles"])
	}
	coder, ok := roles["coder"].(map[string]any)
	if !ok {
		t.Fatalf("init coder role has unexpected type: %T", roles["coder"])
	}
	coder["ticket_tags"] = []string{"backend", "urgent"}
	configData, err = json.Marshal(editableConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "config.json"), configData, 0o600); err != nil {
		t.Fatal(err)
	}
	localDir := filepath.Join(root, ".local")
	prompt := config.Roles["coder"].NudgePrompt
	reviewerPrompt := config.Roles["reviewer"].NudgePrompt
	if !strings.Contains(reviewerPrompt, "substantive review") || !strings.Contains(reviewerPrompt, "specification compliance") || strings.Contains(strings.ToLower(reviewerPrompt), "approve") || strings.Contains(strings.ToLower(reviewerPrompt), "close") || strings.Contains(strings.ToLower(reviewerPrompt), "skill") {
		t.Fatalf("init reviewer nudge is incomplete or contains lifecycle/Skill policy: %q", reviewerPrompt)
	}
	if got := readText(ticketLog); got != "" {
		t.Fatalf("init contacted Ticket: %q", got)
	}
	env := testEnv(helperDir, values, nil)
	runOut, err := os.Create(filepath.Join(root, "run.out"))
	if err != nil {
		t.Fatal(err)
	}
	runErr, err := os.Create(filepath.Join(root, "run.err"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "run", "--port", "0")
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = runOut
	cmd.Stderr = runErr
	if err := cmd.Start(); err != nil {
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
	if _, err := os.Stat(filepath.Join(localDir, "run", "lock")); err != nil {
		t.Fatalf("daemon ownership lock was not created under selected local root: %v", err)
	}
	if _, err := os.Stat(daemon.EndpointPath(root)); !os.IsNotExist(err) {
		t.Fatalf("daemon endpoint leaked into config directory: %v", err)
	}
	configPath := filepath.Join(root, "config.json")
	for _, operation := range []string{"pause", "resume"} {
		control := exec.Command(binary, operation, "--config", configPath)
		control.Dir = root
		control.Env = env
		if output, err := control.CombinedOutput(); err != nil {
			t.Fatalf("daemon %s through selected config: %v: %s", operation, err, output)
		}
	}
	if _, err := os.Stat(filepath.Join(localDir, "daemon-control.json")); err != nil {
		t.Fatalf("daemon control state was not written under selected local root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "daemon-control.json")); !os.IsNotExist(err) {
		t.Fatalf("daemon control state leaked into config directory: %v", err)
	}
	join := exec.Command(binary, "join")
	join.Dir = root
	join.Env = env
	if output, err := join.CombinedOutput(); err != nil {
		t.Fatalf("join failed: %v: %s", err, output)
	}
	// Initial READY establishes the watch baseline. Signal one repository change
	// after the first wake; that event must trigger an authoritative targeted
	// query that observes the active claim, without an idle safety sweep.
	firstWakeDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(firstWakeDeadline) && len(readCodexCalls(t, codexLog)) < 1 {
		time.Sleep(25 * time.Millisecond)
	}
	if len(readCodexCalls(t, codexLog)) != 1 {
		_ = cmd.Process.Kill()
		t.Fatalf("initial Codex wake was not sent; ticket=%q runtime=%q", readText(ticketLog), readText(filepath.Join(localDir, "steer-runtime.json")))
	}
	if got := readText(ticketLog); !strings.Contains(got, "ready open --tag backend --tag urgent --limit 1 --fields id") {
		_ = cmd.Process.Kill()
		t.Fatalf("steering readiness did not use the complete configured role selector: %q", got)
	}
	if err := os.WriteFile(watchEventFile, []byte("change"), 0o600); err != nil {
		t.Fatal(err)
	}
	claimDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(claimDeadline) && countTicketRequest(ticketLog, "list --state open") < 2 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := countTicketRequest(ticketLog, "list --state open"); got < 2 {
		_ = cmd.Process.Kill()
		t.Fatalf("active claim was not observed after the repository watch event; open-queue observations=%d ticket=%q", got, readText(ticketLog))
	}
	resume := exec.Command(binary, "resume", "--config", configPath)
	resume.Dir = root
	resume.Env = env
	if output, err := resume.CombinedOutput(); err != nil {
		t.Fatalf("daemon resume after claim observation failed: %v: %s", err, output)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		calls := readCodexCalls(t, codexLog)
		if len(calls) >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	calls := readCodexCalls(t, codexLog)
	if len(calls) != 2 {
		_ = cmd.Process.Kill()
		t.Fatalf("Codex wakes=%d, want exactly two; codex=%q ticket=%q runtime=%q stdout=%q stderr=%q", len(calls), readText(codexLog), readText(ticketLog), readText(filepath.Join(localDir, "steer-runtime.json")), readText(filepath.Join(root, "run.out")), readText(filepath.Join(root, "run.err")))
	}
	if !strings.Contains(calls[0].Prompt, "[ticket-orc] This session is registered for Orc-steered Ticket work.") || !strings.Contains(calls[0].Prompt, prompt) {
		t.Fatalf("first wake prompt=%q, want bootstrap and configured role prompt", calls[0].Prompt)
	}
	if calls[1].Prompt != prompt {
		t.Fatalf("second wake prompt=%q, want configured role prompt", calls[1].Prompt)
	}
	if strings.Contains(calls[1].Prompt, "[ticket-orc] This session is registered for Orc-steered Ticket work.") {
		t.Fatalf("second wake repeated bootstrap prompt: %q", calls[1].Prompt)
	}
	for i, call := range calls {
		if !containsPair(call.Args, "--thread", thread) {
			t.Fatalf("wake %d args=%#v, want exact registered thread", i, call.Args)
		}
	}
	registrations, err := state.NewRegistrationStore(localDir).Snapshot(t.Context())
	if err != nil || len(registrations.Registrations) != 1 || registrations.Registrations[0].RegistrationID == "" {
		t.Fatalf("registration=%#v err=%v", registrations, err)
	}
	if got := countTicketRequest(ticketLog, "ready open"); got < 2 {
		t.Fatalf("readiness observations=%d; ticket log=%q", got, readText(ticketLog))
	}
	if got := countTicketRequest(ticketLog, "watch -j --ready"); got < 1 {
		t.Fatalf("Ticket watch readiness invocations=%d; ticket log=%q", got, readText(ticketLog))
	}
	if strings.Contains(readText(ticketLog), "claim") {
		t.Fatalf("scheduler mutated/claimed Ticket work: %q", readText(ticketLog))
	}
	_ = cmd.Process.Kill()
}

func containsPair(args []string, key, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == key && args[i+1] == value {
			return true
		}
	}
	return false
}
