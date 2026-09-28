package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

const integrationTicketID = "20260919-12345"

func integrationState(stateDir string) *state.Store {
	return state.NewForRepository(stateDir, integrationRepositoryID("/fake/tickets"))
}

func integrationRepositoryID(path string) string {
	hash := sha256.Sum256([]byte(filepath.Clean(path)))
	identifier := append([]byte(nil), hash[:16]...)
	identifier[6] = identifier[6]&0x0f | 0x40
	identifier[8] = identifier[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", identifier[0:4], identifier[4:6], identifier[6:8], identifier[8:10], identifier[10:16])
}

func TestRoleExecutableWorkflow(t *testing.T) {
	for _, test := range []struct {
		role       string
		state      string
		waitPhrase string
	}{
		{role: "coder", state: "review", waitPhrase: "implementation"},
		{role: "reviewer", state: "signoff", waitPhrase: "review"},
	} {
		t.Run(test.role, func(t *testing.T) {
			binary, helperDir := buildFixture(t)
			stateDir := filepath.Join(t.TempDir(), "state")
			codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
			ticketLog := filepath.Join(t.TempDir(), "ticket.log")
			env := map[string]string{
				"TICKET_ORC_FAKE_TICKET_STATE": test.state,
				"TICKET_ORC_FAKE_CLAIM_STATE":  map[string]string{"coder": "open", "reviewer": "review"}[test.role],
				"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
				"TICKET_ORC_FAKE_CODEX_LOG":    codexLog,
				"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
			}
			result := runRole(t, binary, helperDir, test.role, stateDir, codexLog, ticketLog, env)
			t.Logf("role stdout=%q stderr=%q", result.stdout, result.stderr)
			if !strings.Contains(result.stderr, "wait for "+test.waitPhrase+" work") {
				t.Fatalf("stderr = %q, want queue failure", result.stderr)
			}
			calls := readCodexCalls(t, codexLog)
			if len(calls) != 1 {
				t.Fatalf("Codex calls = %d, want exactly one after queue failure: %#v", len(calls), calls)
			}
			if calls[0].Actor != "integration-"+test.role {
				t.Fatalf("Codex actor = %q, want child actor", calls[0].Actor)
			}
			if test.role == "coder" && !strings.Contains(calls[0].Prompt, "Work only ticket "+integrationTicketID) {
				t.Fatalf("coder prompt = %q", calls[0].Prompt)
			}
			if test.role == "reviewer" && !strings.Contains(calls[0].Prompt, "Review only ticket "+integrationTicketID) {
				t.Fatalf("reviewer prompt = %q", calls[0].Prompt)
			}
			if got := countTicketRequest(ticketLog, "show"); got != 1 {
				t.Fatalf("show requests = %d, want one authoritative reread", got)
			}
		})
	}
}

func TestRoleExecutableInvalidConfigDoesNotClaim(t *testing.T) {
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ticketLog := filepath.Join(root, "ticket.log")
	cmd := exec.Command(binary, "coder", "--actor", "integration-coder", "--config", configPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = testEnv(helperDir, map[string]string{
		"TICKET_ORC_FAKE_TICKET_LOG": ticketLog,
	}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	if err := cmd.Run(); err == nil {
		t.Fatal("invalid config unexpectedly succeeded")
	}
	if !strings.Contains(stderr.String(), "unknown field") {
		t.Fatalf("stderr = %q, want strict config error", stderr.String())
	}
	if _, err := os.Stat(ticketLog); !os.IsNotExist(err) {
		t.Fatalf("ticket child was invoked; log stat = %v", err)
	}
}

func TestRoleExecutableWorkerMismatchDoesNotClaim(t *testing.T) {
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."}},"workers":{"reviewer":{"role":"reviewer"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ticketLog := filepath.Join(root, "ticket.log")
	cmd := exec.Command(binary, "coder", "--actor", "integration-coder", "--config", configPath, "--worker", "reviewer")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = testEnv(helperDir, map[string]string{"TICKET_ORC_FAKE_TICKET_LOG": ticketLog}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	if err := cmd.Run(); err == nil || !strings.Contains(stderr.String(), "cannot run coder") {
		t.Fatalf("worker mismatch error=%v stderr=%q", err, stderr.String())
	}
	if _, err := os.Stat(ticketLog); !os.IsNotExist(err) {
		t.Fatalf("ticket child was invoked; log stat = %v", err)
	}
}

func TestRoleExecutableCoderCompletionSettingDoesNotStartTicket(t *testing.T) {
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ticketLog := filepath.Join(root, "ticket.log")
	cmd := exec.Command(binary, "coder", "--config", configPath, "--actor", "integration-coder", "--review-completion", "close")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = testEnv(helperDir, map[string]string{"TICKET_ORC_FAKE_TICKET_LOG": ticketLog}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	if err := cmd.Run(); err == nil || !strings.Contains(stderr.String(), "only valid for reviewer") {
		t.Fatalf("coder completion error=%v stderr=%q", err, stderr.String())
	}
	if _, err := os.Stat(ticketLog); !os.IsNotExist(err) {
		t.Fatalf("ticket child was invoked; log stat = %v", err)
	}
}

func TestRoleExecutableRetainedAndFreshSessions(t *testing.T) {
	binary, helperDir := buildFixture(t)
	for _, policy := range []string{"ticket", "fresh"} {
		t.Run(policy, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "state")
			codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
			ticketLog := filepath.Join(t.TempDir(), "ticket.log")
			env := map[string]string{
				"TICKET_ORC_FAKE_MODE":         "ticket",
				"TICKET_ORC_FAKE_TICKET_STATE": "review",
				"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
				"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
				"TICKET_ORC_FAKE_CODEX_LOG":    codexLog,
				"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
			}
			result := runRole(t, binary, helperDir, "coder", stateDir, codexLog, ticketLog, envWith(env, "TICKET_ORC_SESSION_POLICY", policy))
			t.Logf("first role stdout=%q stderr=%q", result.stdout, result.stderr)
			calls := readCodexCalls(t, codexLog)
			if len(calls) != 1 {
				t.Fatalf("first Codex calls = %d", len(calls))
			}
			runRole(t, binary, helperDir, "coder", stateDir, codexLog, ticketLog, envWith(env, "TICKET_ORC_SESSION_POLICY", policy))
			calls = readCodexCalls(t, codexLog)
			if len(calls) != 2 {
				t.Fatalf("Codex calls = %d", len(calls))
			}
			hasResume := false
			for _, arg := range calls[1].Args {
				if arg == "resume" {
					hasResume = true
				}
			}
			if hasResume != (policy == "ticket") {
				t.Fatalf("policy %s second args = %q, resume=%v", policy, calls[1].Args, hasResume)
			}
		})
	}
}

func TestMultiHarnessExecutableSessionsAndNamedWorkers(t *testing.T) {
	binary, helperDir := buildFixture(t)
	for _, test := range []struct {
		harness string
		worker  string
		actor   string
		wantArg string
		logEnv  string
	}{
		{harness: "pi", worker: "worker-a", actor: "integration-a", wantArg: "--mode", logEnv: "TICKET_ORC_FAKE_PI_LOG"},
		{harness: "claude", worker: "worker-b", actor: "integration-b", wantArg: "--output-format", logEnv: "TICKET_ORC_FAKE_CLAUDE_LOG"},
	} {
		t.Run(test.harness, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "state")
			configPath := filepath.Join(t.TempDir(), "config.json")
			config := fmt.Sprintf(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."}},"workers":{"%s":{"role":"coder","harness":"%s","actor":"%s"}}}`, stateDir, test.worker, test.harness, test.actor)
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			logPath := filepath.Join(t.TempDir(), test.harness+".jsonl")
			ticketLog := filepath.Join(t.TempDir(), "ticket.log")
			values := map[string]string{
				"TICKET_ORC_FAKE_TICKET_STATE": "review",
				"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
				"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
				"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
				test.logEnv:                    logPath,
			}
			for _, policy := range []string{"ticket", "fresh"} {
				t.Run(policy, func(t *testing.T) {
					before := len(readHarnessCalls(t, logPath))
					runConfiguredRole(t, binary, helperDir, configPath, stateDir, test.worker, policy, values)
					calls := readHarnessCalls(t, logPath)
					if len(calls) != before+1 {
						t.Fatalf("%s calls = %d, want %d", test.harness, len(calls), before+1)
					}
					first := calls[len(calls)-1]
					if !containsArg(first.Args, test.wantArg) {
						t.Fatalf("%s args = %#v, want %q", test.harness, first.Args, test.wantArg)
					}
					runConfiguredRole(t, binary, helperDir, configPath, stateDir, test.worker, policy, values)
					calls = readHarnessCalls(t, logPath)
					if len(calls) != before+2 {
						t.Fatalf("%s calls after second run = %d", test.harness, len(calls))
					}
					hasResume := containsArg(calls[len(calls)-1].Args, "--session") || containsArg(calls[len(calls)-1].Args, "--resume")
					if hasResume != (policy == "ticket") {
						t.Fatalf("%s policy %s args = %#v, resume=%v", test.harness, policy, calls[len(calls)-1].Args, hasResume)
					}
				})
			}
			snapshot, err := integrationState(stateDir).Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			seen := false
			for _, session := range snapshot.Sessions {
				if session.Role == "coder" && session.Harness == test.harness {
					seen = true
				}
			}
			if !seen {
				t.Fatalf("retained %s session missing from state: %#v", test.harness, snapshot.Sessions)
			}
		})
	}
}

func TestSameRoleNamedWorkersSelectDifferentHarnesses(t *testing.T) {
	binary, helperDir := buildFixture(t)
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	const configID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	localDir := filepath.Join(configDir, ".local", configID)
	config := `{"version":1,"id":"` + configID + `","default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."}},"workers":{"worker-a":{"role":"coder","harness":"pi","actor":"integration-a"},"worker-b":{"role":"coder","harness":"claude","actor":"integration-b"}}}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	piLog := filepath.Join(t.TempDir(), "pi.jsonl")
	claudeLog := filepath.Join(t.TempDir(), "claude.jsonl")
	values := map[string]string{
		"TICKET_ORC_FAKE_TICKET_STATE": "review",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
		"TICKET_ORC_FAKE_PI_LOG":       piLog,
		"TICKET_ORC_FAKE_CLAUDE_LOG":   claudeLog,
	}
	runConfiguredRole(t, binary, helperDir, configPath, localDir, "worker-a", "ticket", values)
	runConfiguredRole(t, binary, helperDir, configPath, localDir, "worker-b", "ticket", values)
	if len(readHarnessCalls(t, piLog)) != 1 || len(readHarnessCalls(t, claudeLog)) != 1 {
		t.Fatalf("Pi calls=%d Claude calls=%d; both same-steer workers should run once", len(readHarnessCalls(t, piLog)), len(readHarnessCalls(t, claudeLog)))
	}
	snapshot, err := integrationState(localDir).Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seenHarnesses := map[string]bool{}
	seenOwners := map[string]bool{}
	for _, session := range snapshot.Sessions {
		seenHarnesses[session.Harness] = true
		seenOwners[session.Owner] = true
	}
	if !seenHarnesses["pi"] || !seenHarnesses["claude"] || !seenOwners["worker-a"] || !seenOwners["worker-b"] {
		t.Fatalf("mixed-harness retained sessions = %#v", snapshot.Sessions)
	}
	leases, err := os.ReadDir(filepath.Join(localDir, "workers"))
	if err != nil || len(leases) != 2 {
		t.Fatalf("worker leases=%v err=%v, want two keys beneath local root", leases, err)
	}
	logs, err := os.ReadDir(filepath.Join(localDir, "logs"))
	if err != nil || len(logs) != 2 {
		t.Fatalf("worker logs=%v err=%v, want two logs beneath local root", logs, err)
	}
	logNames := map[string]bool{}
	for _, entry := range logs {
		logNames[entry.Name()] = true
	}
	if !strings.Contains(findLogName(logNames, "worker-a"), "worker-a") || !strings.Contains(findLogName(logNames, "worker-b"), "worker-b") {
		t.Fatalf("worker raw logs are not isolated by worker key: %v", logNames)
	}
	entries, err := os.ReadDir(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name() != ".local" && entries[1].Name() != ".local" {
		t.Fatalf("generated worker data appeared beside config.json: %v", entries)
	}
}

func findLogName(names map[string]bool, worker string) string {
	for name := range names {
		if strings.Contains(name, worker) {
			return name
		}
	}
	return ""
}

func runConfiguredRole(t *testing.T, binary, helperDir, configPath, stateDir, worker, policy string, values map[string]string) {
	t.Helper()
	cmd := exec.Command(binary, "coder", "--config", configPath, "--worker", worker, "--session-policy", policy, "--output", "quiet")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	overrides := map[string]string{}
	for key, value := range values {
		overrides[key] = value
	}
	cmd.Env = testEnv(helperDir, values, overrides)
	if err := cmd.Run(); err == nil {
		t.Fatal("configured role unexpectedly succeeded after fake queue exhaustion")
	}
}

type harnessCall struct {
	Actor string   `json:"actor"`
	Args  []string `json:"args"`
}

func readHarnessCalls(t *testing.T, path string) []harnessCall {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var calls []harnessCall
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var call harnessCall
		if err := json.Unmarshal(scanner.Bytes(), &call); err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return calls
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func TestRoleExecutableBounceLimitReleasesWithoutCodex(t *testing.T) {
	binary, helperDir := buildFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if _, err := integrationState(stateDir).IncrementBounces(context.Background(), integrationTicketID); err != nil {
		t.Fatalf("seed bounce count: %v", err)
	}
	codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
	ticketLog := filepath.Join(t.TempDir(), "ticket.log")
	result := runRoleArgs(t, binary, helperDir, "coder", stateDir, codexLog, ticketLog, map[string]string{
		"TICKET_ORC_FAKE_TICKET_STATE": "review",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
		"TICKET_ORC_FAKE_CODEX_LOG":    codexLog,
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
	}, []string{"--max-bounces", "1"})
	if !strings.Contains(result.stderr, "bounce limit reached") {
		t.Fatalf("stderr = %q, want bounce limit", result.stderr)
	}
	if strings.Contains(result.stderr, "release over-limit claim") {
		t.Fatalf("stderr = %q, release failed", result.stderr)
	}
	if countTicketRequest(ticketLog, "release") != 1 {
		t.Fatalf("ticket log = %q, want one release", readText(ticketLog))
	}
	if got := readText(codexLog); got != "" {
		t.Fatalf("Codex log = %q, want empty", got)
	}
}

func TestRoleExecutableTerminalCleanup(t *testing.T) {
	binary, helperDir := buildFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
	ticketLog := filepath.Join(t.TempDir(), "ticket.log")
	result := runRole(t, binary, helperDir, "coder", stateDir, codexLog, ticketLog, map[string]string{
		"TICKET_ORC_FAKE_TICKET_STATE": "closed",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
		"TICKET_ORC_FAKE_CODEX_LOG":    codexLog,
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
	})
	if !strings.Contains(result.stderr, "wait for implementation work") {
		t.Fatalf("stderr = %q, want queue stop after cleanup", result.stderr)
	}
	calls := readCodexCalls(t, codexLog)
	if len(calls) != 2 || len(calls[1].Args) != 3 || calls[1].Args[0] != "delete" || calls[1].Args[1] != "fake-thread" || calls[1].Args[2] != "--force" {
		t.Fatalf("Codex cleanup calls = %#v", calls)
	}
}

func TestRoleExecutableHarnessFailureRereadsAndStops(t *testing.T) {
	binary, helperDir := buildFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
	ticketLog := filepath.Join(t.TempDir(), "ticket.log")
	result := runRole(t, binary, helperDir, "coder", stateDir, codexLog, ticketLog, map[string]string{
		"TICKET_ORC_FAKE_TICKET_STATE": "review",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_TICKET_LOG":   ticketLog,
		"TICKET_ORC_FAKE_CODEX_LOG":    codexLog,
		"TICKET_ORC_FAKE_CODEX_FAIL":   "1",
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
	})
	if !strings.Contains(result.stderr, "Codex process exited with status 7") && !strings.Contains(result.stderr, "exit status 7") {
		t.Fatalf("stderr = %q, want harness failure", result.stderr)
	}
	if countTicketRequest(ticketLog, "show") != 1 {
		t.Fatalf("ticket log = %q, want authoritative show", readText(ticketLog))
	}
	if len(readCodexCalls(t, codexLog)) != 1 {
		t.Fatalf("Codex launched more than once: %q", readText(codexLog))
	}
}

type roleResult struct {
	stdout string
	stderr string
}

func runRole(t *testing.T, binary, helperDir, role, stateDir, codexLog, ticketLog string, values map[string]string) roleResult {
	return runRoleArgs(t, binary, helperDir, role, stateDir, codexLog, ticketLog, values, nil)
}

func runRoleArgs(t *testing.T, binary, helperDir, role, stateDir, codexLog, ticketLog string, values map[string]string, extra []string) roleResult {
	t.Helper()
	actor := "integration-" + role
	home := t.TempDir()
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(stateDir, "integration-config.json")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":%q}`, stateDir)), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{role, "--actor", actor, "--config", configPath}
	args = append(args, extra...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout = new(bytes.Buffer)
	cmd.Stderr = new(bytes.Buffer)
	cmd.Env = testEnv(helperDir, values, map[string]string{
		"TICKET_ORC_ACTOR":           actor,
		"TICKET_ORC_FAKE_CODEX_LOG":  codexLog,
		"TICKET_ORC_FAKE_TICKET_LOG": ticketLog,
		"HOME":                       home,
		"USERPROFILE":                home,
	})
	err := cmd.Run()
	if err == nil {
		t.Fatal("role unexpectedly exited successfully after fake queue exhaustion")
	}
	return roleResult{stdout: cmd.Stdout.(*bytes.Buffer).String(), stderr: cmd.Stderr.(*bytes.Buffer).String()}
}

type codexCall struct {
	Actor  string   `json:"actor"`
	Args   []string `json:"args"`
	PID    int      `json:"pid"`
	Prompt string   `json:"prompt"`
}

func readCodexCalls(t *testing.T, path string) []codexCall {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Codex log: %v", err)
	}
	var calls []codexCall
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var call codexCall
		if err := json.Unmarshal(scanner.Bytes(), &call); err != nil {
			t.Fatalf("decode Codex log: %v", err)
		}
		calls = append(calls, call)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan Codex log: %v", err)
	}
	return calls
}

func readText(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}

func countTicketRequest(path, command string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.HasPrefix(line, command+" ") || line == command {
			count++
		}
	}
	return count
}

func buildFixture(t *testing.T) (string, string) {
	t.Helper()
	root := filepath.Clean(filepath.Join(filepath.Dir(mustCallerFile()), "../.."))
	dir := t.TempDir()
	name := "ticket-orc"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(dir, name)
	cacheRoot := filepath.Join(os.TempDir(), "codex-go")
	for _, path := range []string{filepath.Join(cacheRoot, "cache"), filepath.Join(cacheRoot, "path"), filepath.Join(cacheRoot, "mod")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("prepare shared Go cache %s: %v", path, err)
		}
	}
	buildEnv := testEnv("", nil, map[string]string{
		"GOTOOLCHAIN": "local",
		"GOPROXY":     "off",
		"GOCACHE":     filepath.Join(cacheRoot, "cache"),
		"GOPATH":      filepath.Join(cacheRoot, "path"),
		"GOMODCACHE":  filepath.Join(cacheRoot, "mod"),
	})
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/ticket-orc")
	cmd.Dir = root
	cmd.Env = buildEnv
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	helperDir := filepath.Join(dir, "helpers")
	if err := os.MkdirAll(helperDir, 0o700); err != nil {
		t.Fatalf("create helper directory: %v", err)
	}
	fakeName := "fake" + executableSuffix()
	fakeBinary := filepath.Join(dir, fakeName)
	fakeBuild := exec.Command("go", "build", "-o", fakeBinary, "./internal/integration/fakebin")
	fakeBuild.Dir = root
	fakeBuild.Env = buildEnv
	if output, err := fakeBuild.CombinedOutput(); err != nil {
		t.Fatalf("build fake executable: %v\n%s", err, output)
	}
	for _, name := range []string{"ticket", "codex", "pi", "claude"} {
		helper := filepath.Join(helperDir, name+executableSuffix())
		if err := copyFile(fakeBinary, helper); err != nil {
			t.Fatalf("copy %s helper: %v", name, err)
		}
	}
	return binary, helperDir
}

func mustCallerFile() string {
	_, file, _, _ := runtime.Caller(0)
	return file
}

func copyFile(source, target string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.WriteFile(target, data, 0o700); err != nil {
		return err
	}
	return os.Chmod(target, 0o700)
}

func executableSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func testEnv(helperDir string, values, overrides map[string]string) []string {
	env := append([]string(nil), os.Environ()...)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	if helperDir != "" {
		path := helperDir
		if current, ok := lookupEnv(env, "PATH"); ok {
			path += string(os.PathListSeparator) + current
		}
		overrides["PATH"] = path
	}
	for key, value := range values {
		overrides[key] = value
	}
	for key, value := range overrides {
		env = setEnv(env, key, value)
	}
	return env
}

func envWith(values map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(values)+1)
	for name, current := range values {
		result[name] = current
	}
	result[key] = value
	return result
}

func lookupEnv(env []string, key string) (string, bool) {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix), true
		}
	}
	return "", false
}

func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, fmt.Sprintf("%s=%s", key, value))
}
