package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestRoleConfigResolvesWorkingDirectoryPrecedence(t *testing.T) {
	root := t.TempDir()
	defaultsDir := filepath.Join(root, "defaults")
	roleDir := filepath.Join(root, "role")
	workerDir := filepath.Join(root, "worker")
	for _, directory := range []string{defaultsDir, roleDir, workerDir} {
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"defaults":{"working_dir":"defaults","repository":"repo-default"},"roles":{"coder":{"working_dir":"role","repository":"repo-role"}},"workers":{"worker-a":{"role":"coder","actor":"worker-a","working_dir":"worker","repository":"repo-worker"}}}`))
	config, help, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-a"}, emptyEnv)
	if help || err != nil {
		t.Fatalf("parseRoleConfig = %#v, %v", config, err)
	}
	if config.WorkingDir != workerDir {
		t.Fatalf("worker working directory = %q, want %q", config.WorkingDir, workerDir)
	}
	if config.Repository != filepath.Join(root, "repo-worker") {
		t.Fatalf("worker repository = %q, want %q", config.Repository, filepath.Join(root, "repo-worker"))
	}
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"defaults":{"working_dir":"defaults","repository":"repo-default"},"roles":{"coder":{"working_dir":"role","repository":"repo-role"}},"workers":{"worker-a":{"role":"coder","actor":"worker-a"}}}`))
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-a"}, emptyEnv)
	if err != nil || config.WorkingDir != roleDir {
		t.Fatalf("role working directory = %q, err=%v, want %q", config.WorkingDir, err, roleDir)
	}
	if config.Repository != filepath.Join(root, "repo-role") {
		t.Fatalf("role repository = %q, want %q", config.Repository, filepath.Join(root, "repo-role"))
	}
	writeConfigFixture(t, path, `{"version":1,"defaults":{"working_dir":"defaults","repository":"repo-default"},"workers":{"worker-a":{"role":"coder","actor":"worker-a"}}}`)
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-a"}, emptyEnv)
	if err != nil || config.WorkingDir != defaultsDir {
		t.Fatalf("default working directory = %q, err=%v, want %q", config.WorkingDir, err, defaultsDir)
	}
	if config.Repository != filepath.Join(root, "repo-default") {
		t.Fatalf("default repository = %q, want %q", config.Repository, filepath.Join(root, "repo-default"))
	}
}

func TestRoleConfigPreservesImplicitAndDirectTicketTargets(t *testing.T) {
	implicit, _, err := parseRoleConfig(RoleCoder, []string{"--actor", "implicit"}, testInstanceEnv(t))
	if err != nil {
		t.Fatalf("implicit config: %v", err)
	}
	if implicit.Ticket.Mode != TicketTargetImplicit || implicit.Ticket.Repository != "" {
		t.Fatalf("implicit Ticket target = %#v", implicit.Ticket)
	}

	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","repository":"tickets"}}}`)
	direct, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "one"}, emptyEnv)
	if err != nil {
		t.Fatalf("direct config: %v", err)
	}
	want := filepath.Join(root, "tickets")
	if direct.Ticket.Mode != TicketTargetRepository || direct.Ticket.Repository != want || direct.Repository != want {
		t.Fatalf("direct Ticket target = %#v, repository = %q, want %q", direct.Ticket, direct.Repository, want)
	}
}

func TestRoleActorInheritancePrecedence(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"roles":{"coder":{"actor":"role-coder"}},"workers":{"inherited":{"role":"coder"},"explicit":{"role":"coder","actor":"worker-coder"},"fallback":{"role":"reviewer"}}}`)
	cases := []struct {
		name string
		want string
	}{
		{name: "inherited", want: "role-coder"},
		{name: "explicit", want: "worker-coder"},
		{name: "fallback", want: "reviewer"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			role := RoleCoder
			if test.name == "fallback" {
				role = RoleReviewer
			}
			config, _, err := parseRoleConfig(role, []string{"--config", path, "--worker", test.name}, emptyEnv)
			if err != nil {
				t.Fatal(err)
			}
			if config.Actor != test.want {
				t.Fatalf("actor = %q, want %q", config.Actor, test.want)
			}
		})
	}
}

func workerConfigFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{
  "version": 1,
  "defaults": {"model":"default-model","reasoning":"low","session_policy":"fresh","session_cleanup":"keep","output":"quiet"},
  "roles": {"coder":{"model":"role-model","reasoning":"medium","max_bounces":3,"session_policy":"ticket"},"reviewer":{"session_policy":"fresh"}},
  "workers": {
    "worker-a": {"role":"coder","actor":"worker-actor","model":"worker-model","reasoning":"high","max_bounces":4},
    "worker-b": {"role":"reviewer","actor":"reviewer-actor"}
  },
  "supervisor": {"startup_groups": []}
}`)
	return path
}

func TestRoleConfigResolvesWorkerPrecedence(t *testing.T) {
	path := workerConfigFixture(t)
	env := mapEnv(map[string]string{
		"TICKET_ORC_WORKER":         "worker-b",
		"TICKET_ORC_ACTOR":          "environment-actor",
		"TICKET_ORC_MODEL":          "environment-model",
		"TICKET_ORC_MAX_BOUNCES":    "5",
		"TICKET_ORC_SESSION_POLICY": "fresh",
	})
	config, help, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-a", "--model", "cli-model"}, env)
	if help || err != nil {
		t.Fatalf("parseRoleConfig = %#v, %v", config, err)
	}
	if config.WorkerName != "worker-a" || config.Actor != "environment-actor" || config.Model != "cli-model" || config.Reasoning != "high" || config.MaxBounces != 5 || config.SessionPolicy != SessionPolicyFresh || config.SessionCleanup != CleanupKeep || config.Output != OutputQuiet {
		t.Fatalf("resolved config = %#v", config)
	}
	if want := filepath.Join(filepath.Dir(path), ".local", "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"); config.StateDir != want {
		t.Fatalf("state dir = %q, want %q", config.StateDir, want)
	}
}

func TestModelOverrideDoesNotChangeWorkerOrTicketActorIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"project-coder":{"role":"coder","actor":"ticket-coder","model":"model-one"}}}`)
	config, help, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "project-coder", "--model", "model-two"}, emptyEnv)
	if help || err != nil {
		t.Fatalf("parseRoleConfig = %#v help=%v err=%v", config, help, err)
	}
	if config.WorkerName != "project-coder" || config.Actor != "ticket-coder" || config.Model != "model-two" {
		t.Fatalf("model override changed worker identity: %#v", config)
	}
}

func TestRoleConfigPrecedenceAcrossFieldFamilies(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{
  "version":1,
  "defaults":{"model":"default-model","reasoning":"low","session_policy":"ticket","session_cleanup":"delete","minimum_reuse_context_percent":10,"output":"compact"},
  "roles":{"coder":{"actor":"role-actor","model":"role-model","reasoning":"medium","max_bounces":2,"session_policy":"fresh","session_cleanup":"archive","minimum_reuse_context_percent":20,"output":"quiet"}},
  "workers":{"worker":{"role":"coder","actor":"worker-actor","model":"worker-model","reasoning":"high","max_bounces":3,"session_policy":"ticket","session_cleanup":"keep","minimum_reuse_context_percent":30,"output":"json"}}
}`)
	tests := []struct {
		name        string
		envName     string
		flag        string
		envValue    string
		flagValue   string
		wantWorker  string
		wantEnv     string
		wantFlag    string
		configValue func(supervisor.RoleConfig) string
	}{
		{"actor", "TICKET_ORC_ACTOR", "--actor", "environment-actor", "flag-actor", "worker-actor", "environment-actor", "flag-actor", func(c supervisor.RoleConfig) string { return c.Actor }},
		{"model", "TICKET_ORC_MODEL", "--model", "environment-model", "flag-model", "worker-model", "environment-model", "flag-model", func(c supervisor.RoleConfig) string { return c.Model }},
		{"reasoning", "TICKET_ORC_REASONING", "--reasoning", "xhigh", "max", "high", "xhigh", "max", func(c supervisor.RoleConfig) string { return c.Reasoning }},
		{"max_bounces", "TICKET_ORC_MAX_BOUNCES", "--max-bounces", "4", "5", "3", "4", "5", func(c supervisor.RoleConfig) string { return strconv.Itoa(c.MaxBounces) }},
		{"session_policy", "TICKET_ORC_SESSION_POLICY", "--session-policy", "fresh", "ticket", "ticket", "fresh", "ticket", func(c supervisor.RoleConfig) string { return string(c.SessionPolicy) }},
		{"session_cleanup", "TICKET_ORC_SESSION_CLEANUP", "--session-cleanup", "archive", "delete", "keep", "archive", "delete", func(c supervisor.RoleConfig) string { return string(c.SessionCleanup) }},
		{"minimum_reuse_context_percent", "TICKET_ORC_MINIMUM_REUSE_CONTEXT_PERCENT", "--minimum-reuse-context-percent", "40", "50", "30", "40", "50", func(c supervisor.RoleConfig) string { return strconv.Itoa(c.MinimumReuseContextPercent) }},
		{"output", "TICKET_ORC_OUTPUT", "--output", "quiet", "compact", "json", "quiet", "compact", func(c supervisor.RoleConfig) string { return string(c.Output) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseArgs := []string{"--config", path, "--worker", "worker"}
			workerConfig, _, err := parseRoleConfig(RoleCoder, baseArgs, emptyEnv)
			if err != nil || test.configValue(workerConfig) != test.wantWorker {
				t.Fatalf("worker precedence = %q, %v; want %q", test.configValue(workerConfig), err, test.wantWorker)
			}
			env := mapEnv(map[string]string{test.envName: test.envValue})
			envConfig, _, err := parseRoleConfig(RoleCoder, baseArgs, env)
			if err != nil || test.configValue(envConfig) != test.wantEnv {
				t.Fatalf("environment precedence = %q, %v; want %q", test.configValue(envConfig), err, test.wantEnv)
			}
			flagArgs := append(append([]string(nil), baseArgs...), test.flag, test.flagValue)
			flagConfig, _, err := parseRoleConfig(RoleCoder, flagArgs, env)
			if err != nil || test.configValue(flagConfig) != test.wantFlag {
				t.Fatalf("CLI precedence = %q, %v; want %q", test.configValue(flagConfig), err, test.wantFlag)
			}
			assertRoleConfigComparable(flagConfig)
			encoded, err := json.Marshal(flagConfig)
			if err != nil {
				t.Fatalf("resolved config is not serializable: %v", err)
			}
			var decoded supervisor.RoleConfig
			if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, flagConfig) {
				t.Fatalf("resolved config JSON round trip = %#v, %v", decoded, err)
			}
		})
	}
	if _, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker", "--state-dir", "split-root"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "unknown flag --state-dir") {
		t.Fatalf("state-dir flag error = %v", err)
	}
}

func assertRoleConfigComparable[T comparable](_ T) {}

func TestRoleConfigResolutionDefersRuntimeHarnessPreflight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"worker-a":{"role":"coder","actor":"worker","harness":"pi","session_cleanup":"delete"}}}`)
	resolved, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-a"}, emptyEnv)
	if err != nil {
		t.Fatalf("pure role config resolution: %v", err)
	}
	if err := preflightRoleConfig(resolved); err == nil || !strings.Contains(err.Error(), "does not support session cleanup policy") {
		t.Fatalf("runtime harness preflight = %v, want unsupported cleanup error", err)
	}
}

func TestRoleConfigRejectsInvalidMinimumReuseContextPercent(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{name: "CLI out of range", args: []string{"--actor", "coder", "--minimum-reuse-context-percent", "101"}},
		{name: "environment malformed", args: []string{"--actor", "coder"}, env: map[string]string{"TICKET_ORC_MINIMUM_REUSE_CONTEXT_PERCENT": "unknown"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := parseRoleConfig(RoleCoder, test.args, testInstanceEnvWith(t, test.env)); err == nil || !strings.Contains(err.Error(), "minimum-reuse-context-percent") {
				t.Fatalf("invalid threshold error = %v", err)
			}
		})
	}
}

func TestRoleConfigSelectsSingleWorkerAndRejectsUnsafeSelection(t *testing.T) {
	path := workerConfigFixture(t)
	config, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--actor", "ad-hoc"}, emptyEnv)
	if err != nil || config.WorkerName != "worker-a" || config.Actor != "ad-hoc" {
		t.Fatalf("single-worker selection = %#v, %v", config, err)
	}
	if _, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker-b", "--actor", "x"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "cannot run coder") {
		t.Fatalf("role mismatch error = %v", err)
	}
	if _, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "missing", "--actor", "x"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unknown worker error = %v", err)
	}
}

func TestRoleConfigRejectsAmbiguousWorkers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder"},"two":{"role":"coder"}}}`)
	if _, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--actor", "x"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "multiple configured coder workers") {
		t.Fatalf("ambiguity error = %v", err)
	}
}

func TestRoleConfigAdHocModeWithoutWorkersRemainsAvailable(t *testing.T) {
	config, _, err := parseRoleConfig(RoleCoder, []string{"--actor", "ad-hoc"}, testInstanceEnv(t))
	if err != nil || config.WorkerName != "" || config.Actor != "ad-hoc" {
		t.Fatalf("ad-hoc config = %#v, %v", config, err)
	}
}

func TestPiRoleUsesPiCleanupDefaultAndProvider(t *testing.T) {
	config, _, err := parseRoleConfig(RoleCoder, []string{"--actor", "worker-a", "--harness", "pi", "--pi-provider", "openai"}, testInstanceEnv(t))
	if err != nil {
		t.Fatalf("Pi config: %v", err)
	}
	if config.Harness != "pi" || config.Pi.Provider != "openai" || config.SessionCleanup != CleanupKeep {
		t.Fatalf("Pi config = %#v", config)
	}
}

func TestClaudeRoleUsesClaudeCleanupDefaultAndPermissionMode(t *testing.T) {
	config, _, err := parseRoleConfig(RoleReviewer, []string{"--actor", "reviewer-agent", "--harness", "claude", "--claude-permission-mode", "acceptEdits"}, testInstanceEnv(t))
	if err != nil {
		t.Fatalf("Claude config: %v", err)
	}
	if config.Harness != "claude" || config.Claude.PermissionMode != "acceptEdits" || config.SessionCleanup != CleanupKeep {
		t.Fatalf("Claude config = %#v", config)
	}
}

func TestExplicitEmptySessionCleanupFailsBeforeHarnessStart(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"CLI", []string{"--harness", "pi", "--session-cleanup", ""}, nil},
		{"environment", []string{"--harness", "pi"}, map[string]string{"TICKET_ORC_SESSION_CLEANUP": ""}},
		{"Codex CLI", []string{"--harness", "codex", "--session-cleanup", ""}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"--actor", "worker"}, test.args...)
			_, _, err := parseRoleConfig(RoleCoder, args, testInstanceEnvWith(t, test.env))
			if err == nil || !strings.Contains(err.Error(), "session-cleanup must") {
				t.Fatalf("error = %v, want session-cleanup validation", err)
			}
		})
	}
}

func TestCodexSandboxResolutionPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{
  "version":1,
  "defaults":{"codex":{"sandbox":"read-only"}},
  "roles":{"coder":{"codex":{"sandbox":"workspace-write"}}},
  "workers":{"coder":{"role":"coder","actor":"coder","codex":{"sandbox":"danger-full-access"}}}
}`)
	env := mapEnv(map[string]string{"TICKET_ORC_CODEX_SANDBOX": "read-only"})
	config, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder", "--codex-sandbox", "workspace-write"}, env)
	if err != nil || config.Codex.Sandbox != "workspace-write" {
		t.Fatalf("CLI sandbox = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, env)
	if err != nil || config.Codex.Sandbox != "read-only" {
		t.Fatalf("environment sandbox = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
	if err != nil || config.Codex.Sandbox != "danger-full-access" {
		t.Fatalf("worker sandbox = %#v, %v", config, err)
	}
}

func TestIrrelevantNamespacedOptionsRejectExplicitEmptyValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"flag for pi", []string{"--harness", "pi", "--codex-sandbox", ""}, nil, "selected pi harness"},
		{"environment for pi", []string{"--harness", "pi"}, map[string]string{"TICKET_ORC_CODEX_SANDBOX": ""}, "selected pi harness"},
		{"flag for codex", []string{"--harness", "codex", "--pi-provider", ""}, nil, "selected codex harness"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"--actor", "worker"}, test.args...)
			_, _, err := parseRoleConfig(RoleCoder, args, testInstanceEnvWith(t, test.env))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestReviewerCompletionResolutionPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{
  "version": 1,
  "defaults": {"review_completion":"signoff"},
  "roles": {"reviewer":{"review_completion":"close"}},
  "workers": {"reviewer":{"role":"reviewer","actor":"reviewer","review_completion":"signoff"}}
}`)
	env := mapEnv(map[string]string{"TICKET_ORC_REVIEW_COMPLETION": "close"})
	config, _, err := parseRoleConfig(RoleReviewer, []string{"--config", path, "--worker", "reviewer", "--review-completion", "signoff"}, env)
	if err != nil || config.ReviewCompletion != ReviewCompletionSignoff {
		t.Fatalf("CLI completion = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleReviewer, []string{"--config", path, "--worker", "reviewer"}, env)
	if err != nil || config.ReviewCompletion != ReviewCompletionClose {
		t.Fatalf("environment completion = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleReviewer, []string{"--config", path, "--worker", "reviewer"}, emptyEnv)
	if err != nil || config.ReviewCompletion != ReviewCompletionSignoff {
		t.Fatalf("worker completion = %#v, %v", config, err)
	}
}

func TestCoderReviewSkipTagResolutionAndFinalGate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{
  "version": 1,
  "review": {"skip_tags":["trivial","no-review"]},
  "roles": {"reviewer":{"review_completion":"signoff"}},
  "workers": {"coder":{"role":"coder","actor":"coder"}}
}`)
	config, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
	if err != nil {
		t.Fatalf("parseRoleConfig: %v", err)
	}
	if config.ReviewSkipTags != "trivial\x1fno-review" {
		t.Fatalf("review skip tags = %q", config.ReviewSkipTags)
	}
	if config.ReviewFinalGate != ReviewCompletionSignoff {
		t.Fatalf("review final gate = %q, want signoff", config.ReviewFinalGate)
	}
}

func TestTicketPromptResolutionPrecedenceAndRoleTemplates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{
  "version":1,
  "defaults":{"ticket_prompt":"default {{ticket}}"},
  "roles":{"coder":{"ticket_prompt":"role {{ticket}}"},"reviewer":{"ticket_prompt":"review {{ticket}} {{review_completion}}"}},
  "workers":{"coder":{"role":"coder","actor":"coder","ticket_prompt":"worker {{ticket}}"},"reviewer":{"role":"reviewer","actor":"reviewer"}}
}`)
	env := mapEnv(map[string]string{"TICKET_ORC_TICKET_PROMPT": "env {{ticket}}"})
	config, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder", "--ticket-prompt", "cli {{ticket}}"}, env)
	if err != nil || config.TicketPrompt != "cli {{ticket}}" {
		t.Fatalf("CLI ticket prompt = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, env)
	if err != nil || config.TicketPrompt != "env {{ticket}}" {
		t.Fatalf("environment ticket prompt = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
	if err != nil || config.TicketPrompt != "worker {{ticket}}" {
		t.Fatalf("worker ticket prompt = %#v, %v", config, err)
	}
	config, _, err = parseRoleConfig(RoleReviewer, []string{"--config", path, "--worker", "reviewer"}, emptyEnv)
	if err != nil || config.TicketPrompt != "review {{ticket}} {{review_completion}}" {
		t.Fatalf("reviewer role ticket prompt = %#v, %v", config, err)
	}
}

func TestRoleConfigRejectsInvalidTicketPromptBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want string
	}{
		{"blank", " \t", "whitespace-only"},
		{"unknown", "work {{ticket}} {{unknown}}", "unknown placeholder"},
		{"missing ticket", "work without an ID", "include {{ticket}}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"coder","ticket_prompt":`+quoteJSON(test.text)+`}}}`)
			_, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func quoteJSON(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestRoleConfigRejectsIrrelevantHarnessNamespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"coder","pi":{"provider":"openai"}}}}`)
	if _, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv); err == nil || !strings.Contains(err.Error(), "cannot use pi") {
		t.Fatalf("namespace error = %v", err)
	}
}

func TestCoderRejectsReviewerOnlyCompletionSettings(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		env  envLookup
		body string
		want string
	}{
		{"cli", []string{"--review-completion", "close"}, emptyEnv, `{"version":1}`, "only valid for reviewer"},
		{"environment", nil, mapEnv(map[string]string{"TICKET_ORC_REVIEW_COMPLETION": "close"}), `{"version":1}`, "only valid for reviewer"},
		{"worker", []string{"--worker", "coder"}, emptyEnv, `{"version":1,"workers":{"coder":{"role":"coder","actor":"coder","review_completion":"close"}}}`, "cannot set review-completion"},
		{"role", []string{"--actor", "coder"}, emptyEnv, `{"version":1,"roles":{"coder":{"review_completion":"close"}}}`, "roles.coder.review_completion is only valid for review roles"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			body := strings.ReplaceAll(test.body, "CONFIG", path)
			writeConfigFixture(t, path, body)
			args := append([]string(nil), test.args...)
			if test.name == "cli" {
				args = append(args, "--actor", "coder")
			} else if test.name == "environment" {
				args = []string{"--actor", "coder"}
			}
			args = append([]string{"--config", path}, args...)
			if _, _, err := parseRoleConfig(RoleCoder, args, test.env); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
