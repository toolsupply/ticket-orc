package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestRunHelp(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"-h"}, {"--help"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
				t.Fatalf("run() code = %d, want 0; stderr = %q", code, stderr.String())
			}
			if got := stdout.String(); !strings.HasPrefix(got, "\nA small agent orchestrator from https://github.com/toolsupply/ticket-orc") || !strings.HasSuffix(got, "\n\n") || !strings.Contains(got, "Usage:\n  ticket-orc <command> [options]") || strings.Contains(got, "Commands:\n") || strings.Contains(got, "Options:\n") || strings.Contains(got, "  ticket-orc coder [options]") || strings.Contains(got, "Run one coder or reviewer role per process") {
				t.Fatalf("normalized help output = %q", got)
			}
			if !strings.Contains(stdout.String(), "Getting started:\n  init") || !strings.Contains(stdout.String(), "Run 'ticket-orc help options'") || strings.Contains(stdout.String(), "    init") {
				t.Fatalf("main help layout or options pointer is incorrect: %q", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestMainHelpUsesInteractiveCommandGroups(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help"}, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
		t.Fatalf("help: code=%d stderr=%q", code, stderr.String())
	}
	text := stdout.String()
	worker := strings.Index(text, "Worker management:")
	queue := strings.Index(text, "Queue control:")
	daemon := strings.Index(text, "Daemon control:")
	if worker < 0 || queue <= worker || daemon <= queue {
		t.Fatalf("main help does not use interactive command groups: %q", text)
	}
	if !strings.Contains(text[worker:queue], "status") || !strings.Contains(text[worker:queue], "doctor") || !strings.Contains(text[queue:daemon], "queue") || !strings.Contains(text[daemon:], "endpoint") || !strings.Contains(text[daemon:], "pause") || !strings.Contains(text[daemon:], "resume") || !strings.Contains(text[daemon:], "shutdown") {
		t.Fatalf("commands are missing from their help groups: %q", text)
	}
}

func TestHelpTopicsCoverShellCommands(t *testing.T) {
	for _, test := range []struct {
		command string
		want    string
	}{
		{"endpoint", "complete URL for the local daemon"},
		{"version", "build commit"},
		{"help", "ticket-orc help config check"},
	} {
		t.Run(test.command, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{"help", test.command}, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
				t.Fatalf("help %s: code=%d stderr=%q", test.command, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), test.want) {
				t.Fatalf("help %s omitted %q: %q", test.command, test.want, stdout.String())
			}
		})
	}
}

func TestMainPageCommandsHaveHelpPaths(t *testing.T) {
	tests := []struct {
		command string
		path    []string
		usage   string
	}{
		{"init", []string{"help", "init"}, "ticket-orc init"},
		{"join", []string{"help", "join"}, "ticket-orc join"},
		{"leave", []string{"help", "leave"}, "ticket-orc leave"},
		{"whoami", []string{"help", "whoami"}, "ticket-orc whoami"},
		{"state", []string{"help", "state"}, "ticket-orc state"},
		{"next", []string{"help", "next"}, "ticket-orc next"},
		{"coder", []string{"help", "coder"}, "ticket-orc coder"},
		{"reviewer", []string{"help", "reviewer"}, "ticket-orc reviewer"},
		{"run", []string{"help", "run"}, "ticket-orc run"},
		{"doctor", []string{"help", "doctor"}, "ticket-orc doctor"},
		{"status", []string{"help", "status"}, "ticket-orc status"},
		{"worker", []string{"help", "worker"}, "ticket-orc worker"},
		{"group", []string{"help", "group"}, "ticket-orc group"},
		{"pause", []string{"help", "pause"}, "ticket-orc pause"},
		{"resume", []string{"help", "resume"}, "ticket-orc resume"},
		{"abort", []string{"help", "abort"}, "ticket-orc abort"},
		{"queue", []string{"help", "queue"}, "ticket-orc queue"},
		{"endpoint", []string{"help", "endpoint"}, "ticket-orc endpoint"},
		{"attach", []string{"help", "attach"}, "ticket-orc attach"},
		{"reload", []string{"help", "reload"}, "ticket-orc reload"},
		{"shutdown", []string{"help", "shutdown"}, "ticket-orc shutdown"},
		{"gc", []string{"help", "gc"}, "ticket-orc gc"},
		{"report review", []string{"help", "report", "review"}, "ticket-orc report review"},
		{"config", []string{"help", "config", "check"}, "ticket-orc config check"},
		{"help", []string{"help", "help"}, "ticket-orc help"},
		{"version", []string{"help", "version"}, "ticket-orc version"},
	}
	paths := make(map[string]struct {
		path  []string
		usage string
	}, len(tests))
	for _, test := range tests {
		paths[test.command] = struct {
			path  []string
			usage string
		}{test.path, test.usage}
	}
	for _, line := range strings.Split(topLevelHelp, "\n") {
		if !strings.Contains(line, "\t") {
			continue
		}
		command := strings.Join(strings.Fields(strings.SplitN(line, "\t", 2)[0]), " ")
		if command == "" {
			continue
		}
		if _, ok := paths[command]; !ok {
			t.Errorf("main help command %q has no tested help path", command)
		}
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(test.path, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 {
				t.Fatalf("help path %v: code=%d stderr=%q", test.path, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), test.usage) {
				t.Fatalf("help path %v omitted %q: %q", test.path, test.usage, stdout.String())
			}
		})
	}
}

func TestNestedCommandHelpFormsDescribeCanonicalSyntax(t *testing.T) {
	for _, test := range []struct {
		name  string
		forms [][]string
		want  string
	}{
		{name: "config check", forms: [][]string{{"config", "check", "--help"}, {"help", "config", "check"}}, want: "ticket-orc config check [options]"},
		{name: "report review", forms: [][]string{{"report", "--help"}, {"report", "review", "--help"}, {"help", "report", "review"}}, want: "ticket-orc report review [options]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var first string
			for i, args := range test.forms {
				var stdout, stderr bytes.Buffer
				if code := run(args, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 {
					t.Fatalf("%v: code=%d stderr=%q", args, code, stderr.String())
				}
				if !strings.Contains(stdout.String(), test.want) {
					t.Fatalf("%v omitted canonical syntax %q: %q", args, test.want, stdout.String())
				}
				if i == 0 {
					first = stdout.String()
				} else if stdout.String() != first {
					t.Fatalf("%v help differs from first form:\n%s\n---\n%s", args, first, stdout.String())
				}
			}
		})
	}
}

func TestConfigCheckHelpDescribesRoleTagRouting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help", "config", "check"}, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 || stderr.Len() != 0 {
		t.Fatalf("config check help: code=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{"roles.<name>.ticket_tags", "every listed tag is", "review.skip_tags", "committed reload", "running managed workers"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("config check help omitted %q: %q", want, stdout.String())
		}
	}
}

func TestOptionsHelpPageDocumentsGlobalFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help", "options"}, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
		t.Fatalf("help options: code=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{"Usage:\n  ticket-orc help options", "-h, --help", "-v, --version"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("options help omitted %q: %q", want, stdout.String())
		}
	}
}

func TestRoleErrorSanitizesHarnessDiagnostics(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWithExecutors([]string{"coder", "--actor", "worker"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		role: func(supervisor.RoleConfig) error { return errors.New("harness\x1b[31m failed\x1b]52;c;secret\a\x00") },
	})
	if code != 1 {
		t.Fatalf("role error code=%d stderr=%q", code, stderr.String())
	}
	for _, r := range stderr.String() {
		if r != '\n' && (r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)) {
			t.Fatalf("unsafe role error=%q", stderr.String())
		}
	}
	if !strings.Contains(stderr.String(), "harness[31m failed]52;c;secret") {
		t.Fatalf("sanitized diagnostic lost text: %q", stderr.String())
	}
}

func TestConfigShortAliasIsAcceptedAcrossCommands(t *testing.T) {
	if values, help, err := parseRoleFlags([]string{"-c", "role.json", "--help"}); err != nil || !help || values["config"] != "role.json" {
		t.Fatalf("role -c = %#v help=%v err=%v", values, help, err)
	}
	if config, help, err := parseRunConfig([]string{"-c", "run.json", "--help"}, emptyEnv); err != nil || !help || config.ConfigPath != "" {
		t.Fatalf("run -c = %#v help=%v err=%v", config, help, err)
	}
	if config, help, err := parseConfigCheckConfig([]string{"-c", "check.json", "--help"}, emptyEnv); err != nil || !help || config.ConfigPath != "" {
		t.Fatalf("config check -c = %#v help=%v err=%v", config, help, err)
	}
	if config, help, err := parseStateConfig([]string{"-c", "state.json", "--help"}, emptyEnv); err != nil || !help || config.StateDir != "" {
		t.Fatalf("state -c = %#v help=%v err=%v", config, help, err)
	}
	if config, help, err := parseGCConfig([]string{"-c", "gc.json", "--help"}, emptyEnv); err != nil || !help || config.StateDir != "" {
		t.Fatalf("gc -c = %#v help=%v err=%v", config, help, err)
	}
	if _, _, err := parseRoleFlags([]string{"-c", "one.json", "--config", "two.json"}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatal("short and long config flags should conflict")
	}
}

func TestRunRoleHelpDoesNotRequireActor(t *testing.T) {
	for _, args := range [][]string{{"coder", "--help"}, {"reviewer", "-h"}, {"help", "coder"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
				t.Fatalf("run() code = %d, want 0; stderr = %q", code, stderr.String())
			}
			for _, want := range []string{"--actor NAME", "TICKET_ORC_ACTOR", "Command options override environment"} {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("help output missing %q: %q", want, stdout.String())
				}
			}
		})
	}
}

func TestCommandHelpUsesUsageFirstAndBlankBoundaries(t *testing.T) {
	commands := [][]string{
		{"help", "help"},
		{"help", "options"},
		{"help", "endpoint"},
		{"help", "version"},
		{"coder", "--help"},
		{"reviewer", "--help"},
		{"state", "--help"},
		{"gc", "--help"},
		{"run", "--help"},
		{"config", "check", "--help"},
	}
	for _, args := range commands {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr.String())
		}
		got := stdout.String()
		if !strings.HasPrefix(got, "\nUsage:\n") || !strings.HasSuffix(got, "\n\n") {
			t.Fatalf("%v: help boundaries = %q", args, got)
		}
		if strings.Index(got, "Usage:\n") != 1 {
			t.Fatalf("%v: Usage is not the first block: %q", args, got)
		}
	}
}

func TestCommandHelpTopicsEndWithBlankLine(t *testing.T) {
	commands := [][]string{
		{"help", "init"}, {"help", "join"}, {"help", "leave"}, {"help", "whoami"}, {"help", "next"},
		{"help", "help"}, {"help", "options"}, {"help", "endpoint"}, {"help", "version"},
		{"help", "queue"}, {"help", "coder"}, {"help", "reviewer"}, {"help", "state"}, {"help", "gc"},
		{"help", "run"}, {"help", "doctor"}, {"help", "report", "review"}, {"help", "attach"}, {"help", "status"},
		{"help", "worker"}, {"help", "group"}, {"help", "pause"}, {"help", "resume"}, {"help", "reload"}, {"help", "shutdown"}, {"help", "config", "check"},
		{"endpoint", "--help"},
	}
	for _, args := range commands {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
				t.Fatalf("%v: code=%d stderr=%q", args, code, stderr.String())
			}
			if !strings.HasSuffix(stdout.String(), "\n\n") {
				t.Fatalf("%v: help lacks a blank line at the end: %q", args, stdout.String())
			}
		})
	}
}

func TestRunVersion(t *testing.T) {
	oldVersion, oldCommit := Version, Commit
	t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })
	tests := []struct {
		name    string
		version string
		commit  string
		want    string
	}{
		{name: "development", version: "dev", want: "ticket-orc dev\n"},
		{name: "CI build", version: "1.2.3", commit: strings.Repeat("a", 40), want: "ticket-orc 1.2.3 (" + strings.Repeat("a", 40) + ")\n"},
		{name: "release from source archive", version: "1.2.3", commit: "source-archive", want: "ticket-orc 1.2.3 (source-archive)\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Version, Commit = test.version, test.commit
			var stdout, stderr bytes.Buffer
			if code := run([]string{"version"}, &stdout, &stderr, emptyEnv, rejectExecution); code != 0 {
				t.Fatalf("run() code = %d, want 0", code)
			}
			if got := stdout.String(); got != test.want {
				t.Fatalf("stdout = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRoleConfigDefaults(t *testing.T) {
	env := mapEnv(map[string]string{"TICKET_ORC_ACTOR": "coder-1"})
	config := captureConfig(t, []string{"coder"}, env)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	want := supervisor.RoleConfig{
		Role:                       RoleCoder,
		Harness:                    "codex",
		Actor:                      "coder-1",
		MaxBounces:                 6,
		SessionPolicy:              SessionPolicyTicket,
		SessionCleanup:             CleanupDelete,
		MinimumReuseContextPercent: defaultMinimumReuseContextPercent,
		StateDir:                   filepath.Join(home, defaultInstanceDirectoryName, ".local"),
		InstanceID:                 "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
		Output:                     OutputCompact,
		TicketPrompt:               orc.CoderTicketPromptTemplate,
	}
	if config != want {
		t.Fatalf("config = %#v, want %#v", config, want)
	}
}

func TestRoleConfigEnvironment(t *testing.T) {
	env := mapEnv(map[string]string{
		"TICKET_ORC_HARNESS":         "codex",
		"TICKET_ORC_ACTOR":           "reviewer-1",
		"TICKET_ORC_MODEL":           "gpt-test",
		"TICKET_ORC_REASONING":       "high",
		"TICKET_ORC_MAX_BOUNCES":     "9",
		"TICKET_ORC_SESSION_POLICY":  "fresh",
		"TICKET_ORC_SESSION_CLEANUP": "archive",
		"TICKET_ORC_OUTPUT":          "quiet",
	})
	config := captureConfig(t, []string{"reviewer"}, env)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if config.Role != RoleReviewer || config.Actor != "reviewer-1" || config.Model != "gpt-test" ||
		config.Reasoning != "high" || config.MaxBounces != 9 || config.SessionPolicy != SessionPolicyFresh ||
		config.SessionCleanup != CleanupArchive || config.StateDir != filepath.Join(home, defaultInstanceDirectoryName, ".local") || config.Output != OutputQuiet {
		t.Fatalf("unexpected config: %#v", config)
	}
}

func TestRoleConfigCLIOverridesEnvironment(t *testing.T) {
	env := mapEnv(map[string]string{
		"TICKET_ORC_ACTOR":          "env-actor",
		"TICKET_ORC_MAX_BOUNCES":    "invalid",
		"TICKET_ORC_SESSION_POLICY": "invalid",
		"TICKET_ORC_OUTPUT":         "invalid",
	})
	config := captureConfig(t, []string{
		"coder",
		"--actor=flag-actor",
		"--model", "flag-model",
		"--reasoning", "xhigh",
		"--max-bounces", "3",
		"--session-policy", "ticket",
		"--session-cleanup", "keep",
		"--output", "json",
	}, env)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if config.Actor != "flag-actor" || config.Model != "flag-model" || config.MaxBounces != 3 ||
		config.SessionPolicy != SessionPolicyTicket || config.SessionCleanup != CleanupKeep ||
		config.StateDir != filepath.Join(home, defaultInstanceDirectoryName, ".local") || config.Output != OutputJSON || config.Reasoning != "xhigh" {
		t.Fatalf("unexpected config: %#v", config)
	}
}

func TestRoleConfigDefaultsActorToRole(t *testing.T) {
	config, _, err := parseRoleConfig(RoleCoder, nil, testInstanceEnv(t))
	if err != nil {
		t.Fatalf("parseRoleConfig = %v", err)
	}
	if config.Actor != string(RoleCoder) {
		t.Fatalf("actor = %q, want %q", config.Actor, RoleCoder)
	}
}

func TestRoleConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"harness", []string{"--harness", "unknown"}, "unsupported harness"},
		{"actor whitespace", []string{"--actor", "two actors"}, "actor must not contain whitespace"},
		{"reasoning", []string{"--reasoning", "extreme"}, "Codex reasoning value"},
		{"sandbox", []string{"--codex-sandbox", "unsafe"}, "Codex sandbox mode"},
		{"max bounces text", []string{"--max-bounces", "many"}, "positive integer"},
		{"max bounces zero", []string{"--max-bounces", "0"}, "positive integer"},
		{"session policy", []string{"--session-policy", "forever"}, "session-policy must be"},
		{"cleanup", []string{"--session-cleanup", "purge"}, "session-cleanup must be"},
		{"output", []string{"--output", "verbose"}, "output must be"},
		{"unknown flag", []string{"--unknown", "value"}, "unknown flag"},
		{"duplicate flag", []string{"--actor", "one", "--actor", "two"}, "duplicate flag"},
		{"missing value", []string{"--actor"}, "requires a value"},
		{"positional", []string{"extra"}, "unexpected argument"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"coder", "--actor", "coder-1"}, tt.args...)
			if tt.name == "actor whitespace" || tt.name == "duplicate flag" || tt.name == "missing value" || tt.name == "positional" {
				args = append([]string{"coder"}, tt.args...)
			}
			assertUsageError(t, args, testInstanceEnv(t), tt.want)
		})
	}
}

func TestValidRoleInvokesExecutor(t *testing.T) {
	wantErr := errors.New("worker failed")
	var got supervisor.RoleConfig
	var stdout, stderr bytes.Buffer
	code := run([]string{"reviewer", "--actor", "reviewer-1"}, &stdout, &stderr, testInstanceEnv(t), func(config supervisor.RoleConfig) error {
		got = config
		return wantErr
	})
	if code != 1 || got.Role != RoleReviewer || got.Actor != "reviewer-1" {
		t.Fatalf("code = %d, config = %#v", code, got)
	}
	if !strings.Contains(stderr.String(), wantErr.Error()) {
		t.Fatalf("stderr = %q, want executor error", stderr.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	assertUsageError(t, []string{"unknown"}, emptyEnv, "unknown command: unknown")
}

func TestStateCommandRendersRetainedState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	configPath := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q}`, dir))
	store := state.NewForRepository(dir, filepath.Join(dir, "repo"))
	ctx := t.Context()
	for _, session := range []state.Session{
		{Ticket: "ticket-b", Role: "reviewer", Harness: "codex", ID: "review-session"},
		{Ticket: "ticket-a", Role: "coder", Harness: "codex", ID: "code-session"},
	} {
		if err := store.SetSession(ctx, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
	}
	if _, err := store.IncrementBounces(ctx, "ticket-a"); err != nil {
		t.Fatalf("IncrementBounces: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := runWithExecutors(
		[]string{"state", "--config", configPath},
		&stdout,
		&stderr,
		emptyEnv,
		commandExecutors{role: rejectExecution, state: executeState, gc: rejectGCExecution},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("state code = %d, stderr = %q", code, stderr.String())
	}
	output := stdout.String()
	for _, want := range []string{"Sessions:", "ticket-a", "coder", "code-session", "ticket-b", "reviewer", "review-session", "Review bounces:", "repository=" + filepath.Join(dir, "repo") + "  ticket=ticket-a  bounces=1"} {
		if !strings.Contains(output, want) {
			t.Errorf("state output %q does not contain %q", output, want)
		}
	}
	if strings.Index(output, "ticket-a") > strings.Index(output, "ticket-b") {
		t.Fatalf("state sessions are not sorted: %q", output)
	}
}

func TestGCCommandResolvesConfiguration(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"workers":{"worker":{"role":"coder","actor":"env-actor"}}}`)
	env := mapEnv(map[string]string{
		"TICKET_ORC_SESSION_CLEANUP": "archive",
	})
	var captured GCConfig
	var stdout, stderr bytes.Buffer
	code := runWithExecutors(
		[]string{"gc", "--config", configPath, "--worker", "worker", "--actor", "env-actor", "--session-cleanup=keep"},
		&stdout,
		&stderr,
		env,
		commandExecutors{
			role:  rejectExecution,
			state: rejectStateExecution,
			gc: func(config GCConfig, stdout, stderr io.Writer) error {
				captured = config
				_, _ = io.WriteString(stdout, "gc complete\n")
				return nil
			},
		},
	)
	if code != 0 || stderr.Len() != 0 || stdout.String() != "gc complete\n" {
		t.Fatalf("gc code = %d, stdout = %q, stderr = %q", code, stdout.String(), stderr.String())
	}
	if captured.Worker != "worker" || captured.Actor != "env-actor" || captured.SessionCleanup != CleanupKeep || !captured.CleanupExplicit || captured.StateDir != filepath.Join(root, ".local") {
		t.Fatalf("GC config = %#v", captured)
	}
}

func TestMaintenanceUsesConfigDefaultsAndHigherLayerOverrides(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"defaults":{"session_cleanup":"archive"},"workers":{"worker":{"role":"coder","actor":"gc-actor"}}}`)

	var stateConfig StateConfig
	var stdout, stderr bytes.Buffer
	code := runWithExecutors(
		[]string{"state", "--config", configPath},
		&stdout,
		&stderr,
		emptyEnv,
		commandExecutors{
			role: rejectExecution,
			state: func(config StateConfig, _ io.Writer) error {
				stateConfig = config
				return nil
			},
			gc: rejectGCExecution,
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("state code = %d, stderr = %q", code, stderr.String())
	}
	if want := filepath.Join(root, ".local"); stateConfig.StateDir != want {
		t.Fatalf("state dir = %q, want %q", stateConfig.StateDir, want)
	}

	var gcConfig GCConfig
	stdout.Reset()
	stderr.Reset()
	env := mapEnv(map[string]string{
		"TICKET_ORC_SESSION_CLEANUP": "keep",
	})
	code = runWithExecutors(
		[]string{"gc", "--config", configPath, "--worker", "worker", "--actor", "gc-actor"},
		&stdout,
		&stderr,
		env,
		commandExecutors{
			role:  rejectExecution,
			state: rejectStateExecution,
			gc: func(config GCConfig, _, _ io.Writer) error {
				gcConfig = config
				return nil
			},
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("gc code = %d, stderr = %q", code, stderr.String())
	}
	if gcConfig.Worker != "worker" || gcConfig.Actor != "gc-actor" || gcConfig.SessionCleanup != CleanupKeep || !gcConfig.CleanupExplicit || gcConfig.StateDir != filepath.Join(root, ".local") {
		t.Fatalf("GC config = %#v", gcConfig)
	}

	stdout.Reset()
	stderr.Reset()
	var flagGC GCConfig
	code = runWithExecutors(
		[]string{"gc", "--config", configPath, "--worker", "worker", "--actor", "gc-actor", "--session-cleanup", "delete"},
		&stdout,
		&stderr,
		env,
		commandExecutors{
			role:  rejectExecution,
			state: rejectStateExecution,
			gc: func(config GCConfig, _, _ io.Writer) error {
				flagGC = config
				return nil
			},
		},
	)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("flag gc code = %d, stderr = %q", code, stderr.String())
	}
	if flagGC.Worker != "worker" || flagGC.Actor != "gc-actor" || flagGC.SessionCleanup != CleanupDelete || !flagGC.CleanupExplicit || flagGC.StateDir != filepath.Join(root, ".local") {
		t.Fatalf("flag GC config = %#v", flagGC)
	}
}

func TestMaintenanceCommandValidationAndHelp(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"gc"}, "worker is required"},
		{[]string{"gc", "--worker", "worker", "--session-cleanup", "purge"}, "worker \"worker\" is not configured"},
		{[]string{"gc", "--actor", "worker", "--unknown", "value"}, "unknown flag"},
		{[]string{"state", "--state-dir", " "}, "unknown flag"},
		{[]string{"state", "extra"}, "unexpected argument"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runWithExecutors(tt.args, &stdout, &stderr, testInstanceEnv(t), rejectingExecutors())
			if code != 2 || !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("code = %d, stderr = %q, want %q", code, stderr.String(), tt.want)
			}
		})
	}
	for _, args := range [][]string{{"state", "--help"}, {"gc", "-h"}, {"help", "state"}, {"help", "gc"}} {
		var stdout, stderr bytes.Buffer
		if code := runWithExecutors(args, &stdout, &stderr, testInstanceEnv(t), rejectingExecutors()); code != 0 || stderr.Len() != 0 {
			t.Fatalf("%v: code = %d, stderr = %q", args, code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "ticket-orc "+args[len(args)-1]) && args[0] == "help" {
			t.Fatalf("%v: help output = %q", args, stdout.String())
		}
	}
}

func TestRoleCommandDoesNotDependOnAmbientGlobalInstance(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Chdir(cwd)
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	var stdout, stderr bytes.Buffer
	called := false
	code := run([]string{"coder", "--actor", "worker"}, &stdout, &stderr, emptyEnv, func(supervisor.RoleConfig) error {
		called = true
		return nil
	})
	if code != 2 || called || !strings.Contains(stderr.String(), "no Orc instance found") {
		t.Fatalf("role command without an instance: code=%d called=%t stderr=%q", code, called, stderr.String())
	}
}

func TestRunCommandHelpAndIdleConfiguration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runWithExecutors([]string{"run", "--help"}, &stdout, &stderr, emptyEnv, rejectingExecutors()); code != 0 || stderr.Len() != 0 {
		t.Fatalf("run help code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "--group NAME") || !strings.Contains(stdout.String(), "never means --all") {
		t.Fatalf("run help = %q", stdout.String())
	}

	var captured RunConfig
	stdout.Reset()
	stderr.Reset()
	code := runWithExecutors([]string{"run"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
		role:  rejectExecution,
		state: rejectStateExecution,
		gc:    rejectGCExecution,
		run: func(config RunConfig, _, _ io.Writer) error {
			captured = config
			return nil
		},
	})
	if code != 0 || stderr.Len() != 0 || len(captured.Workers) != 0 {
		t.Fatalf("idle run code = %d, config = %#v, stderr = %q", code, captured, stderr.String())
	}
}

func TestRunCommandRendersOnlyMissingFailureDiagnostic(t *testing.T) {
	tests := []struct {
		name string
		run  func(io.Writer) error
		want string
	}{
		{
			name: "unrendered cause is sanitized and printed",
			run: func(io.Writer) error {
				return errors.New("endpoint capability failed\n\x1b[31msecret\x1b[0m")
			},
			want: "endpoint capability failed",
		},
		{
			name: "complete diagnostic is not repeated",
			run: func(stderr io.Writer) error {
				fmt.Fprintln(stderr, "error: this Orc instance is already running")
				return markRunFailureRendered(errors.New("lock timeout"))
			},
			want: "this Orc instance is already running",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runWithExecutors([]string{"run"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
				run: func(_ RunConfig, _, errOut io.Writer) error { return test.run(errOut) },
			})
			if code != 1 || !strings.Contains(stderr.String(), test.want) || strings.Contains(stderr.String(), "ticket-orc run failed") {
				t.Fatalf("run code=%d stderr=%q, want one diagnostic containing %q", code, stderr.String(), test.want)
			}
			if strings.Contains(stderr.String(), "\x1b") || strings.Count(stderr.String(), "\n") != 1 {
				t.Fatalf("run diagnostic was not sanitized/rendered once: %q", stderr.String())
			}
		})
	}
}

func TestRunCommandReportsStartupFailureCauses(t *testing.T) {
	invoke := func(configPath string) (int, string) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"run", "--config", configPath}, &stdout, &stderr, emptyEnv, rejectExecution)
		return code, stderr.String()
	}
	assertNoGeneric := func(t *testing.T, code int, output string) {
		t.Helper()
		if code == 0 || strings.Contains(output, "ticket-orc run failed; see the startup diagnostic above") {
			t.Fatalf("run code=%d stderr=%q, want one useful failure", code, output)
		}
	}

	t.Run("unusable local root before supervisor", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "config.json")
		localDir := filepath.Join(root, "not-a-directory")
		if err := os.WriteFile(localDir, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q}`, localDir))
		code, output := invoke(configPath)
		assertNoGeneric(t, code, output)
		if !strings.Contains(output, "local directory") || !strings.Contains(output, "not a directory") {
			t.Fatalf("local-root failure omitted its cause: %q", output)
		}
	})

	t.Run("endpoint capability initialization", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "config.json")
		localDir := filepath.Join(root, "runtime")
		if err := os.MkdirAll(filepath.Join(localDir, "endpoint.key"), 0o700); err != nil {
			t.Fatal(err)
		}
		writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q}`, localDir))
		code, output := invoke(configPath)
		assertNoGeneric(t, code, output)
		if !strings.Contains(output, "endpoint capability") || !strings.Contains(output, "not a regular file") {
			t.Fatalf("endpoint initialization failure omitted its cause: %q", output)
		}
	})

	t.Run("duplicate daemon ownership is not repeated", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "config.json")
		localDir := filepath.Join(root, "runtime")
		writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"local_dir":%q}`, localDir))
		lock, err := state.TryAcquireLock(context.Background(), filepath.Join(localDir, "run"))
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Release()
		code, output := invoke(configPath)
		assertNoGeneric(t, code, output)
		if strings.Count(output, "already running") != 1 || !strings.Contains(output, localDir) {
			t.Fatalf("duplicate daemon diagnostic=%q", output)
		}
	})

	t.Run("listener bind failure is not repeated", func(t *testing.T) {
		reserved, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Skipf("loopback sockets unavailable: %v", err)
		}
		defer reserved.Close()
		port := reserved.Addr().(*net.TCPAddr).Port
		root := t.TempDir()
		configPath := filepath.Join(root, "config.json")
		config := fmt.Sprintf(`{"version":1,"local_dir":%q,"supervisor":{"listen":"127.0.0.1","port":%d,"startup_groups":[]}}`, filepath.Join(root, "runtime"), port)
		writeConfigFixture(t, configPath, config)
		code, output := invoke(configPath)
		assertNoGeneric(t, code, output)
		if !strings.Contains(output, fmt.Sprintf("127.0.0.1:%d", port)) || !strings.Contains(output, "daemon listen failed") {
			t.Fatalf("listener failure omitted its address/cause: %q", output)
		}
	})
}

func TestRunCommandFailureDiagnosticsHaveCauseWithoutPhasePreamble(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, RunConfig) RunConfig
		want  string
	}{
		{
			name: "daemon creation",
			setup: func(_ *testing.T, config RunConfig) RunConfig {
				config.startDaemon = func(context.Context, *supervisor.RuntimeState[supervisor.RunWorker], []supervisor.RunWorker) (*daemon.Server, error) {
					return nil, errors.New("injected daemon creation failure")
				}
				return config
			},
			want: "create daemon: injected daemon creation failure",
		},
		{
			name: "worker start",
			setup: func(t *testing.T, config RunConfig) RunConfig {
				worker := supervisorTestWorker("worker", config.StateDir)
				config.Workers = []supervisor.RunWorker{worker}
				config.Runtime = NewRuntimeState(config.Workers)
				config.startChild = func(context.Context, string, string, supervisor.RunWorker, io.Writer, io.Writer, *sync.Mutex, *sync.Mutex) (*runChild, error) {
					return nil, errors.New("injected worker start failure")
				}
				return config
			},
			want: "worker \"worker\" failed to start: worker_unavailable: injected worker start failure",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runWithExecutors([]string{"run"}, &stdout, &stderr, testInstanceEnv(t), commandExecutors{
				run: func(config RunConfig, out, errOut io.Writer) error {
					config = test.setup(t, config)
					return runSupervisor(context.Background(), config, os.Args[0], out, errOut)
				},
			})
			output := stderr.String()
			if code != 1 || strings.Count(output, "\n") != 1 || !strings.Contains(output, test.want) || strings.Contains(output, "startup failed worker=") {
				t.Fatalf("run code=%d stderr=%q, want one complete diagnostic containing %q", code, output, test.want)
			}
		})
	}
}

func captureConfig(t *testing.T, args []string, env envLookup) supervisor.RoleConfig {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	writeConfigFixture(t, filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName), `{"version":1}`)
	var captured supervisor.RoleConfig
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr, env, func(config supervisor.RoleConfig) error {
		captured = config
		return nil
	})
	if code != 0 {
		t.Fatalf("run() code = %d, stderr = %q", code, stderr.String())
	}
	return captured
}

func assertUsageError(t *testing.T, args []string, env envLookup, want string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	called := false
	code := run(args, &stdout, &stderr, env, func(supervisor.RoleConfig) error {
		called = true
		return nil
	})
	if code != 2 {
		t.Fatalf("run() code = %d, want 2; stderr = %q", code, stderr.String())
	}
	if called {
		t.Fatal("executor called for invalid configuration")
	}
	if !strings.Contains(stderr.String(), want) {
		t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
	}
}

func emptyEnv(string) (string, bool) { return "", false }

func mapEnv(values map[string]string) envLookup {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func rejectExecution(supervisor.RoleConfig) error { return errors.New("executor should not run") }

func rejectStateExecution(StateConfig, io.Writer) error {
	return errors.New("state executor should not run")
}

func rejectGCExecution(GCConfig, io.Writer, io.Writer) error {
	return errors.New("gc executor should not run")
}

func rejectingExecutors() commandExecutors {
	return commandExecutors{role: rejectExecution, state: rejectStateExecution, gc: rejectGCExecution}
}
