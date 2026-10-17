package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKnownObsoleteNudgePromptsFollowQueueNotRoleName(t *testing.T) {
	tests := []struct {
		name, queue, prompt string
		want                bool
	}{
		{"historical built-in coder", "open", legacyCoderNudgePrompt, true},
		{"historical built-in reviewer", "review", legacyReviewerNudgePrompt, true},
		{"README coder example", "open", legacyReadmeCoderNudgePrompt, true},
		{"README reviewer example", "review", legacyReadmeReviewerNudgePrompt, true},
		{"renamed open role", "open", "  " + legacyReadmeCoderNudgePrompt + "\n", true},
		{"renamed review role", "review", "  " + legacyReviewerNudgePrompt + "\n", true},
		{"wrong queue", "review", legacyReadmeCoderNudgePrompt, false},
		{"new coder default", "open", coderNudgePrompt, false},
		{"new reviewer default", "review", reviewerNudgePrompt, false},
		{"unrelated custom text", "open", "Process the current ticket.", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := knownObsoleteNudgePrompt(test.queue, test.prompt); got != test.want {
				t.Fatalf("knownObsoleteNudgePrompt(%q, %q) = %t, want %t", test.queue, test.prompt, got, test.want)
			}
		})
	}
}

func TestObsoleteNudgePromptWarningConfigAndInstanceAdvice(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	globalConfig := filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName)
	localDir := filepath.Join(t.TempDir(), defaultInstanceDirectoryName)
	selectedDir := t.TempDir()
	explicitDir := t.TempDir()
	otherDir := t.TempDir()
	customDir := t.TempDir()

	tests := []struct {
		name, configPath string
		instance         InstanceContext
		env              map[string]string
		want, unwanted   string
	}{
		{
			name:       "global default config",
			configPath: globalConfig,
			instance:   InstanceContext{ConfigPath: globalConfig, InstanceDir: filepath.Dir(globalConfig)},
			want:       "ticket-orc init --global --force",
		},
		{
			name:       "local default config",
			configPath: filepath.Join(localDir, instanceConfigFileName),
			instance:   InstanceContext{ConfigPath: filepath.Join(localDir, instanceConfigFileName), InstanceDir: localDir},
			want:       "ticket-orc init --force from the project root that selected this local instance",
			unwanted:   "--global",
		},
		{
			name:       "selected instance directory",
			configPath: filepath.Join(selectedDir, instanceConfigFileName),
			instance:   InstanceContext{ConfigPath: filepath.Join(selectedDir, instanceConfigFileName), InstanceDir: selectedDir},
			env:        map[string]string{"TICKET_ORC": selectedDir},
			want:       "set TICKET_ORC to the directory containing the loaded config file above, then run ticket-orc init --force",
		},
		{
			name:       "explicit canonical config overrides environment selection",
			configPath: filepath.Join(explicitDir, instanceConfigFileName),
			instance:   InstanceContext{ConfigPath: filepath.Join(explicitDir, instanceConfigFileName), InstanceDir: explicitDir, Explicit: true},
			env:        map[string]string{"TICKET_ORC": otherDir},
			want:       "set TICKET_ORC to the directory containing the loaded config file above, then run ticket-orc init --force",
			unwanted:   otherDir,
		},
		{
			name:       "explicit custom filename",
			configPath: filepath.Join(customDir, "orc-settings.json"),
			instance:   InstanceContext{ConfigPath: filepath.Join(customDir, "orc-settings.json"), InstanceDir: customDir, Explicit: true},
			want:       "will not replace this explicitly named file",
			unwanted:   "ticket-orc init --force for this instance",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := FileConfig{Roles: map[string]RoleFileConfig{
				"quality": {TicketQueue: "review", NudgePrompt: legacyReviewerNudgePrompt},
				"backend": {TicketQueue: "open", NudgePrompt: legacyReadmeCoderNudgePrompt},
			}}
			lookup := mapEnv(test.env)
			diagnostics := obsoleteNudgePromptDiagnostic(config, test.instance, lookup)
			if len(diagnostics) != 1 {
				t.Fatalf("diagnostics = %#v, want one warning", diagnostics)
			}
			warning := diagnostics[0]
			if warning.Severity != DiagnosticWarning || warning.Code != "run.obsolete_nudge_prompt" || warning.Path != test.configPath {
				t.Fatalf("warning = %#v", warning)
			}
			for _, want := range []string{"roles.backend.nudge_prompt", "roles.quality.nudge_prompt", test.want, "at most one ticket", "new instance ID", "owned runtime state", "back up customizations"} {
				if !strings.Contains(warning.Message+" "+warning.Remediation, want) {
					t.Errorf("warning missing %q: %#v", want, warning)
				}
			}
			if test.unwanted != "" && strings.Contains(warning.Remediation, test.unwanted) {
				t.Errorf("warning remediation unexpectedly contains %q: %s", test.unwanted, warning.Remediation)
			}
		})
	}
}

func TestObsoleteNudgePromptRenderedAdvicePreservesInstancePath(t *testing.T) {
	root := t.TempDir()
	instanceDir := filepath.Join(root, "my  folder", "it's an instance")
	configPath := filepath.Join(instanceDir, instanceConfigFileName)
	diagnostics := obsoleteNudgePromptDiagnostic(
		FileConfig{Roles: map[string]RoleFileConfig{"quality": {TicketQueue: "review", NudgePrompt: legacyReviewerNudgePrompt}}},
		InstanceContext{ConfigPath: configPath, InstanceDir: instanceDir, Explicit: true},
		mapEnv(nil),
	)
	var output bytes.Buffer
	renderRuntimeConfigWarnings(&output, diagnostics)
	text := output.String()
	for _, want := range []string{
		configPath,
		"set TICKET_ORC to the directory containing the loaded config file above, then run ticket-orc init --force",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered warning missing %q: %q", want, text)
		}
	}
	if strings.Contains(text, filepath.Join(root, "my folder")) {
		t.Fatalf("rendered warning collapsed repeated path spaces: %q", text)
	}
}

func TestObsoleteNudgePromptWithControlPathDoesNotSuggestResetTarget(t *testing.T) {
	instanceDir := filepath.Join(t.TempDir(), "legacy\ninstance")
	diagnostics := obsoleteNudgePromptDiagnostic(
		FileConfig{Roles: map[string]RoleFileConfig{"quality": {TicketQueue: "review", NudgePrompt: legacyReviewerNudgePrompt}}},
		InstanceContext{ConfigPath: filepath.Join(instanceDir, instanceConfigFileName), InstanceDir: instanceDir, Explicit: true},
		mapEnv(nil),
	)
	var output bytes.Buffer
	renderRuntimeConfigWarnings(&output, diagnostics)
	text := output.String()
	if strings.Contains(text, "set TICKET_ORC") || !strings.Contains(text, "no reset target is suggested") || !strings.Contains(text, `legacy\u000ainstance`) {
		t.Fatalf("control-containing path guidance = %q", text)
	}
}

func TestParseRunConfigWarnsForObsoletePromptsInBothRunModes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"backend","roles":{"backend":{"ticket_queue":"open","nudge_prompt":"`+legacyReadmeCoderNudgePrompt+`"},"quality":{"ticket_queue":"review","nudge_prompt":"`+legacyReviewerNudgePrompt+`"}},"workers":{}}`)

	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "ordinary run", args: []string{"--config", path}},
		{name: "interactive run", args: []string{"-i", "--config", path}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, help, err := parseRunConfig(test.args, emptyEnv)
			if err != nil || help {
				t.Fatalf("parseRunConfig help=%t err=%v", help, err)
			}
			if config.Interactive != (test.name == "interactive run") {
				t.Fatalf("interactive = %t", config.Interactive)
			}
			if len(config.Workers) != 0 {
				t.Fatalf("workers = %#v, want no workers", config.Workers)
			}
			found := false
			for _, diagnostic := range config.Diagnostics {
				if diagnostic.Code == "run.obsolete_nudge_prompt" {
					found = true
					if diagnostic.Path != path || !strings.Contains(diagnostic.Message, "roles.backend.nudge_prompt") || !strings.Contains(diagnostic.Message, "roles.quality.nudge_prompt") {
						t.Fatalf("obsolete prompt diagnostic = %#v", diagnostic)
					}
				}
			}
			if !found {
				t.Fatalf("run diagnostics = %#v, missing obsolete prompt warning", config.Diagnostics)
			}
		})
	}
}

type cancelOnWarningWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	cancel context.CancelFunc
	seen   chan struct{}
	once   sync.Once
}

func (writer *cancelOnWarningWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	n, err := writer.buffer.Write(data)
	text := writer.buffer.String()
	writer.mu.Unlock()
	if strings.Contains(text, "run.obsolete_nudge_prompt") || strings.Contains(text, "obsolete dynamic nudge prompt") {
		writer.once.Do(func() {
			close(writer.seen)
			writer.cancel()
		})
	}
	return n, err
}

func (writer *cancelOnWarningWriter) String() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.String()
}

func TestRunSupervisorShowsObsoletePromptWarningBeforeReadyWithoutWorkers(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			writer := &cancelOnWarningWriter{cancel: cancel, seen: make(chan struct{})}
			configPath := filepath.Join(t.TempDir(), "legacy", instanceConfigFileName)
			done := startSupervisorTestRun(t, cancel, func() error {
				return runSupervisor(ctx, RunConfig{
					ConfigPath:  configPath,
					StateDir:    filepath.Join(t.TempDir(), "state"),
					Interactive: interactive,
					Diagnostics: ConfigDiagnostics{{
						Severity:    DiagnosticWarning,
						Code:        "run.obsolete_nudge_prompt",
						Path:        configPath,
						Message:     "obsolete dynamic nudge prompt found in roles.backend.nudge_prompt",
						Remediation: "Dynamic turns now handle at most one ticket.",
					}},
				}, os.Args[0], io.Discard, writer)
			})
			select {
			case <-writer.seen:
			case <-time.After(3 * time.Second):
				t.Fatal("startup did not render the warning before readiness")
			}
			select {
			case err := <-done:
				if err == nil && ctx.Err() == nil {
					t.Fatal("supervisor exited without cancellation")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("supervisor did not stop after warning cancellation")
			}
			output := writer.String()
			for _, want := range []string{configPath, "roles.backend.nudge_prompt", "at most one ticket"} {
				if !strings.Contains(output, want) {
					t.Errorf("startup warning missing %q: %q", want, output)
				}
			}
			if count := strings.Count(output, "obsolete dynamic nudge prompt"); count != 1 {
				t.Errorf("startup obsolete prompt warning count = %d, want one: %q", count, output)
			}
		})
	}
}

func TestObsoleteNudgePromptConfigInspectionHasNoDiagnosticSideEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom-settings.json")
	writeConfigFixture(t, path, `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"old","roles":{"old":{"ticket_queue":"open","nudge_prompt":"`+legacyReadmeCoderNudgePrompt+`"}},"workers":{}}`)
	var stdout, stderr bytes.Buffer
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, emptyEnv); err != nil {
		t.Fatalf("config check: %v stderr=%q", err, stderr.String())
	}
	if strings.Contains(stdout.String(), "obsolete_nudge_prompt") || strings.Contains(stderr.String(), "obsolete dynamic nudge") {
		t.Fatalf("inspection unexpectedly emitted run warning: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), ".local")); !os.IsNotExist(err) {
		t.Fatalf("config inspection created runtime state: %v", err)
	}
}
