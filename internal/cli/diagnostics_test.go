package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderRuntimeSkillReloadWarningRequiresRestart(t *testing.T) {
	var output bytes.Buffer
	renderRuntimeConfigWarnings(&output, ConfigDiagnostics{{
		Severity:    DiagnosticWarning,
		Code:        "reload.restart_required",
		Worker:      "coder",
		Message:     "running worker keeps its effective configuration until restarted",
		Remediation: "restart this worker to apply the desired configuration",
	}})
	text := output.String()
	if !strings.Contains(text, "requires restart") || !strings.Contains(text, "restart this worker") {
		t.Fatalf("runtime Skill reload warning = %q", text)
	}
	if strings.Contains(text, "keeps its current identity and routing") {
		t.Fatalf("runtime warning used restart-only wording: %q", text)
	}
}

func TestLoadFileConfigUnknownFieldIncludesPathAndSuggestion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","actro":"x"}}}`)
	_, err := LoadFileConfig(filepath.Dir(path), path, true)
	if err == nil || !strings.Contains(err.Error(), "workers.one.actro") || !strings.Contains(err.Error(), "actor") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigCheckPreservesSafeInheritedActorValidationReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding.","actor":"two actors"},"reviewer":{"ticket_queue":"review","nudge_prompt":"Review."}},"workers":{"one":{"role":"coder"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(stdout.String(), "actor.invalid") || strings.Contains(stdout.String(), "two actors") {
		t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
}

func TestConfigCheckDoesNotCreateRuntimeRootOrMarker(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1}`)
	var stdout, stderr bytes.Buffer
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputQuiet}, &stdout, &stderr, emptyEnv); err != nil {
		t.Fatalf("config check: %v stderr=%q", err, stderr.String())
	}
	if _, err := os.Lstat(filepath.Join(root, ".local")); !os.IsNotExist(err) {
		t.Fatalf("config check created runtime state: lstat err=%v", err)
	}
}

func TestConfigCheckUsesTypedFieldDiagnostics(t *testing.T) {
	tests := []struct {
		name, field, value, code, path string
	}{
		{"output", "output", "verbose", "output.invalid", "workers.one.output"},
		{"reasoning", "reasoning", "invalid", "reasoning.invalid", "workers.one.reasoning"},
		{"sandbox", "codex", `{"sandbox":"unsafe"}`, "codex_sandbox.invalid", "workers.one.codex.sandbox"},
		{"permission", "harness", "claude", "claude_permission_mode.invalid", "workers.one.claude.permission_mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.json")
			value := fmt.Sprintf("%q", test.value)
			if test.name == "sandbox" {
				value = test.value
			}
			worker := fmt.Sprintf(`{"role":"coder","actor":"actor-one","%s":%s}`, test.field, value)
			if test.name == "permission" {
				worker = `{"role":"coder","actor":"actor-one","harness":"claude","claude":{"permission_mode":"unsafe"}}`
			}
			data := `{"version":1,"workers":{"one":` + worker + `}}`
			writeConfigFixture(t, path, data)
			var stdout, stderr bytes.Buffer
			if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, func(string) (string, bool) { return "", false }); err == nil || !strings.Contains(stdout.String(), test.code) || !strings.Contains(stdout.String(), test.path) {
				t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
			}
		})
	}
}

func TestConfigCheckRejectsMissingWorkingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","actor":"one","working_dir":"missing-repository"}}}`)
	var stdout, stderr bytes.Buffer
	err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, emptyEnv)
	if err == nil || !strings.Contains(stderr.String(), "workers.one.working_dir") {
		t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
}

func TestExecuteConfigCheckWarningsDoNotFailOrContactTicket(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","actor":"actor-one"}}}`)
	var stdout, stderr bytes.Buffer
	err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, func(string) (string, bool) { return "", false })
	if err != nil || strings.Contains(stdout.String(), "no_workers") {
		t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
}

func TestConfigCheckAcceptsZeroConfiguredWorkers(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","workers":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, func(string) (string, bool) { return "", false }); err != nil || strings.Contains(stdout.String(), "no_workers") {
		t.Fatalf("empty config err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
}

func TestConfigCheckRejectsInvalidReviewCompletionWithoutWorkers(t *testing.T) {
	tests := []struct {
		name, config, field string
	}{
		{
			name:   "role policy",
			config: `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"Review.","review_completion":"bogus"}},"workers":{}}`,
			field:  "roles.reviewer.review_completion",
		},
		{
			name:   "default policy",
			config: `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","defaults":{"review_completion":"bogus"},"workers":{}}`,
			field:  "defaults.review_completion",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(test.config), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, emptyEnv)
			if err == nil || !strings.Contains(stderr.String(), test.field) {
				t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRenderRuntimeConfigWarningsUsesHumanMessages(t *testing.T) {
	diagnostics := ConfigDiagnostics{{Severity: DiagnosticWarning, Code: "duplicate_ticket_actor", Path: "workers.actor", Message: "workers share a Ticket actor identity"}}
	var output bytes.Buffer
	renderRuntimeConfigWarnings(&output, diagnostics)
	want := "[ticket-orc] config warning: workers share a Ticket actor identity\n"
	if output.String() != want {
		t.Fatalf("runtime warnings = %q, want %q", output.String(), want)
	}
	for _, forbidden := range []string{"code=", "path=", "remediation=", "role-scoped"} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("runtime warning leaked structured detail %q: %q", forbidden, output.String())
		}
	}
}

func TestRenderConfigDiagnosticsKeepsStructuredDetails(t *testing.T) {
	diagnostics := ConfigDiagnostics{{Severity: DiagnosticWarning, Code: "reload.restart_required", Path: "workers.one", Worker: "one", Message: "worker changes require restart", Remediation: "restart this worker"}}
	var output bytes.Buffer
	renderConfigDiagnostics(&output, diagnostics)
	for _, want := range []string{"code=reload.restart_required", "path=workers.one", "remediation=restart this worker"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("structured diagnostics = %q, missing %q", output.String(), want)
		}
	}
}

func TestConfigCheckJSONOutputIsDeterministic(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	data := `{"version":1,"workers":{"z":{"role":"reviewer","actor":"shared"},"a":{"role":"coder","actor":"shared"}}}`
	writeConfigFixture(t, path, data)
	var first, second, stderr bytes.Buffer
	for _, output := range []*bytes.Buffer{&first, &second} {
		if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputJSON}, output, &stderr, func(string) (string, bool) { return "", false }); err != nil {
			t.Fatal(err)
		}
	}
	if first.String() != second.String() {
		t.Fatalf("nondeterministic JSON diagnostics: %q != %q", first.String(), second.String())
	}
}
