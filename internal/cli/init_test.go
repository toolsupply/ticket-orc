package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestInitCreatesMinimalSecureInstanceAndRequiresForceToReplace(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "instance")
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	var out, stderr bytes.Buffer
	if code := executeInit(nil, &out, &stderr, lookup); code != 0 {
		t.Fatalf("init code=%d stderr=%q", code, stderr.String())
	}
	wantOutput := fmt.Sprintf("\nCreated Orc configuration:\n\n  %s\n\nStart it with:\n\n  ticket-orc run -i\n\n", filepath.Join(dir, instanceConfigFileName))
	if got := out.String(); got != wantOutput {
		t.Fatalf("init output = %q, want %q", got, wantOutput)
	}
	if strings.Contains(out.String(), "ID:") || strings.Contains(out.String(), "HTTP:") {
		t.Fatalf("init output includes instance details: %q", out.String())
	}
	path := filepath.Join(dir, "config.json")
	ignorePath := filepath.Join(dir, ".gitignore")
	ignore, err := os.ReadFile(ignorePath)
	if err != nil || string(ignore) != initGitignore {
		t.Fatalf("init ignore = %q, error=%v, want generated mutable-data policy", ignore, err)
	}
	if string(ignore) != "/.local/\n" {
		t.Fatalf("init ignore = %q, want only the local runtime directory ignored", ignore)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 || entries[0].Name() != ".gitignore" || entries[1].Name() != "config.json" {
		t.Fatalf("init created unexpected files: %v, error=%v", entries, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".local")); !os.IsNotExist(err) {
		t.Fatalf("init eagerly created local runtime data: %v", err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("instance dir mode=%v error=%v", info, err)
		}
		info, err = os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("config mode=%v error=%v", info, err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config["version"] != float64(1) || config["default_role"] != "coder" || config["workers"] != nil || config["repositories"] != nil || len(config) != 4 {
		t.Fatalf("init config contains unexpected fields: %#v", config)
	}
	if _, err := os.Stat(filepath.Join(dir, "steer.json")); !os.IsNotExist(err) {
		t.Fatalf("init created dynamic registrations: %v", err)
	}
	id, ok := config["id"].(string)
	if !ok || !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("instance id=%v is not UUIDv4", config["id"])
	}
	roles, ok := config["roles"].(map[string]any)
	if !ok || roles["coder"].(map[string]any)["ticket_queue"] != "open" || roles["reviewer"].(map[string]any)["ticket_queue"] != "review" {
		t.Fatalf("init roles=%#v", config["roles"])
	}
	coderFields := roles["coder"].(map[string]any)
	if _, exists := coderFields["review_completion"]; exists {
		t.Fatalf("init coder role unexpectedly sets review_completion: %#v", coderFields)
	}
	reviewerFields := roles["reviewer"].(map[string]any)
	if got := reviewerFields["review_completion"]; got != ReviewCompletionClose {
		t.Fatalf("init reviewer review_completion=%v, want %q", got, ReviewCompletionClose)
	}
	for name, role := range roles {
		fields := role.(map[string]any)
		wantFieldCount := 2
		if name == "reviewer" {
			wantFieldCount = 3
		}
		if len(fields) != wantFieldCount || fields["nudge_prompt"] == nil || fields["queue"] != nil || fields["queue_prompt"] != nil {
			t.Fatalf("init role %q fields=%#v", name, fields)
		}
	}
	if prompt := roles["coder"].(map[string]any)["nudge_prompt"].(string); !strings.Contains(prompt, "current actor") || !strings.Contains(prompt, "objective and acceptance criteria") || !strings.Contains(prompt, "relevant checks") || !strings.Contains(prompt, "Set a goal") || strings.Contains(strings.ToLower(prompt), "skill") {
		t.Fatalf("coder nudge lost required guidance or activates Skills: %q", prompt)
	}
	if prompt := roles["reviewer"].(map[string]any)["nudge_prompt"].(string); !strings.Contains(prompt, "substantive review") || !strings.Contains(prompt, "specification compliance") || !strings.Contains(prompt, "Set a goal") || strings.Contains(strings.ToLower(prompt), "approve") || strings.Contains(strings.ToLower(prompt), "close") || strings.Contains(strings.ToLower(prompt), "skill") {
		t.Fatalf("reviewer nudge has incomplete review guidance or lifecycle/Skill instructions: %q", prompt)
	}
	loaded, err := LoadFileConfig(dir, path, true)
	wantLocalDir := filepath.Join(dir, ".local", id)
	if err != nil || loaded.Config.ID != id || loaded.Instance.LocalDir != wantLocalDir {
		t.Fatalf("load initialized config id=%q local_dir=%q err=%v, want runtime root %q", loaded.Config.ID, loaded.Instance.LocalDir, err, wantLocalDir)
	}
	statePath := filepath.Join(dir, "state.json")
	stateData := []byte("existing state")
	if err := os.WriteFile(statePath, stateData, 0o600); err != nil {
		t.Fatal(err)
	}
	var againOut, againErr bytes.Buffer
	if code := executeInit(nil, &againOut, &againErr, lookup); code == 0 || againErr.Len() == 0 {
		t.Fatalf("repeated init code=%d stderr=%q, want overwrite error", code, againErr.String())
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != string(data) {
		t.Fatalf("init without --force changed existing config: %v", err)
	}
	if code := executeInit([]string{"--force"}, &againOut, &againErr, lookup); code != 0 {
		t.Fatalf("forced init code=%d stderr=%q", code, againErr.String())
	}
	if got, err := os.ReadFile(statePath); err != nil || string(got) != string(stateData) {
		t.Fatalf("forced init changed existing state to %q, error=%v", got, err)
	}
}

func TestInitUsesProjectLocalDirectoryWithoutAncestorSearch(t *testing.T) {
	parent := t.TempDir()
	parentInstance := filepath.Join(parent, defaultInstanceDirectoryName)
	if err := os.Mkdir(parentInstance, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parentInstance, instanceConfigFileName), []byte("parent config"), 0o600); err != nil {
		t.Fatal(err)
	}
	cwd := filepath.Join(parent, "nested", "project")
	if err := os.MkdirAll(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)
	var stdout, stderr bytes.Buffer
	if code := executeInit(nil, &stdout, &stderr, emptyEnv); code != 0 {
		t.Fatalf("init code=%d stderr=%q", code, stderr.String())
	}
	localConfig := filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName)
	if _, err := os.Stat(localConfig); err != nil {
		t.Fatalf("local config was not created: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(parentInstance, instanceConfigFileName)); err != nil || string(got) != "parent config" {
		t.Fatalf("ancestor config changed: %q error=%v", got, err)
	}
}

func TestInitGlobalAndTicketORCOverrideSelection(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	lookup := mapEnv(map[string]string{"TICKET_ORC": "custom-instance"})

	var stdout, stderr bytes.Buffer
	if code := executeInit(nil, &stdout, &stderr, lookup); code != 0 {
		t.Fatalf("override init code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(cwd, "custom-instance", instanceConfigFileName)); err != nil {
		t.Fatalf("relative TICKET_ORC override was not used: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := executeInit([]string{"--global"}, &stdout, &stderr, lookup); code != 0 {
		t.Fatalf("global init code=%d stderr=%q", code, stderr.String())
	}
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	globalConfigPath := filepath.Join(globalDir, instanceConfigFileName)
	if _, err := os.Stat(globalConfigPath); err != nil {
		t.Fatalf("--global did not take precedence over TICKET_ORC: %v", err)
	}
	globalIgnore, err := os.ReadFile(filepath.Join(globalDir, ".gitignore"))
	if err != nil || string(globalIgnore) != "/.local/\n" {
		t.Fatalf("--global ignore = %q, error=%v; want same local-data policy", globalIgnore, err)
	}
	globalEntries, err := os.ReadDir(globalDir)
	if err != nil || len(globalEntries) != 2 || globalEntries[0].Name() != ".gitignore" || globalEntries[1].Name() != instanceConfigFileName {
		t.Fatalf("--global created unexpected files: entries=%v error=%v", globalEntries, err)
	}
	globalConfig, err := LoadFileConfig(globalDir, globalConfigPath, true)
	if err != nil {
		t.Fatalf("load --global config: %v", err)
	}
	wantGlobalLocalDir := filepath.Join(globalDir, ".local", globalConfig.Config.ID)
	if globalConfig.Instance.LocalDir != wantGlobalLocalDir {
		t.Fatalf("--global implicit runtime root=%q, want %q", globalConfig.Instance.LocalDir, wantGlobalLocalDir)
	}
	if _, err := os.Stat(filepath.Join(globalDir, ".local")); !os.IsNotExist(err) {
		t.Fatalf("--global init eagerly created local runtime data: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName)); !os.IsNotExist(err) {
		t.Fatalf("--global unexpectedly created the local instance: %v", err)
	}
}

func TestRunHelpDocumentsLocalRuntimeDirectory(t *testing.T) {
	for _, phrase := range []string{"top-level local_dir", "exact root", "Relative paths are resolved from the configuration", "file.", ".local/<config-id>"} {
		if !strings.Contains(runCommandHelp, phrase) {
			t.Fatalf("run help is missing %q", phrase)
		}
	}
	if strings.Contains(initHelp, "<config-id>") {
		t.Fatalf("init quick start exposes config ID mechanics: %q", initHelp)
	}
}

func TestInitForcePreservesExistingGitignore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	customIgnore := []byte("custom project rules\n")
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), customIgnore, 0o600); err != nil {
		t.Fatal(err)
	}
	lookup := mapEnv(map[string]string{"TICKET_ORC": dir})
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, lookup); code != 0 {
		t.Fatalf("init code=%d stderr=%q", code, stderr.String())
	}
	if got, err := os.ReadFile(filepath.Join(dir, ".gitignore")); err != nil || string(got) != string(customIgnore) {
		t.Fatalf("existing ignore changed to %q, error=%v", got, err)
	}
}

func TestManualReviewerConfigurationKeepsSignoffDefaultAndHonorsExplicitPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   string
	}{
		{name: "omitted", want: ReviewCompletionSignoff},
		{name: "explicit signoff", policy: `,"review_completion":"signoff"`, want: ReviewCompletionSignoff},
		{name: "explicit close", policy: `,"review_completion":"close"`, want: ReviewCompletionClose},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), instanceConfigFileName)
			writeConfigFixture(t, path, `{"version":1,"default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"review"`+test.policy+`}}}`)
			config, _, err := parseRoleConfig(RoleReviewer, []string{"--config", path}, emptyEnv)
			if err != nil {
				t.Fatal(err)
			}
			if config.ReviewCompletion != test.want {
				t.Fatalf("review completion=%q, want %q", config.ReviewCompletion, test.want)
			}
		})
	}
}

func TestInitSecuresExistingInstanceDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureSecureInstanceDir(dir); err != nil {
		t.Fatalf("secure existing directory: %v", err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("existing instance dir mode=%v error=%v, want 0700", info, err)
		}
	}
}

func TestInitRejectsNonDirectoryInstancePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureSecureInstanceDir(path); err == nil {
		t.Fatal("expected non-directory instance path to be rejected")
	}
}

func TestInitRejectsSymlinkInstancePath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "instance")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := ensureSecureInstanceDir(link); err == nil {
		t.Fatal("expected symlink instance path to be rejected")
	}
}

func TestResolveInstanceDirectoryAndExplicitConfigDirectory(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "relative TICKET_ORC", value: filepath.Join("state", "orc"), want: filepath.Join(cwd, "state", "orc")},
		{name: "absolute TICKET_ORC", value: filepath.Join(cwd, "other-instance"), want: filepath.Join(cwd, "other-instance")},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, err := resolveInstanceDir(cwd, mapEnv(map[string]string{"TICKET_ORC": test.value}))
			if err != nil || dir != test.want {
				t.Fatalf("instance dir=%q err=%v, want %q", dir, err, test.want)
			}
		})
	}
	configPath, explicit, err := invocationConfigPath(map[string]string{"config": filepath.Join("other", instanceConfigFileName)}, emptyEnv)
	if err != nil || !explicit || configPath != filepath.Join(cwd, "other", instanceConfigFileName) {
		t.Fatalf("explicit config path=%q explicit=%v err=%v", configPath, explicit, err)
	}
	t.Setenv("HOME", cwd)
	t.Setenv("USERPROFILE", cwd)
	if err := os.MkdirAll(filepath.Join(cwd, defaultInstanceDirectoryName), 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := resolveInstanceDir(cwd, emptyEnv)
	if err != nil || dir != filepath.Join(cwd, defaultInstanceDirectoryName) {
		t.Fatalf("default instance dir=%q err=%v", dir, err)
	}
}

func TestManagedWorkerUsesArbitraryRoleQueue(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"default_role":"architect","roles":{"architect":{"ticket_queue":"open","nudge_prompt":"Implement."},"quality":{"ticket_queue":"review","nudge_prompt":"Review."}},"workers":{"one":{"role":"architect","actor":"worker-one"},"two":{"role":"quality","actor":"worker-two"}}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatal(err)
	}
	workers, diagnostics := resolveAllWorkers(loaded, emptyEnv)
	if diagnostics.HasErrors() {
		t.Fatalf("worker diagnostics: %v", diagnostics)
	}
	if got := workers["one"].Config.Role; got != RoleCoder {
		t.Fatalf("architect queue dispatched to %q, want implementation workflow", got)
	}
	if got := workers["two"].Config.Role; got != RoleReviewer {
		t.Fatalf("quality queue dispatched to %q, want review workflow", got)
	}
	loaded.Config.Workers["one"] = WorkerFileConfig{Role: "missing"}
	if err := validateAndNormalizeFileConfig(&loaded.Config, root); err == nil || !regexp.MustCompile(`unknown role`).MatchString(err.Error()) {
		t.Fatalf("unknown role error=%v", err)
	}
}
