package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
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
	if string(ignore) != "/.local/\n/.local.guard/\n" {
		t.Fatalf("init ignore = %q, want runtime and coordination directories ignored", ignore)
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
	wantLocalDir := filepath.Join(dir, ".local")
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
	if err != nil || string(globalIgnore) != initGitignore {
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
	wantGlobalLocalDir := filepath.Join(globalDir, ".local")
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
	for _, phrase := range []string{"top-level local_dir", "exact root", "Relative paths are resolved from the configuration", "file.", ".local beside"} {
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

func TestInitForceRemovesDefaultRuntimeLayouts(t *testing.T) {
	for _, layout := range []string{"direct", "legacy", "obsolete migration artifact"} {
		t.Run(layout, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "instance")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, instanceConfigFileName)
			writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
			localRoot := filepath.Join(dir, ".local")
			switch layout {
			case "direct":
				if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(localRoot, "run"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(localRoot, "run", "sentinel"), []byte("owned"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "legacy":
				for _, id := range []string{runtimeMarkerTestID, runtimeMarkerOtherID} {
					root := filepath.Join(localRoot, id)
					if err := os.MkdirAll(root, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, "sentinel"), []byte("owned"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case "obsolete migration artifact":
				if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
					t.Fatal(err)
				}
				holdingChild := filepath.Join(dir, obsoleteRuntimeMigrationName, runtimeMarkerTestID)
				if err := os.MkdirAll(holdingChild, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := writeRuntimeMarker(filepath.Join(holdingChild, runtimeMarkerFileName), runtimeMarker{Version: runtimeMarkerVersion, Layout: runtimeLayoutVersion, InstanceID: runtimeMarkerTestID}); err != nil {
					t.Fatal(err)
				}
			}

			var stdout, stderr bytes.Buffer
			if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code != 0 {
				t.Fatalf("force init code=%d stderr=%q", code, stderr.String())
			}
			if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
				t.Fatalf("runtime root remains after force reset: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, obsoleteRuntimeMigrationName)); !os.IsNotExist(err) {
				t.Fatalf("migration holding path remains after force reset: %v", err)
			}
			loaded, err := LoadFileConfig(dir, configPath, true)
			if err != nil || loaded.Config.ID == runtimeMarkerTestID {
				t.Fatalf("forced config ID=%q err=%v, want a fresh ID", loaded.Config.ID, err)
			}
			if err := ensureLoadedRuntime(loaded); err != nil {
				t.Fatalf("lazily recreate runtime after force reset: %v", err)
			}
			marker, err := readAndValidateRuntimeMarker(localRoot)
			if err != nil || marker.InstanceID != loaded.Config.ID {
				t.Fatalf("recreated runtime marker=%#v err=%v, want fresh config ID %s", marker, err, loaded.Config.ID)
			}
		})
	}
}

func TestInitForceRefusesActiveDaemonWithoutChangingInstance(t *testing.T) {
	for _, layout := range []string{"direct", "legacy", "external"} {
		t.Run(layout, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "instance")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, instanceConfigFileName)
			runtimeRoot := filepath.Join(dir, ".local")
			switch layout {
			case "legacy":
				runtimeRoot = filepath.Join(runtimeRoot, runtimeMarkerTestID)
				writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
			case "external":
				runtimeRoot = filepath.Join(t.TempDir(), "external-runtime")
				if err := os.MkdirAll(runtimeRoot, 0o700); err != nil {
					t.Fatal(err)
				}
				writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`","local_dir":`+mustJSONString(t, runtimeRoot)+`}`)
				if err := ensureRuntimeOwnership(runtimeRoot, runtimeMarkerTestID, true); err != nil {
					t.Fatal(err)
				}
			default:
				writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
				if err := ensureRuntimeOwnership(runtimeRoot, runtimeMarkerTestID, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll(runtimeRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(runtimeRoot, "sentinel"), []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			lock, err := state.TryAcquireLock(context.Background(), filepath.Join(runtimeRoot, "run"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Release()

			var stdout, stderr bytes.Buffer
			if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code == 0 || !strings.Contains(stderr.String(), "instance is running") {
				t.Fatalf("force init code=%d stderr=%q, want active-instance refusal", code, stderr.String())
			}
			after, err := os.ReadFile(configPath)
			if err != nil || string(after) != string(before) {
				t.Fatalf("config changed after active-daemon refusal: %v", err)
			}
			if data, err := os.ReadFile(filepath.Join(runtimeRoot, "sentinel")); err != nil || string(data) != "keep" {
				t.Fatalf("runtime changed after active-daemon refusal: data=%q err=%v", data, err)
			}
		})
	}
}

func TestInitForceProtectsSelectedChildDaemonWithDirectMarker(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	localRoot := filepath.Join(dir, defaultRuntimeRootName)
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localRoot, "sentinel"), []byte("direct"), 0o600); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(localRoot, runtimeMarkerTestID)
	if err := os.MkdirAll(filepath.Join(selected, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	childSentinel := filepath.Join(selected, "sentinel")
	if err := os.WriteFile(childSentinel, []byte("selected"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := state.TryAcquireLock(context.Background(), filepath.Join(selected, "run"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code == 0 || !strings.Contains(stderr.String(), "instance is running") {
		t.Fatalf("force init code=%d stderr=%q, want selected-child daemon refusal", code, stderr.String())
	}
	if after, err := os.ReadFile(configPath); err != nil || string(after) != string(before) {
		t.Fatalf("config changed after active-child refusal: %v", err)
	}
	if direct, err := os.ReadFile(filepath.Join(localRoot, "sentinel")); err != nil || string(direct) != "direct" {
		t.Fatalf("direct runtime changed after active-child refusal: %q err=%v", direct, err)
	}
	if child, err := os.ReadFile(childSentinel); err != nil || string(child) != "selected" {
		t.Fatalf("selected child changed after active-child refusal: %q err=%v", child, err)
	}
}

func TestInitForceRequiresExternalRuntimeOwnershipMarker(t *testing.T) {
	for _, ownership := range []string{"matching", "missing", "mismatching", "malformed", "unsupported"} {
		t.Run(ownership, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "instance")
			external := filepath.Join(t.TempDir(), "runtime")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(external, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, instanceConfigFileName)
			writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`","local_dir":`+mustJSONString(t, external)+`}`)
			if ownership != "missing" {
				markerID := runtimeMarkerTestID
				if ownership == "mismatching" {
					markerID = runtimeMarkerOtherID
				}
				layout := 2
				if ownership == "unsupported" {
					layout = 3
				}
				if err := writeRuntimeMarker(filepath.Join(external, runtimeMarkerFileName), runtimeMarker{Version: 1, Layout: layout, InstanceID: markerID}); err != nil {
					t.Fatal(err)
				}
				if ownership == "malformed" {
					if err := os.WriteFile(filepath.Join(external, runtimeMarkerFileName), []byte(`{"version":1}`), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := os.WriteFile(filepath.Join(external, "sentinel"), []byte("owned"), 0o600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir}))
			if ownership == "matching" {
				if code != 0 {
					t.Fatalf("force init code=%d stderr=%q", code, stderr.String())
				}
				if _, err := os.Lstat(external); !os.IsNotExist(err) {
					t.Fatalf("owned external runtime remains: %v", err)
				}
				return
			}
			if code == 0 {
				t.Fatalf("force init code=%d stderr=%q, want ownership refusal", code, stderr.String())
			}
			if data, err := os.ReadFile(filepath.Join(external, "sentinel")); err != nil || string(data) != "owned" {
				t.Fatalf("unowned external runtime changed: data=%q err=%v", data, err)
			}
			after, err := os.ReadFile(configPath)
			if err != nil || string(after) != string(before) {
				t.Fatalf("config changed after ownership refusal: %v", err)
			}
		})
	}
}

func mustJSONString(t *testing.T, value string) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestInitForceResetsGlobalLegacyRuntime(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	dir := filepath.Join(home, defaultInstanceDirectoryName)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	legacyRoot := filepath.Join(dir, ".local", runtimeMarkerTestID)
	if err := os.MkdirAll(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyRoot, "sentinel"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--global", "--force"}, &stdout, &stderr, emptyEnv); code != 0 {
		t.Fatalf("global force init code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(filepath.Join(dir, ".local")); !os.IsNotExist(err) {
		t.Fatalf("global legacy runtime remains: %v", err)
	}
}

func TestInitForceResetsProjectLocalLegacyRuntime(t *testing.T) {
	project := t.TempDir()
	t.Chdir(project)
	dir := filepath.Join(project, defaultInstanceDirectoryName)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	legacyRoot := filepath.Join(dir, ".local", runtimeMarkerTestID)
	if err := os.MkdirAll(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyRoot, "sentinel"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, emptyEnv); code != 0 {
		t.Fatalf("project-local force init code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(filepath.Join(dir, ".local")); !os.IsNotExist(err) {
		t.Fatalf("project-local legacy runtime remains: %v", err)
	}
}

func TestInitForceRefusesAmbiguousMigrationHoldingState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	if err := ensureRuntimeOwnership(filepath.Join(dir, ".local"), runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	holding := filepath.Join(dir, obsoleteRuntimeMigrationName)
	if err := os.Mkdir(holding, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(holding, "unexpected"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code == 0 || !strings.Contains(stderr.String(), "unrecognized entry") {
		t.Fatalf("force init code=%d stderr=%q, want ambiguous-state refusal", code, stderr.String())
	}
	after, err := os.ReadFile(configPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("config changed after ambiguous-state refusal: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(holding, "unexpected")); err != nil || string(data) != "keep" {
		t.Fatalf("ambiguous holding state changed: data=%q err=%v", data, err)
	}
}

func TestInitForceRemovesRecognizedPromotedMigrationArtifact(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	localRoot := filepath.Join(dir, defaultRuntimeRootName)
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	// This is the interrupted post-promotion form: the active runtime already
	// has a direct marker while a UUID-named orphan remains in the old holding
	// directory without its own marker.
	orphan := filepath.Join(dir, obsoleteRuntimeMigrationName, runtimeMarkerOtherID)
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "sentinel"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code != 0 {
		t.Fatalf("force init code=%d stderr=%q", code, stderr.String())
	}
	for _, path := range []string{localRoot, filepath.Join(dir, obsoleteRuntimeMigrationName)} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("runtime artifact remains at %s: %v", path, err)
		}
	}
}

func TestInitForceRefusesTopLevelRuntimeSymlinkAndDoesNotFollowNestedSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating directory symlinks may require elevated Windows privileges")
	}
	t.Run("top-level", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "instance")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		configPath := filepath.Join(dir, instanceConfigFileName)
		writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
		target := t.TempDir()
		if err := os.WriteFile(filepath.Join(target, "sentinel"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, defaultRuntimeRootName)); err != nil {
			t.Skipf("create directory symlink: %v", err)
		}
		before, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code == 0 || !strings.Contains(stderr.String(), "runtime root is not a real directory") {
			t.Fatalf("force init code=%d stderr=%q, want symlink refusal", code, stderr.String())
		}
		after, err := os.ReadFile(configPath)
		if err != nil || string(after) != string(before) {
			t.Fatalf("config changed after symlink refusal: %v", err)
		}
		if data, err := os.ReadFile(filepath.Join(target, "sentinel")); err != nil || string(data) != "keep" {
			t.Fatalf("symlink target changed: data=%q err=%v", data, err)
		}
	})
	t.Run("nested", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "instance")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		writeConfigFixture(t, filepath.Join(dir, instanceConfigFileName), `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
		localRoot := filepath.Join(dir, defaultRuntimeRootName)
		if err := os.Mkdir(localRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		target := t.TempDir()
		if err := os.WriteFile(filepath.Join(target, "sentinel"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(localRoot, "linked-data")); err != nil {
			t.Skipf("create nested directory symlink: %v", err)
		}
		var stdout, stderr bytes.Buffer
		if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code != 0 {
			t.Fatalf("force init code=%d stderr=%q", code, stderr.String())
		}
		if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
			t.Fatalf("runtime root remains: %v", err)
		}
		if data, err := os.ReadFile(filepath.Join(target, "sentinel")); err != nil || string(data) != "keep" {
			t.Fatalf("nested symlink target changed: data=%q err=%v", data, err)
		}
	})
}

func TestRuntimeGuardSerializesForceResetAgainstDaemonStartup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	localRoot := filepath.Join(dir, ".local")
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localRoot, "sentinel"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	forceGuard, err := acquireRuntimeGuard(context.Background(), localRoot)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := inspectInitRuntimeReset(dir, configPath)
	if err != nil {
		forceGuard.Release()
		t.Fatalf("force reset ownership check: %v", err)
	}
	guardPath, err := runtimeGuardPath(localRoot)
	if err != nil {
		forceGuard.Release()
		t.Fatal(err)
	}
	if _, err := state.TryAcquireLock(context.Background(), guardPath); !errors.Is(err, state.ErrLockTimeout) {
		forceGuard.Release()
		t.Fatalf("startup guard probe during force reset = %v, want lock contention", err)
	}

	if err := plan.apply(); err != nil {
		forceGuard.Release()
		t.Fatalf("apply checked force reset: %v", err)
	}
	if err := writeInitConfig(configPath, []byte(`{"version":1,"id":"`+runtimeMarkerOtherID+`"}`), true); err != nil {
		forceGuard.Release()
		t.Fatalf("replace config ID: %v", err)
	}
	if err := forceGuard.Release(); err != nil {
		t.Fatal(err)
	}
	startupGuard, err := acquireRuntimeGuard(context.Background(), localRoot)
	if err != nil {
		t.Fatal(err)
	}
	validationErr := validateRunConfigIdentity(RunConfig{
		ConfigPath: configPath, StateDir: localRoot, InstanceID: runtimeMarkerTestID,
	})
	startupGuard.Release()
	if validationErr == nil || !strings.Contains(validationErr.Error(), "config changed before runtime startup") {
		t.Fatalf("stale startup validation error = %v", validationErr)
	}
	if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
		t.Fatalf("stale startup recreated the reset runtime root: %v", err)
	}
}

func TestRuntimeGuardUsesCanonicalInstancePath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating directory symlinks requires elevated privileges on Windows")
	}
	root := t.TempDir()
	instanceDir := filepath.Join(root, "instance")
	if err := os.Mkdir(instanceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "instance-alias")
	if err := os.Symlink(instanceDir, alias); err != nil {
		t.Fatal(err)
	}
	guard, err := acquireRuntimeGuard(context.Background(), instanceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()

	aliasGuardPath, err := runtimeGuardPath(alias)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.TryAcquireLock(context.Background(), aliasGuardPath); !errors.Is(err, state.ErrLockTimeout) {
		t.Fatalf("symlink alias guard probe = %v, want lock contention", err)
	}
}

func TestForceResetSharesGuardWithStartupForExternalRuntime(t *testing.T) {
	root := t.TempDir()
	configDirA := filepath.Join(root, "instance-a")
	configDirB := filepath.Join(root, "instance-b")
	sharedRuntime := filepath.Join(root, "shared-runtime")
	for _, dir := range []string{configDirA, configDirB} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(FileConfig{Version: 1, ID: runtimeMarkerTestID, LocalDir: sharedRuntime})
		if err != nil {
			t.Fatal(err)
		}
		writeConfigFixture(t, filepath.Join(dir, instanceConfigFileName), string(data))
	}
	if err := ensureRuntimeOwnership(sharedRuntime, runtimeMarkerTestID, true); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(sharedRuntime, "sentinel")
	if err := os.WriteFile(sentinel, []byte("shared runtime"), 0o600); err != nil {
		t.Fatal(err)
	}

	startupGuard, err := acquireRuntimeGuard(context.Background(), sharedRuntime)
	if err != nil {
		t.Fatal(err)
	}
	daemonLock, err := state.TryAcquireLock(context.Background(), filepath.Join(sharedRuntime, "run"))
	if err != nil {
		startupGuard.Release()
		t.Fatal(err)
	}
	defer daemonLock.Release()

	resetGuardRoot := initRuntimeResetGuardRoot(configDirA, filepath.Join(configDirA, instanceConfigFileName))
	if resetGuardRoot != sharedRuntime {
		startupGuard.Release()
		t.Fatalf("reset guard root = %q, want shared runtime %q", resetGuardRoot, sharedRuntime)
	}
	guardPath, err := runtimeGuardPath(resetGuardRoot)
	if err != nil {
		startupGuard.Release()
		t.Fatal(err)
	}
	if _, err := state.TryAcquireLock(context.Background(), guardPath); !errors.Is(err, state.ErrLockTimeout) {
		startupGuard.Release()
		t.Fatalf("force reset guard probe = %v, want startup lock contention", err)
	}
	if err := startupGuard.Release(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr strings.Builder
	if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": configDirA})); code == 0 || !strings.Contains(stderr.String(), "instance is running") {
		t.Fatalf("force reset code=%d stderr=%q, want active shared daemon refusal", code, stderr.String())
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "shared runtime" {
		t.Fatalf("force reset changed shared runtime: data=%q err=%v", data, err)
	}
	loaded, err := LoadFileConfig(configDirA, filepath.Join(configDirA, instanceConfigFileName), true)
	if err != nil || loaded.Config.ID != runtimeMarkerTestID {
		t.Fatalf("force reset changed instance A config: ID=%q err=%v", loaded.Config.ID, err)
	}
}

func TestInitForceReplacesMalformedConfigAndRemovesStandardRuntime(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "instance")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, instanceConfigFileName)
	external := filepath.Join(t.TempDir(), "external-runtime")
	if err := os.Mkdir(external, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(external, "sentinel"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"local_dir":`+mustJSONString(t, external)+`, invalid`), 0o600); err != nil {
		t.Fatal(err)
	}
	localRoot := filepath.Join(dir, ".local")
	if err := os.MkdirAll(localRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localRoot, "sentinel"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := executeInit([]string{"--force"}, &stdout, &stderr, mapEnv(map[string]string{"TICKET_ORC": dir})); code != 0 {
		t.Fatalf("force init code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
		t.Fatalf("standard runtime remains after malformed-config reset: %v", err)
	}
	if _, err := LoadFileConfig(dir, configPath, true); err != nil {
		t.Fatalf("replacement config is invalid: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(external, "sentinel")); err != nil || string(data) != "keep" {
		t.Fatalf("malformed config cleanup touched arbitrary external path: data=%q err=%v", data, err)
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
	writeConfigFixture(t, filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName), `{"version":1}`)
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
