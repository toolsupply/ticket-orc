package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func writeConfigFixture(t *testing.T, path, data string) {
	t.Helper()
	// Most pre-generic config tests exercise inheritance and worker validation;
	// give their built-in role fixtures the now-required Ticket queue policy fields.
	var root map[string]any
	if json.Unmarshal([]byte(data), &root) == nil {
		changed := false
		if root["id"] == nil {
			root["id"] = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
			changed = true
		}
		roles, ok := root["roles"].(map[string]any)
		if !ok && root["workers"] != nil {
			roles = map[string]any{}
			root["roles"] = roles
			changed = true
		}
		if ok || root["workers"] != nil {
			for name, queue := range map[string]string{"coder": "open", "reviewer": "review"} {
				raw, exists := roles[name].(map[string]any)
				if !exists {
					raw = map[string]any{}
					roles[name] = raw
					changed = true
				}
				if raw["ticket_queue"] == nil {
					raw["ticket_queue"] = queue
					changed = true
					if name == "coder" {
						raw["nudge_prompt"] = coderNudgePrompt
					} else {
						raw["nudge_prompt"] = reviewerNudgePrompt
					}
				}
			}
			if root["default_role"] == nil {
				root["default_role"] = "coder"
				changed = true
			}
		}
		if changed {
			if encoded, err := json.Marshal(root); err == nil {
				data = string(encoded)
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func testInstanceEnv(t *testing.T) envLookup {
	return testInstanceEnvWith(t, nil)
}

func testInstanceEnvWith(t *testing.T, values map[string]string) envLookup {
	t.Helper()
	dir := t.TempDir()
	writeConfigFixture(t, filepath.Join(dir, instanceConfigFileName), `{"version":1}`)
	env := map[string]string{"TICKET_ORC": dir}
	for name, value := range values {
		env[name] = value
	}
	return mapEnv(env)
}

func TestLoadFileConfigRequiresAnInstanceWhenNoPathIsSelected(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if _, err := LoadFileConfig(cwd, "", false); err == nil || !strings.Contains(err.Error(), "no Orc instance found") {
		t.Fatalf("missing instance error = %v", err)
	}
}

func TestResolveInstanceDirectoryPrefersProjectAndPinsSelectedFailures(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	localDir := filepath.Join(cwd, defaultInstanceDirectoryName)
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	writeConfigFixture(t, filepath.Join(localDir, instanceConfigFileName), `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	writeConfigFixture(t, filepath.Join(globalDir, instanceConfigFileName), `{"version":1,"id":"2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	loaded, err := LoadFileConfig(cwd, "", false)
	if err != nil || loaded.Instance.InstanceDir != localDir || loaded.Config.ID != "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa" {
		t.Fatalf("selected instance=%#v err=%v, want project local %s", loaded.Instance, err, localDir)
	}
	if err := os.WriteFile(filepath.Join(localDir, instanceConfigFileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFileConfig(cwd, "", false); err == nil || !strings.Contains(err.Error(), localDir) || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("invalid selected local config fell back or returned an unclear error: %v", err)
	}
}

func TestResolveInstanceDirectoryUsesGlobalWhenNoConfiguredLocalInstance(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	writeConfigFixture(t, filepath.Join(globalDir, instanceConfigFileName), `{"version":1,"id":"2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	loaded, err := LoadFileConfig(cwd, "", false)
	if err != nil || loaded.Instance.InstanceDir != globalDir {
		t.Fatalf("selected instance=%#v err=%v, want global %s", loaded.Instance, err, globalDir)
	}
}

func TestJoinConfigDiscoveryIgnoresLocalRuntimeArtifactsWithoutConfig(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Chdir(cwd)

	localDir := filepath.Join(cwd, defaultInstanceDirectoryName)
	for _, name := range []string{"run", "workers"} {
		if err := os.MkdirAll(filepath.Join(localDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	globalConfig := filepath.Join(globalDir, instanceConfigFileName)
	writeConfigFixture(t, globalConfig, `{"version":1,"id":"2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)

	loaded, err := loadSteerConfig(emptyEnv)
	if err != nil {
		t.Fatalf("join config discovery: %v", err)
	}
	if loaded.Instance.InstanceDir != globalDir || loaded.Instance.ConfigPath != globalConfig {
		t.Fatalf("join selected instance=%#v, want home instance %s", loaded.Instance, globalDir)
	}
}

func TestExplicitInstanceDiscoveryDoesNotFallBackToHome(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Chdir(cwd)
	globalConfig := filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName)
	writeConfigFixture(t, globalConfig, `{"version":1}`)

	explicitInstance := filepath.Join(cwd, "selected-instance")
	if err := os.Mkdir(explicitInstance, 0o700); err != nil {
		t.Fatal(err)
	}
	explicitConfig := filepath.Join(cwd, "selected-config", instanceConfigFileName)
	for _, test := range []struct {
		name           string
		lookup         envLookup
		path           string
		instanceSelect bool
		invalid        bool
	}{
		{name: "TICKET_ORC missing", lookup: mapEnv(map[string]string{"TICKET_ORC": explicitInstance}), path: filepath.Join(explicitInstance, instanceConfigFileName), instanceSelect: true},
		{name: "TICKET_ORC invalid", lookup: mapEnv(map[string]string{"TICKET_ORC": explicitInstance}), path: filepath.Join(explicitInstance, instanceConfigFileName), instanceSelect: true, invalid: true},
		{name: "explicit config missing", lookup: emptyEnv, path: explicitConfig},
		{name: "explicit config invalid", lookup: emptyEnv, path: explicitConfig, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.invalid {
				if err := os.MkdirAll(filepath.Dir(test.path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(test.path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if test.instanceSelect {
				_, err = loadSteerConfig(test.lookup)
			} else {
				_, err = loadSteerConfigPath(test.path, test.lookup)
			}
			wantError := "config file not found"
			if test.invalid {
				wantError = "rejected"
			}
			if err == nil || !strings.Contains(err.Error(), test.path) || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("explicit selection error = %v, want %q for %s", err, wantError, test.path)
			}
		})
	}
}

func TestInstanceDiscoveryRejectsNonRegularLocalConfig(t *testing.T) {
	for _, test := range []struct {
		name      string
		makeEntry func(t *testing.T, configPath, globalConfig string)
	}{
		{
			name: "directory",
			makeEntry: func(t *testing.T, configPath, _ string) {
				if err := os.Mkdir(configPath, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			makeEntry: func(t *testing.T, configPath, globalConfig string) {
				if runtime.GOOS == "windows" {
					t.Skip("symlink creation may require elevated Windows privileges")
				}
				if err := os.Symlink(globalConfig, configPath); err != nil {
					t.Skipf("symlink creation unavailable: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cwd := t.TempDir()
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("USERPROFILE", home)
			t.Chdir(cwd)
			globalConfig := filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName)
			writeConfigFixture(t, globalConfig, `{"version":1}`)
			localConfig := filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName)
			if err := os.MkdirAll(filepath.Dir(localConfig), 0o700); err != nil {
				t.Fatal(err)
			}
			test.makeEntry(t, localConfig, globalConfig)
			if _, err := loadSteerConfig(emptyEnv); err == nil || !strings.Contains(err.Error(), "instance config is not a regular file: "+localConfig) {
				t.Fatalf("non-regular local config error = %v, want safe rejection for %s", err, localConfig)
			}
		})
	}
}

func TestResolveInstanceDirectoryDoesNotSearchAncestors(t *testing.T) {
	root := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	parent := filepath.Join(root, "parent")
	if err := os.MkdirAll(filepath.Join(parent, defaultInstanceDirectoryName), 0o700); err != nil {
		t.Fatal(err)
	}
	cwd := filepath.Join(parent, "nested")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveInstanceDir(cwd, emptyEnv); err == nil || !strings.Contains(err.Error(), "no Orc instance found") {
		t.Fatalf("ancestor instance was selected: %v", err)
	}
}

func TestCommandsReportMissingInstanceConsistently(t *testing.T) {
	cwd := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Chdir(cwd)
	tests := []struct {
		name string
		load func() error
	}{
		{name: "role", load: func() error { _, _, err := parseRoleConfig(RoleCoder, nil, emptyEnv); return err }},
		{name: "run", load: func() error { _, _, err := parseRunConfig(nil, emptyEnv); return err }},
		{name: "state", load: func() error { _, _, err := parseStateConfig(nil, emptyEnv); return err }},
		{name: "daemon", load: func() error { _, _, _, err := parseDaemonCommandOptions(nil, emptyEnv, false); return err }},
		{name: "steer", load: func() error { _, err := loadSteerConfig(emptyEnv); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.load(); err == nil || !strings.Contains(err.Error(), "no Orc instance found") {
				t.Fatalf("missing-instance error = %v", err)
			}
		})
	}
}

func TestLoadFileConfigPreservesResolvedImplicitDefault(t *testing.T) {
	cwd := t.TempDir()
	path := filepath.Join(cwd, "selected-instance", instanceConfigFileName)
	writeConfigFixture(t, path, `{"version":1}`)
	loaded, err := LoadFileConfig(filepath.Dir(path), path, false)
	if err != nil {
		t.Fatalf("load resolved default: %v", err)
	}
	if loaded.Instance.ConfigPath != path || loaded.Instance.Explicit || loaded.Config.Version != 1 {
		t.Fatalf("loaded = %#v, want path %q and implicit config", loaded, path)
	}
	want := filepath.Join(filepath.Dir(path), ".local")
	if loaded.Instance.LocalDir != want || loaded.Config.LocalDir != want {
		t.Fatalf("omitted local_dir = %q (config %q), want %q", loaded.Instance.LocalDir, loaded.Config.LocalDir, want)
	}
}

func TestLoadFileConfigPreservesArbitraryExplicitFilename(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "managed-team.json")
	writeConfigFixture(t, path, `{"version":1,"id":"3e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	loaded, err := loadInvocationConfig(map[string]string{"config": path}, emptyEnv)
	if err != nil || loaded.Instance.ConfigPath != path || loaded.Instance.InstanceDir != root || !loaded.Instance.Explicit {
		t.Fatalf("explicit config instance=%#v err=%v", loaded.Instance, err)
	}
}

func TestLoadFileConfigOmittedLocalDirUsesOneConfigDirectoryRoot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, defaultInstanceDirectoryName, instanceConfigFileName)
	writeConfigFixture(t, path, `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, defaultInstanceDirectoryName, ".local")
	if loaded.Instance.LocalDir != want {
		t.Fatalf("omitted local_dir = %q, want %q", loaded.Instance.LocalDir, want)
	}
	otherPath := filepath.Join(root, defaultInstanceDirectoryName, "other.json")
	writeConfigFixture(t, otherPath, `{"version":1,"id":"2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"}`)
	other, err := LoadFileConfig(root, otherPath, true)
	if err != nil || other.Instance.LocalDir != want {
		t.Fatalf("second config default root = %q, err=%v, want shared root %q", other.Instance.LocalDir, err, want)
	}
}

func TestLoadFileConfigLocalDirResolutionIsIndependentOfID(t *testing.T) {
	root := t.TempDir()
	firstPath := filepath.Join(root, "a.json")
	secondPath := filepath.Join(root, "b.json")
	firstID := "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	secondID := "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	writeConfigFixture(t, firstPath, fmt.Sprintf(`{"version":1,"id":%q}`, firstID))
	writeConfigFixture(t, secondPath, fmt.Sprintf(`{"version":1,"id":%q}`, secondID))
	first, err := LoadFileConfig(t.TempDir(), firstPath, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadFileConfig(t.TempDir(), secondPath, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Instance.LocalDir != filepath.Join(root, ".local") || second.Instance.LocalDir != first.Instance.LocalDir {
		t.Fatalf("local roots = %q, %q", first.Instance.LocalDir, second.Instance.LocalDir)
	}
	writeConfigFixture(t, secondPath, fmt.Sprintf(`{"version":1,"id":%q}`, firstID))
	sameID, err := LoadFileConfig(t.TempDir(), secondPath, true)
	if err != nil || sameID.Instance.LocalDir != first.Instance.LocalDir {
		t.Fatalf("same ID local root = %q, want %q; err=%v", sameID.Instance.LocalDir, first.Instance.LocalDir, err)
	}
	newID := "3e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	writeConfigFixture(t, secondPath, fmt.Sprintf(`{"version":1,"id":%q}`, newID))
	changedID, err := LoadFileConfig(t.TempDir(), secondPath, true)
	if err != nil || changedID.Instance.LocalDir != filepath.Join(root, ".local") {
		t.Fatalf("changed ID local root = %q, err=%v", changedID.Instance.LocalDir, err)
	}

	absolute := filepath.Join(t.TempDir(), "exact-root")
	writeConfigFixture(t, firstPath, fmt.Sprintf(`{"version":1,"id":%q,"local_dir":%q}`, firstID, absolute))
	loaded, err := LoadFileConfig(t.TempDir(), firstPath, true)
	if err != nil || loaded.Instance.LocalDir != absolute {
		t.Fatalf("absolute local root = %q, err=%v; want %q", loaded.Instance.LocalDir, err, absolute)
	}
	writeConfigFixture(t, firstPath, fmt.Sprintf(`{"version":1,"id":%q,"local_dir":"../runtime"}`, firstID))
	loaded, err = LoadFileConfig(t.TempDir(), firstPath, true)
	want := filepath.Join(filepath.Dir(root), "runtime")
	if err != nil || loaded.Instance.LocalDir != want {
		t.Fatalf("relative local root = %q, err=%v; want %q", loaded.Instance.LocalDir, err, want)
	}
}

func TestFinalLocalDirSymlinkIsRejectedByConfigCheckAndRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	target := filepath.Join(root, "real-runtime")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "local-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	writeConfigFixture(t, configPath, `{"version":1,"local_dir":"local-link"}`)

	loaded, err := LoadFileConfig(root, configPath, true)
	if err == nil || !strings.Contains(err.Error(), "local_dir must name a directory, not a symlink: "+link) {
		t.Fatalf("load symlinked local_dir = %#v, error=%v", loaded, err)
	}
	for _, args := range [][]string{{"config", "check", "--config", configPath}, {"run", "--config", configPath}} {
		var stdout, stderr strings.Builder
		code := run(args, &stdout, &stderr, emptyEnv, rejectExecution)
		if code == 0 || !strings.Contains(stderr.String(), "local_dir must name a directory, not a symlink") {
			t.Fatalf("%v code=%d stdout=%q stderr=%q, want config validation failure for symlinked local_dir", args, code, stdout.String(), stderr.String())
		}
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Fatalf("config check or run followed the final local_dir symlink: entries=%v error=%v", entries, err)
	}
}

func TestLoadFileConfigRequiresUUIDAndUsableLocalDir(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		want string
	}{
		{name: "missing ID", data: `{"version":1}`, want: "id must be a UUIDv4"},
		{name: "invalid ID", data: `{"version":1,"id":"unstable"}`, want: "id must be a UUIDv4"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("load error = %v, want %q", err, test.want)
			}
		})
	}
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"local_dir":%q}`, file))
	if _, err := LoadFileConfig(root, path, true); err == nil || !strings.Contains(err.Error(), "local directory "+file+" is not a directory") {
		t.Fatalf("unusable local root error = %v", err)
	}
}

func TestLoadFileConfigMissingExplicitFails(t *testing.T) {
	_, err := LoadFileConfig(t.TempDir(), "missing.json", true)
	if err == nil || !strings.Contains(err.Error(), "config file not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadFileConfigStrictJSONAndFinalSchema(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{"malformed", `{"version":`, "rejected"},
		{"unknown root", `{"version":1,"common":{}}`, "unknown field"},
		{"duplicate", `{"version":1,"version":1}`, "duplicate key"},
		{"trailing", `{"version":1} {}`, "trailing content"},
		{"version", `{"version":2}`, "unsupported version"},
		{"unknown worker field", `{"version":1,"workers":{"one":{"role":"coder","future":true}}}`, "unknown field"},
		{"worker ticket tags override", `{"version":1,"workers":{"one":{"role":"coder","ticket_tags":["backend"]}}}`, "unknown field"},
		{"default ticket tags override", `{"version":1,"defaults":{"ticket_tags":["backend"]}}`, "unknown field"},
		{"removed defaults state_dir", `{"version":1,"defaults":{"state_dir":"state"}}`, "unknown field at defaults.state_dir"},
		{"removed role state_dir", `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"work","state_dir":"state"}}}`, "unknown field at roles.coder.state_dir"},
		{"removed worker state_dir", `{"version":1,"workers":{"one":{"role":"coder","state_dir":"state"}}}`, "unknown field at workers.one.state_dir"},
		{"old role queue field", `{"version":1,"default_role":"coder","roles":{"coder":{"queue":"open","nudge_prompt":"work"}}}`, "unknown field"},
		{"old role prompt field", `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","queue_prompt":"work"}}}`, "unknown field"},
		{"removed queue scope", `{"version":1,"workers":{"one":{"role":"coder","queue_scope":"role"}}}`, "unknown field"},
		{"unknown review field", `{"version":1,"review":{"skip_tags":["trivial"],"close":true}}`, "unknown field"},
		{"uppercase review tag", `{"version":1,"review":{"skip_tags":["No-Review"]}}`, "non-canonical"},
		{"duplicate review tag", `{"version":1,"review":{"skip_tags":["trivial","trivial"]}}`, "duplicate"},
		{"invalid review tag", `{"version":1,"review":{"skip_tags":["needs review"]}}`, "invalid Ticket tag"},
		{"invalid role tag punctuation", `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"work","ticket_tags":["to:team-a"]}}}`, "invalid Ticket tag"},
		{"contradictory review tag", `{"version":1,"default_role":"reviewer","review":{"skip_tags":["no-review"]},"roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"work","ticket_tags":["no-review"]}}}`, "excluded by review.skip_tags"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if test.name == "duplicate" {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				writeConfigFixture(t, path, test.data)
			}
			_, err := LoadFileConfig(filepath.Dir(path), path, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRoleTicketTagsParsingAndResolvedCanonicalization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"work","ticket_tags":["urgent","backend"]},"reviewer":{"ticket_queue":"review","nudge_prompt":"review"}},"review":{"skip_tags":["no-review"]}}`)
	loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if !reflect.DeepEqual(loaded.Config.Roles["coder"].TicketTags, []string{"urgent", "backend"}) {
		t.Fatalf("parsed ticket_tags = %#v", loaded.Config.Roles["coder"].TicketTags)
	}
	role := loaded.Config.Roles["coder"]
	originalTags := append([]string(nil), role.TicketTags...)
	filters, err := resolvedRoleQueueFilters(role, loaded.Config.Review)
	if err != nil {
		t.Fatal(err)
	}
	want := ticketclient.QueueFilters{Tags: []string{"backend", "urgent"}}
	if !reflect.DeepEqual(filters, want) {
		t.Fatalf("open role filters = %#v, want %#v", filters, want)
	}
	if !reflect.DeepEqual(role.TicketTags, originalTags) {
		t.Fatalf("selector resolution mutated parsed config: %#v", role.TicketTags)
	}
	role.TicketTags = []string{"urgent", "backend"}
	first, _ := resolvedRoleQueueFilters(role, loaded.Config.Review)
	role.TicketTags = []string{"backend", "urgent"}
	second, _ := resolvedRoleQueueFilters(role, loaded.Config.Review)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("reordered selectors differ: %#v vs %#v", first, second)
	}
	base := supervisor.RunWorker{Name: "worker", Config: supervisor.RoleConfig{
		Role: RoleCoder, RoleName: "coder", TicketQueue: "open", TicketTags: strings.Join(first.Tags, "\x1f"),
	}}
	reordered := base
	reordered.Config.TicketTags = strings.Join(second.Tags, "\x1f")
	if !equalLaunchSnapshots(base, reordered) {
		t.Fatal("reordered selectors should not require a worker restart")
	}
	reviewer := loaded.Config.Roles["reviewer"]
	reviewFilters, err := resolvedRoleQueueFilters(reviewer, loaded.Config.Review)
	if err != nil || !reflect.DeepEqual(reviewFilters, ticketclient.QueueFilters{WithoutTags: []string{"no-review"}}) {
		t.Fatalf("review filters = %#v, %v", reviewFilters, err)
	}
}

func TestTicketTagListBoundaries(t *testing.T) {
	valid := make([]string, 64)
	for i := range valid {
		valid[i] = fmt.Sprintf("tag-%02d", i)
	}
	valid[0] = strings.Repeat("a", 64)
	if err := validateTicketTagList("roles.coder.ticket_tags", valid); err != nil {
		t.Fatalf("valid boundary tags: %v", err)
	}
	tooMany := append(append([]string(nil), valid...), "extra")
	if err := validateTicketTagList("roles.coder.ticket_tags", tooMany); err == nil {
		t.Fatal("accepted more than 64 tags")
	}
	if err := validateTicketTagList("roles.coder.ticket_tags", []string{"duplicate", "duplicate"}); err == nil {
		t.Fatal("accepted duplicate tags")
	}
	for _, invalid := range []string{"Uppercase", "", "has space", "to:team-a", strings.Repeat("a", 65)} {
		if err := validateTicketTagList("roles.coder.ticket_tags", []string{invalid}); err == nil {
			t.Errorf("accepted invalid tag %q", invalid)
		}
	}
}

func TestRoleTicketTagsOmittedAndEmptyResolveUnrestricted(t *testing.T) {
	var snapshots []supervisor.RunWorker
	for _, field := range []string{"", `,"ticket_tags":[]`} {
		path := filepath.Join(t.TempDir(), "config.json")
		writeConfigFixture(t, path, `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"work"`+field+`}},"workers":{"worker":{"role":"coder","actor":"worker"}}}`)
		config, help, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "worker"}, emptyEnv)
		if help || err != nil {
			t.Fatalf("parseRoleConfig for %q: help=%v err=%v", field, help, err)
		}
		if config.TicketTags != "" {
			t.Fatalf("ticket tags for %q = %q, want unrestricted", field, config.TicketTags)
		}
		if config.RoleName != "coder" || config.TicketQueue != "open" {
			t.Fatalf("effective configured role policy for %q = %q/%q, want coder/open", field, config.RoleName, config.TicketQueue)
		}
		snapshots = append(snapshots, supervisor.RunWorker{Name: "worker", Config: supervisor.RoleConfig{
			Role: config.Role, RoleName: config.RoleName, TicketQueue: config.TicketQueue,
			TicketTags: config.TicketTags, ReviewSkipTags: config.ReviewSkipTags,
		}})
	}
	if len(snapshots) != 2 || !equalLaunchSnapshots(snapshots[0], snapshots[1]) {
		t.Fatalf("omitted and empty selector launch snapshots differ: %#v", snapshots)
	}
}

func TestLoadFileConfigValidatesRoleTicketQueueAndNudgePrompt(t *testing.T) {
	tests := []struct {
		name string
		role string
		want string
	}{
		{name: "invalid ticket queue", role: `"ticket_queue":"closed","nudge_prompt":"work"`, want: "roles.coder.ticket_queue must be open or review"},
		{name: "empty nudge prompt", role: `"ticket_queue":"open","nudge_prompt":"  "`, want: "roles.coder.nudge_prompt must not be empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, `{"version":1,"default_role":"coder","roles":{"coder":{`+test.role+`}}}`)
			if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadFileConfigReviewSkipTags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"review":{"skip_tags":["trivial","no-review"]}}`)
	loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if !reflect.DeepEqual(loaded.Config.Review.SkipTags, []string{"trivial", "no-review"}) {
		t.Fatalf("review skip tags = %#v", loaded.Config.Review.SkipTags)
	}
}

func TestLoadFileConfigWorkersAndGroups(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "config.json")
	writeConfigFixture(t, path, `{"version":1,"roles":{"coder":{"groups":["role-default","shared"]}},"workers":{"worker-a":{"role":"coder","groups":["default","backend"]}},"supervisor":{"startup_groups":["default"]}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if got := loaded.Config.Roles["coder"].Groups; !reflect.DeepEqual(got, []string{"role-default", "shared"}) {
		t.Fatalf("role groups = %#v", got)
	}
}

func TestLoadFileConfigSupervisorListenSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"supervisor":{"listen":"127.0.0.2","port":43123}}`)
	loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if loaded.Config.Supervisor.ListenAddress != "127.0.0.2" || loaded.Config.Supervisor.Port == nil || *loaded.Config.Supervisor.Port != 43123 {
		t.Fatalf("supervisor = %#v", loaded.Config.Supervisor)
	}
	for name, data := range map[string]string{
		"hostname":             `{"version":1,"supervisor":{"listen":"localhost"}}`,
		"listen-address-alias": `{"version":1,"supervisor":{"listen_address":"127.0.0.2"}}`,
		"privileged":           `{"version":1,"supervisor":{"port":80}}`,
		"invalid-port":         `{"version":1,"supervisor":{"port":65536}}`,
	} {
		t.Run(name, func(t *testing.T) {
			candidate := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, candidate, data)
			if _, err := LoadFileConfig(filepath.Dir(candidate), candidate, true); err == nil {
				t.Fatal("invalid supervisor listen settings accepted")
			}
		})
	}
}

func TestLoadFileConfigNormalizesWorkingDirectories(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "config.json")
	configDir := filepath.Dir(path)
	defaultsDir := filepath.Join(root, "defaults-repo")
	roleDir := filepath.Join(root, "role-repo")
	workerDir := filepath.Join(root, "worker-repo")
	for _, directory := range []string{defaultsDir, roleDir, workerDir} {
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"defaults":{"working_dir":"../defaults-repo","repository":"../default-tickets"},"roles":{"coder":{"working_dir":"../role-repo","repository":"../role-tickets"}},"workers":{"worker-a":{"role":"coder","working_dir":"../worker-repo","repository":"../worker-tickets"}}}`))
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if loaded.Config.Defaults.WorkingDir != defaultsDir || loaded.Config.Roles["coder"].WorkingDir != roleDir || loaded.Config.Workers["worker-a"].WorkingDir != workerDir {
		t.Fatalf("working directories = %#v %#v %#v, want %q %q %q", loaded.Config.Defaults.WorkingDir, loaded.Config.Roles["coder"].WorkingDir, loaded.Config.Workers["worker-a"].WorkingDir, defaultsDir, roleDir, workerDir)
	}
	for name, got := range map[string]string{
		"defaults": loaded.Config.Defaults.Repository,
		"role":     loaded.Config.Roles["coder"].Repository,
		"worker":   loaded.Config.Workers["worker-a"].Repository,
	} {
		want := filepath.Join(configDir, map[string]string{"defaults": "../default-tickets", "role": "../role-tickets", "worker": "../worker-tickets"}[name])
		if got != want {
			t.Errorf("%s repository = %q, want %q", name, got, want)
		}
	}
}

func TestLoadFileConfigResolvesTicketTargetPathsAndInheritance(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "config.json")
	writeConfigFixture(t, path, `{"version":1,"defaults":{"ticket":{"config":"../ticket.json"}},"roles":{"coder":{"ticket":{"scope":"role-scope"}}},"workers":{"coder":{"role":"coder","ticket":{"scope":"worker-scope"}}}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if got, want := loaded.Config.Defaults.Ticket.Config, filepath.Join(filepath.Dir(path), "..", "ticket.json"); got != filepath.Clean(want) {
		t.Fatalf("default Ticket config = %q, want %q", got, filepath.Clean(want))
	}
	if got := loaded.Config.Workers["coder"].Ticket.Scope; got != "worker-scope" {
		t.Fatalf("worker Ticket scope = %q", got)
	}
	resolved, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder", "--actor", "coder"}, emptyEnv)
	if err != nil {
		t.Fatalf("parseRoleConfig: %v", err)
	}
	if resolved.Ticket.Mode != TicketTargetScoped || resolved.Ticket.Config != filepath.Clean(filepath.Join(filepath.Dir(path), "..", "ticket.json")) || resolved.Ticket.Scope != "worker-scope" {
		t.Fatalf("resolved Ticket target = %#v", resolved.Ticket)
	}
}

func TestLoadFileConfigResolvesRepositoryRegistryAndWorkerKey(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"repositories":{"orc":{"repository":"tickets"},"ui":{"ticket":{"config":"ticket.json","scope":"ui"}}},"workers":{"coder":{"role":"coder","repository":"orc","actor":"coder"}}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if got, want := loaded.Config.Repositories["orc"].Repository, filepath.Join(root, "tickets"); got != want {
		t.Fatalf("repository target = %q, want %q", got, want)
	}
	if got, want := loaded.Config.Repositories["ui"].Ticket.Config, filepath.Join(root, "ticket.json"); got != want {
		t.Fatalf("scoped config = %q, want %q", got, want)
	}
	resolved, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
	if err != nil {
		t.Fatalf("parseRoleConfig: %v", err)
	}
	if resolved.RepositoryKey != "orc" || resolved.Ticket.Repository != filepath.Join(root, "tickets") {
		t.Fatalf("resolved repository = %#v target=%#v", resolved.RepositoryKey, resolved.Ticket)
	}
}

func TestLoadFileConfigResolvesDefaultsRepositoryAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"defaults":{"repository":"orc"},"repositories":{"orc":{"repository":"tickets"}},"workers":{"coder":{"role":"coder","actor":"coder"}}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if loaded.Config.Defaults.Repository != "orc" {
		t.Fatalf("default repository alias = %q, want orc", loaded.Config.Defaults.Repository)
	}
	resolved, _, err := parseRoleConfig(RoleCoder, []string{"--config", path, "--worker", "coder"}, emptyEnv)
	if err != nil {
		t.Fatalf("parseRoleConfig: %v", err)
	}
	if resolved.RepositoryKey != "orc" || resolved.Ticket.Repository != filepath.Join(root, "tickets") {
		t.Fatalf("resolved default repository alias = %#v, want key orc and target %q", resolved, filepath.Join(root, "tickets"))
	}
}

func TestLoadFileConfigRejectsWorkerTargetOutsideRepositoryRegistry(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"repositories":{"orc":{"repository":"tickets"}},"workers":{"coder":{"role":"coder","repository":"missing","actor":"coder"}}}`)
	if _, err := LoadFileConfig(root, path, true); err == nil || !strings.Contains(err.Error(), "unknown repository") {
		t.Fatalf("error = %v, want unknown repository", err)
	}
}

func TestLoadFileConfigAllowsZeroWorkerRepositories(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	writeConfigFixture(t, path, `{"version":1,"repositories":{"ui":{"repository":"tickets"}}}`)
	loaded, err := LoadFileConfig(root, path, true)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if len(loaded.Config.Repositories) != 1 || len(loaded.Config.Workers) != 0 {
		t.Fatalf("loaded repositories/workers = %d/%d", len(loaded.Config.Repositories), len(loaded.Config.Workers))
	}
}

func TestLoadFileConfigRejectsIncompleteAndMixedTicketTargets(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{"incomplete config", `{"version":1,"workers":{"one":{"role":"coder","ticket":{"config":"ticket.json"}}}}`, "ticket.target_incomplete"},
		{"incomplete scope", `{"version":1,"workers":{"one":{"role":"coder","ticket":{"scope":"one"}}}}`, "ticket.target_incomplete"},
		{"mixed target", `{"version":1,"workers":{"one":{"role":"coder","repository":"tickets","ticket":{"config":"ticket.json","scope":"one"}}}}`, "ticket.target_mixed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, test.data)
			if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadFileConfigRejectsUnknownTicketFieldWithSuggestion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"one":{"role":"coder","ticket":{"scop":"one"}}}}`)
	if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), "workers.one.ticket.scop") || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("error = %v, want nested Ticket typo diagnostic", err)
	}
}

func TestLoadFileConfigRejectsInvalidWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"missing": "missing", "file": "file"} {
		path := filepath.Join(root, name+".json")
		writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"defaults":{"working_dir":%q}}`, value))
		if _, err := LoadFileConfig(root, path, true); err == nil || !strings.Contains(err.Error(), "working_dir") {
			t.Fatalf("%s working_dir error = %v", name, err)
		}
	}
}

func TestLoadFileConfigRejectsWorkerStructureErrors(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{"missing role", `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"Coding."}},"workers":{"one":{}}}`, "must reference a configured role"},
		{"unknown role", `{"version":1,"workers":{"one":{"role":"admin"}}}`, "unknown role"},
		{"empty group", `{"version":1,"workers":{"one":{"role":"coder","groups":[" "]}}}`, "invalid group"},
		{"duplicate group", `{"version":1,"workers":{"one":{"role":"coder","groups":["default","default"]}}}`, "duplicate group"},
		{"role empty group", `{"version":1,"roles":{"coder":{"groups":[" "]}}}`, "invalid group"},
		{"role duplicate group", `{"version":1,"roles":{"coder":{"groups":["default","default"]}}}`, "duplicate group"},
		{"bad name", `{"version":1,"workers":{" one ":{"role":"coder"}}}`, "worker name"},
		{"worker path separator", `{"version":1,"workers":{"one/two":{"role":"coder"}}}`, "ASCII letters"},
		{"group path separator", `{"version":1,"workers":{"one":{"role":"coder","groups":["default/api"]}}}`, "invalid group"},
		{"worker space", `{"version":1,"workers":{"Worker Name":{"role":"coder"}}}`, "ASCII letters"},
		{"worker unicode", `{"version":1,"workers":{"worker-a-é":{"role":"coder"}}}`, "ASCII letters"},
		{"worker dot segment", `{"version":1,"workers":{".":{"role":"coder"}}}`, "ASCII letters"},
		{"worker parent dot segment", `{"version":1,"workers":{"..":{"role":"coder"}}}`, "ASCII letters"},
		{"group dot segment", `{"version":1,"workers":{"one":{"role":"coder","groups":["."]}}}`, "invalid group"},
		{"group parent dot segment", `{"version":1,"workers":{"one":{"role":"coder","groups":[".."]}}}`, "invalid group"},
		{"wrong namespace", `{"version":1,"workers":{"one":{"role":"coder","harness":"pi","codex":{"sandbox":"workspace-write"}}}}`, "codex settings"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, test.data)
			_, err := LoadFileConfig(filepath.Dir(path), path, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadFileConfigAllowsEmptyWorkers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{}}`)
	loaded, err := LoadFileConfig(filepath.Dir(path), path, true)
	if err != nil || len(loaded.Config.Workers) != 0 {
		t.Fatalf("loaded = %#v, error = %v", loaded, err)
	}
}

func TestWorkerAndGroupIdentifierLengthLimit(t *testing.T) {
	valid := strings.Repeat("a", maxIdentifierBytes)
	if err := validateWorkerName(valid); err != nil {
		t.Fatalf("maximum worker name rejected: %v", err)
	}
	if err := validateGroups("groups", []string{valid}); err != nil {
		t.Fatalf("maximum group name rejected: %v", err)
	}
	tooLong := valid + "a"
	if err := validateWorkerName(tooLong); err == nil {
		t.Fatal("overlong worker name accepted")
	}
	if err := validateGroups("groups", []string{tooLong}); err == nil {
		t.Fatal("overlong group name accepted")
	}
}

func TestLoadFileConfigValidatesRequiredSkills(t *testing.T) {
	tests := []struct {
		name string
		list string
		want string
	}{
		{"empty", `[""]`, "invalid skill"},
		{"whitespace", `[" ticket"]`, "invalid skill"},
		{"control", `["ticket\\nworker"]`, "invalid skill"},
		{"duplicate", `["ticket","ticket"]`, "duplicate skill"},
		{"unsafe", `["ticket/orc"]`, "invalid skill"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"coder","required_skills":`+test.list+`}}}`)
			_, err := LoadFileConfig(filepath.Dir(path), path, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
	for _, scope := range []struct {
		name string
		data string
	}{
		{"defaults", `{"version":1,"defaults":{"required_skills":["ticket","ticket"]}}`},
		{"role", `{"version":1,"roles":{"coder":{"required_skills":["ticket/orc"]}}}`},
	} {
		t.Run(scope.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeConfigFixture(t, path, scope.data)
			if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), "skill") {
				t.Fatalf("error = %v, want required skill validation", err)
			}
		})
	}
	tooMany := make([]string, maxRequiredSkills+1)
	for i := range tooMany {
		tooMany[i] = "skill-" + strconv.Itoa(i)
	}
	data, _ := json.Marshal(tooMany)
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfigFixture(t, path, `{"version":1,"workers":{"coder":{"role":"coder","actor":"coder","required_skills":`+string(data)+`}}}`)
	if _, err := LoadFileConfig(filepath.Dir(path), path, true); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("too many skills error = %v", err)
	}
}

func TestEffectiveRequiredSkillsMergeScopesAndBounds(t *testing.T) {
	config := FileConfig{
		ID:          "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
		DefaultRole: "coder",
		Defaults:    DefaultsFileConfig{RequiredSkills: []string{"ticket", "common"}},
		Roles:       map[string]RoleFileConfig{"coder": {TicketQueue: "open", NudgePrompt: coderNudgePrompt, RequiredSkills: []string{"common", "worker"}}, "reviewer": {TicketQueue: "review", NudgePrompt: reviewerNudgePrompt}},
		Workers:     map[string]WorkerFileConfig{"coder": {Role: "coder", RequiredSkills: []string{"worker", "repository"}}},
	}
	if got, want := effectiveWorkerRequiredSkills(config, "coder"), []string{"ticket", "common", "worker", "repository"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective skills = %#v, want %#v", got, want)
	}
	tooMany := make([]string, maxRequiredSkills)
	for i := range tooMany {
		tooMany[i] = "skill-" + strconv.Itoa(i)
	}
	config.Defaults.RequiredSkills = tooMany
	config.Roles["coder"] = RoleFileConfig{TicketQueue: "open", NudgePrompt: coderNudgePrompt, RequiredSkills: []string{"role-skill"}}
	if err := validateAndNormalizeFileConfig(&config, t.TempDir()); err == nil || !strings.Contains(err.Error(), "effective required_skills") {
		t.Fatalf("effective skill bound error = %v", err)
	}
}

func TestWorkerPolicyUsesOnlyExplicitWorkerRole(t *testing.T) {
	config := FileConfig{
		DefaultRole: "reviewer",
		Roles: map[string]RoleFileConfig{
			"coder":    {TicketQueue: "open", NudgePrompt: "Code.", RequiredSkills: []string{"code"}},
			"reviewer": {TicketQueue: "review", NudgePrompt: "Review.", RequiredSkills: []string{"review"}},
		},
	}
	if got := workerPolicy(config, WorkerFileConfig{Role: "coder"}); got.NudgePrompt != "Code." || !reflect.DeepEqual(got.RequiredSkills, []string{"code"}) {
		t.Fatalf("explicit coder worker policy = %#v", got)
	}
	if got := workerPolicy(config, WorkerFileConfig{}); got.NudgePrompt != "" || len(got.RequiredSkills) != 0 {
		t.Fatalf("missing worker role inherited default policy: %#v", got)
	}
}

func TestTICKETORCCONFIGCannotSelectASecondInstance(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	instanceA := filepath.Join(cwd, "orc-a")
	instanceB := filepath.Join(cwd, "orc-b")
	if err := os.MkdirAll(instanceA, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(instanceB, 0o700); err != nil {
		t.Fatal(err)
	}
	configA := filepath.Join(instanceA, "config.json")
	configB := filepath.Join(instanceB, "managed.json")
	writeConfigFixture(t, configA, `{"version":1,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"A"}}}`)
	writeConfigFixture(t, configB, `{"version":1,"default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"B"}}}`)
	lookup := mapEnv(map[string]string{"TICKET_ORC": instanceA, "TICKET_ORC_CONFIG": configB})
	loaded, err := loadInvocationConfig(nil, lookup)
	if err != nil || loaded.Instance.ConfigPath != configA || loaded.Instance.InstanceDir != instanceA || loaded.Instance.Explicit || loaded.Config.DefaultRole != "coder" {
		t.Fatalf("split-brain config = %#v, error = %v", loaded, err)
	}
	explicit, err := loadInvocationConfig(map[string]string{"config": configB}, lookup)
	if err != nil || explicit.Instance.ConfigPath != configB || explicit.Instance.InstanceDir != instanceB || !explicit.Instance.Explicit || explicit.Config.DefaultRole != "reviewer" {
		t.Fatalf("explicit config = %#v, error = %v", explicit, err)
	}
}

func TestInstanceContextKeepsRunJoinAndStateFilesTogether(t *testing.T) {
	home := t.TempDir()
	foreign := t.TempDir()
	foreignConfig := filepath.Join(foreign, "config.json")
	writeConfigFixture(t, foreignConfig, `{"version":1,"id":"9e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"reviewer","roles":{"reviewer":{"ticket_queue":"review","nudge_prompt":"foreign"}}}`)
	globalDir := filepath.Join(home, defaultInstanceDirectoryName)
	writeConfigFixture(t, filepath.Join(globalDir, instanceConfigFileName), `{"version":1,"id":"7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"global"}}}`)
	tests := []struct {
		name      string
		instance  string
		relative  bool
		local     bool
		configArg string
		wantID    string
	}{
		{name: "default", instance: filepath.Join(home, defaultInstanceDirectoryName), wantID: "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"},
		{name: "project local wins over global", local: true, wantID: "7e4f5f6d-3a59-49f6-8c2f-e18186ac45ab"},
		{name: "TICKET_ORC", instance: filepath.Join(t.TempDir(), "orc-a"), wantID: "8e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"},
		{name: "relative TICKET_ORC", instance: filepath.Join("instances", "orc-relative"), relative: true, wantID: "5e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"},
		{name: "explicit config", instance: filepath.Join(t.TempDir(), "orc-explicit"), configArg: "explicit", wantID: "6e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cwd := t.TempDir()
			t.Chdir(cwd)
			t.Setenv("HOME", home)
			t.Setenv("USERPROFILE", home)
			instanceDir := test.instance
			if test.local {
				instanceDir = filepath.Join(cwd, defaultInstanceDirectoryName)
			} else if test.relative {
				instanceDir = filepath.Join(cwd, test.instance)
			}
			if err := os.MkdirAll(instanceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(instanceDir, instanceConfigFileName)
			writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q,"default_role":"coder","roles":{"coder":{"ticket_queue":"open","nudge_prompt":"local"}}}`, test.wantID))
			runInstance := filepath.Join(instanceDir, "run-other")
			lookupValues := map[string]string{"TICKET_ORC_CONFIG": foreignConfig}
			if test.name == "TICKET_ORC" || test.relative {
				lookupValues["TICKET_ORC"] = instanceDir
				if test.relative {
					lookupValues["TICKET_ORC"] = test.instance
				}
			}
			if test.name == "explicit config" {
				lookupValues["TICKET_ORC"] = runInstance
			}
			lookup := mapEnv(lookupValues)
			args := []string{}
			if test.configArg != "" {
				relativeConfig, err := filepath.Rel(cwd, configPath)
				if err != nil {
					t.Fatal(err)
				}
				args = []string{"--config", relativeConfig}
			}
			runConfig, help, err := parseRunConfig(args, lookup)
			if err != nil || help {
				t.Fatalf("parse run config=%#v help=%t err=%v", runConfig, help, err)
			}
			localDir := filepath.Join(instanceDir, ".local")
			if runConfig.ConfigPath != configPath || runConfig.StateDir != localDir {
				t.Fatalf("run paths config=%q state=%q, want config %q and local root %q", runConfig.ConfigPath, runConfig.StateDir, configPath, localDir)
			}
			loaded, err := loadInvocationConfig(map[string]string{"config": configPath}, lookup)
			if err != nil || loaded.Config.ID != test.wantID || loaded.Instance.InstanceDir != instanceDir || loaded.Instance.LocalDir != localDir {
				t.Fatalf("loaded instance=%#v err=%v", loaded, err)
			}
			stateConfig, help, err := parseStateConfig(args, lookup)
			if err != nil || help || stateConfig.Instance.InstanceDir != instanceDir || stateConfig.StateDir != localDir {
				t.Fatalf("state paths=%#v help=%t err=%v", stateConfig, help, err)
			}
			daemonOptions, _, help, err := parseDaemonCommandOptions(args, lookup, false)
			if err != nil || help || daemonOptions.localDir != localDir || daemonOptions.environmentURL != "" {
				t.Fatalf("daemon state path=%#v help=%t err=%v", daemonOptions, help, err)
			}
			joinValues := map[string]string{"TICKET_ORC_CONFIG": foreignConfig}
			if test.name != "default" && !test.local {
				joinValues["TICKET_ORC"] = instanceDir
				if test.relative {
					joinValues["TICKET_ORC"] = test.instance
				}
			}
			joinValues["CODEX_THREAD_ID"] = steerTestThread
			joinValues["CODEX_HOME"] = filepath.Join(instanceDir, "codex")
			joinValues["TICKET_ORC_ENDPOINT"] = "http://127.0.0.1/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
			joinLookup := mapEnv(joinValues)
			runner, _ := fakeTicketRunner("")
			var output strings.Builder
			if err := joinWithRunner(context.Background(), "", &output, joinLookup, runner); err != nil {
				t.Fatalf("join: %v", err)
			}
			registration, ok, err := state.NewRegistrationStore(localDir).Find(context.Background(), "d659917f-5939-4e93-bfde-6346a0f2bc50", "reviewer")
			if err != nil || !ok || registration.Role != "reviewer" {
				t.Fatalf("registration=%#v ok=%t err=%v", registration, ok, err)
			}
			if err := state.NewSteerRuntimeStore(localDir).Reconcile(context.Background(), []state.SteerRegistration{registration}); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{"steer.json", "steer-runtime.json"} {
				if _, err := os.Stat(filepath.Join(localDir, file)); err != nil {
					t.Errorf("local runtime file %s was not written: %v", file, err)
				}
			}
			paths := []string{
				runConfig.ConfigPath,
				filepath.Join(localDir, "steer.json"),
				filepath.Join(localDir, "steer-runtime.json"),
				daemon.EndpointKeyPath(daemonOptions.localDir),
				daemon.EndpointPath(daemonOptions.localDir),
			}
			wants := []string{
				configPath,
				filepath.Join(localDir, "steer.json"),
				filepath.Join(localDir, "steer-runtime.json"),
				filepath.Join(localDir, "endpoint.key"),
				filepath.Join(localDir, "run", "endpoint.json"),
			}
			for index, path := range paths {
				if path != wants[index] {
					t.Errorf("instance path %d = %q, want %q", index, path, wants[index])
				}
			}
			if _, err := os.Stat(filepath.Join(foreign, "steer.json")); !os.IsNotExist(err) {
				t.Fatalf("foreign instance received a registration: %v", err)
			}
		})
	}
}

func TestInvocationConfigPathUsesWorkingDirectoryOnce(t *testing.T) {
	home := t.TempDir()
	cwd := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Chdir(cwd)
	path := filepath.Join(home, defaultInstanceDirectoryName, instanceConfigFileName)
	writeConfigFixture(t, path, `{"version":1}`)
	loaded, err := loadInvocationConfig(nil, emptyEnv)
	if err != nil {
		t.Fatalf("load implicit config: %v", err)
	}
	if loaded.Instance.ConfigPath != path || loaded.Instance.InstanceDir != filepath.Dir(path) {
		t.Fatalf("implicit instance = %#v, want config %q", loaded.Instance, path)
	}
	if _, err := os.Stat(filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName)); !os.IsNotExist(err) {
		t.Fatalf("cwd-relative default path exists or stat failed: %v", err)
	}
}
