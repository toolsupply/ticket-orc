package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartupGroupsValidatedOnLoadCheckAndRun(t *testing.T) {
	// Declarative checks and parsing must work without any Ticket or harness
	// executable, and must not create runtime state or rewrite configuration.
	t.Setenv("PATH", t.TempDir())
	for _, test := range []struct {
		name, workers, groups, wantError string
	}{
		{name: "unknown", workers: `"one":{"role":"coder","actor":"one"}`, groups: `["missing"]`, wantError: `supervisor.startup_groups[0]: group "missing" has no configured workers`},
		{name: "unused role group", workers: `"one":{"role":"reviewer","actor":"one"}`, groups: `["role-group"]`, wantError: `supervisor.startup_groups[0]: group "role-group" has no configured workers`},
		{name: "no workers", groups: `["role-group"]`, wantError: `supervisor.startup_groups[0]: group "role-group" has no configured workers`},
		{name: "mixed valid invalid", workers: `"one":{"role":"coder","actor":"one"}`, groups: `["role-group","missing"]`, wantError: `supervisor.startup_groups[1]: group "missing" has no configured workers`},
		{name: "mixed invalid valid", workers: `"one":{"role":"coder","actor":"one"}`, groups: `["missing","role-group"]`, wantError: `supervisor.startup_groups[0]: group "missing" has no configured workers`},
		{name: "duplicate startup group", workers: `"one":{"role":"coder","actor":"one"}`, groups: `["role-group","role-group"]`, wantError: `supervisor.startup_groups contains duplicate group "role-group"`},
		{name: "duplicate worker group", workers: `"one":{"role":"coder","actor":"one","groups":["worker-group","worker-group"]}`, groups: `["worker-group"]`, wantError: `worker one groups contains duplicate group "worker-group"`},
		{name: "role inherited", workers: `"one":{"role":"coder","actor":"one"}`, groups: `["role-group"]`},
		{name: "worker and role overlap", workers: `"one":{"role":"coder","actor":"one","groups":["role-group","worker-group"]}`, groups: `["role-group","worker-group"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "config.json")
			writeConfigFixture(t, path, fmt.Sprintf(`{"version":1,"roles":{"coder":{"groups":["role-group"]}},"workers":{%s},"supervisor":{"startup_groups":%s}}`, test.workers, test.groups))
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			assertResult := func(operation string, err error) {
				t.Helper()
				if test.wantError == "" {
					if err != nil {
						t.Fatalf("%s: %v", operation, err)
					}
				} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("%s: err=%v, want %q", operation, err, test.wantError)
				}
			}
			_, err = LoadFileConfig(root, path, true)
			assertResult("load", err)
			var stdout, stderr bytes.Buffer
			err = executeConfigCheck(ConfigCheckConfig{ConfigPath: path, Explicit: true, Output: OutputCompact}, &stdout, &stderr, emptyEnv)
			assertResult("config check", err)
			if test.wantError != "" && !strings.Contains(stderr.String(), test.wantError) {
				t.Fatalf("config check omitted error: %q", stderr.String())
			}
			for _, selectors := range [][]string{nil, {"-i"}, {"--worker", "one"}, {"--group", "role-group"}, {"--all"}} {
				config, _, err := parseRunConfig(append([]string{"--config", path}, selectors...), emptyEnv)
				assertResult(fmt.Sprint("run ", selectors), err)
				if err == nil && (len(config.StartupWorkers) != 1 || config.StartupWorkers[0] != "one") {
					t.Fatalf("run %v selected %v, want one", selectors, config.StartupWorkers)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(original, after) {
				t.Fatalf("configuration changed: err=%v", err)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 1 || entries[0].Name() != "config.json" {
				t.Fatalf("validation created files: %v err=%v", entries, err)
			}
		})
	}
}
