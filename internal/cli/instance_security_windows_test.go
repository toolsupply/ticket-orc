//go:build windows

package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsInitUsesProjectLocalDirectoryAndOverrides(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name             string
		args             []string
		instance         string
		ticketORC        string
		absoluteOverride bool
		global           bool
	}{
		{name: "project local", instance: filepath.Join("project", defaultInstanceDirectoryName)},
		{name: "global", args: []string{"--global"}, instance: filepath.Join("profile", defaultInstanceDirectoryName), ticketORC: "ignored-override", global: true},
		{name: "absolute override", instance: "absolute-instance", ticketORC: "absolute-instance", absoluteOverride: true},
		{name: "relative override", instance: filepath.Join("project", "relative-instance"), ticketORC: "relative-instance"},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseRoot := filepath.Join(root, test.name)
			cwd := filepath.Join(caseRoot, "project")
			profile := filepath.Join(caseRoot, "profile")
			if err := os.MkdirAll(cwd, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(profile, 0o700); err != nil {
				t.Fatal(err)
			}
			t.Chdir(cwd)
			t.Setenv("USERPROFILE", profile)
			t.Setenv("HOME", profile)

			lookup := emptyEnv
			if test.ticketORC != "" {
				value := test.ticketORC
				if test.absoluteOverride {
					value = filepath.Join(caseRoot, value)
				}
				lookup = mapEnv(map[string]string{"TICKET_ORC": value})
			}
			var stdout, stderr bytes.Buffer
			if code := executeInit(test.args, &stdout, &stderr, lookup); code != 0 {
				t.Fatalf("init code=%d stderr=%q", code, stderr.String())
			}
			configPath := filepath.Join(caseRoot, test.instance, instanceConfigFileName)
			info, err := os.Stat(configPath)
			if err != nil {
				t.Fatalf("stat expected config %s: %v", configPath, err)
			}
			if !info.Mode().IsRegular() {
				t.Fatalf("config is not a regular file: %s", configPath)
			}
			if !bytes.Contains(stdout.Bytes(), []byte(configPath)) {
				t.Fatalf("init output %q does not identify resolved config path %s", stdout.String(), configPath)
			}
			if test.global {
				if _, err := os.Stat(filepath.Join(cwd, defaultInstanceDirectoryName, instanceConfigFileName)); !os.IsNotExist(err) {
					t.Fatalf("--global created or selected project config: %v", err)
				}
			}
		})
	}
}
