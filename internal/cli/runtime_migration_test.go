package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyRuntimeLayoutIsRejectedWithoutMutation(t *testing.T) {
	instanceDir := t.TempDir()
	configPath := filepath.Join(instanceDir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	localRoot := filepath.Join(instanceDir, defaultRuntimeRootName)
	legacy := filepath.Join(localRoot, runtimeMarkerTestID)
	if err := os.MkdirAll(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(legacy, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadFileConfig(instanceDir, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	err = ensureLoadedRuntime(loaded)
	wantError := "unsupported legacy ticket-orc runtime layout detected in " + localRoot +
		"\n\nTo preserve config.json and reset only local runtime state, run:\n\n  ticket-orc doctor --reset-local" +
		"\n\nTo replace both configuration and runtime with generated defaults, run:\n\n  ticket-orc init --force"
	if err == nil || err.Error() != wantError {
		t.Fatalf("legacy runtime error = %q, want formatted diagnostic %q", err, wantError)
	}
	if _, err := inspectDoctorRuntime(loaded); err == nil || err.Error() != wantError {
		t.Fatalf("doctor legacy runtime error = %q, want formatted diagnostic %q", err, wantError)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
		t.Fatalf("legacy runtime changed: data=%q err=%v", data, err)
	}
	if _, err := os.Lstat(filepath.Join(localRoot, runtimeMarkerFileName)); !os.IsNotExist(err) {
		t.Fatalf("legacy runtime received direct marker: %v", err)
	}
}

func TestConfigCheckLeavesLegacyRuntimeUntouched(t *testing.T) {
	instanceDir := t.TempDir()
	configPath := filepath.Join(instanceDir, instanceConfigFileName)
	writeConfigFixture(t, configPath, `{"version":1,"id":"`+runtimeMarkerTestID+`"}`)
	legacy := filepath.Join(instanceDir, defaultRuntimeRootName, runtimeMarkerTestID)
	if err := os.MkdirAll(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(legacy, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr strings.Builder
	if err := executeConfigCheck(ConfigCheckConfig{ConfigPath: configPath, Explicit: true, Output: OutputQuiet}, &stdout, &stderr, emptyEnv); err != nil {
		t.Fatalf("config check: %v stderr=%q", err, stderr.String())
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
		t.Fatalf("config check changed legacy runtime: data=%q err=%v", data, err)
	}
	if _, err := os.Lstat(filepath.Join(instanceDir, defaultRuntimeRootName, runtimeMarkerFileName)); !os.IsNotExist(err) {
		t.Fatalf("config check created direct runtime marker: %v", err)
	}
}
