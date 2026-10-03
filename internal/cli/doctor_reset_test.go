package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func doctorResetFixture(t *testing.T, localDir string) (string, string) {
	t.Helper()
	instanceDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(instanceDir, "repository"), 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(instanceDir, instanceConfigFileName)
	config := fmt.Sprintf(`{"version":1,"id":%q,"repositories":{"main":{"repository":"repository"}},"workers":{"coder":{"role":"coder","actor":"coder","repository":"main","ticket_prompt":"preserve this prompt {{ticket}}"}},"review":{"skip_tags":["trivial"]}}`, runtimeMarkerTestID)
	if localDir != "" {
		config = strings.TrimSuffix(config, "}") + fmt.Sprintf(`,"local_dir":%q}`, localDir)
	}
	writeConfigFixture(t, configPath, config)
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return configPath, string(data)
}

func runDoctorReset(t *testing.T, configPath string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := executeDoctor(doctorConfig{configPath: configPath, resetLocal: true}, &stdout, &stderr, emptyEnv)
	return stdout.String(), stderr.String(), err
}

func TestDoctorResetLocalPreservesConfigAndRecreatesRuntimeWithSameID(t *testing.T) {
	configPath, before := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(localRoot, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localRoot, "run", "sentinel"), []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, err := runDoctorReset(t, configPath)
	if err != nil {
		t.Fatalf("reset error=%v stderr=%q", err, stderr)
	}
	if !strings.Contains(stdout, "Reset local Orc runtime.") || strings.Contains(stdout, "rejoin") {
		t.Fatalf("reset output=%q", stdout)
	}
	if got, err := os.ReadFile(configPath); err != nil || string(got) != before {
		t.Fatalf("config changed: err=%v before=%q after=%q", err, before, got)
	}
	if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
		t.Fatalf("runtime root remains after reset: %v", err)
	}
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatalf("recreate runtime: %v", err)
	}
	marker, err := readAndValidateRuntimeMarker(localRoot)
	if err != nil || marker.InstanceID != runtimeMarkerTestID {
		t.Fatalf("recreated marker=%#v err=%v", marker, err)
	}
}

func TestDoctorResetLocalCountsCurrentRegistrationsAndReportsCorruptStore(t *testing.T) {
	t.Run("current registration", func(t *testing.T) {
		configPath, _ := doctorResetFixture(t, "")
		localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
		if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
			t.Fatal(err)
		}
		if err := writeDoctorRegistration(t, localRoot, "worker"); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, err := runDoctorReset(t, configPath)
		if err != nil {
			t.Fatalf("reset error=%v stderr=%q", err, stderr)
		}
		if !strings.Contains(stdout, "1 steered session") || !strings.Contains(stdout, "ticket-orc join") {
			t.Fatalf("registration warning missing: %q", stdout)
		}
	})
	t.Run("corrupt authoritative store", func(t *testing.T) {
		configPath, _ := doctorResetFixture(t, "")
		localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
		if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(localRoot, "steer.json"), []byte("not-json"), 0o600); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, err := runDoctorReset(t, configPath)
		if err != nil {
			t.Fatalf("reset error=%v stderr=%q", err, stderr)
		}
		if !strings.Contains(stderr, "number of steered sessions requiring rejoin is unknown") || strings.Contains(stdout, "steered session") {
			t.Fatalf("corrupt-store diagnostics stdout=%q stderr=%q", stdout, stderr)
		}
	})
}

func TestDoctorResetLocalLegacyLayoutDeletesWholeRootAndCountsOnlySelectedStore(t *testing.T) {
	configPath, before := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	selected := filepath.Join(localRoot, runtimeMarkerTestID)
	stale := filepath.Join(localRoot, runtimeMarkerOtherID)
	for _, root := range []string{selected, stale} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "sentinel"), []byte("legacy"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeDoctorRegistration(t, selected, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := writeDoctorRegistration(t, stale, "stale"); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := runDoctorReset(t, configPath)
	if err != nil {
		t.Fatalf("reset error=%v stderr=%q", err, stderr)
	}
	if !strings.Contains(stdout, "1 steered session") || !strings.Contains(stdout, "ticket-orc join") {
		t.Fatalf("selected registration warning missing: %q", stdout)
	}
	if got, err := os.ReadFile(configPath); err != nil || string(got) != before {
		t.Fatalf("config changed: err=%v before=%q after=%q", err, before, got)
	}
	if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
		t.Fatalf("legacy root remains: %v", err)
	}
}

func TestDoctorResetLocalLegacyWarningUsesOnlyConfigID(t *testing.T) {
	configPath, _ := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	selected := filepath.Join(localRoot, runtimeMarkerTestID)
	stale := filepath.Join(localRoot, runtimeMarkerOtherID)
	for _, root := range []string{selected, stale} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeDoctorRegistration(t, stale, "stale"); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := runDoctorReset(t, configPath)
	if err != nil {
		t.Fatalf("reset error=%v stderr=%q", err, stderr)
	}
	if strings.Contains(stdout, "steered session") {
		t.Fatalf("stale registration triggered warning: %q", stdout)
	}
}

func TestDoctorResetLocalRefusesRunningDaemonWithoutMutation(t *testing.T) {
	configPath, before := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatal(err)
	}
	lock, err := state.TryAcquireLock(context.Background(), filepath.Join(localRoot, "run"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	stdout, stderr, err := runDoctorReset(t, configPath)
	if err == nil || !strings.Contains(err.Error(), "cannot reset local runtime while this Orc instance is running") {
		t.Fatalf("running daemon reset err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if _, err := os.Stat(filepath.Join(localRoot, runtimeMarkerFileName)); err != nil {
		t.Fatalf("runtime marker changed or disappeared: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(localRoot, "lock")); !os.IsNotExist(err) {
		t.Fatalf("active daemon refusal touched registration store lock: %v", err)
	}
	if got, err := os.ReadFile(configPath); err != nil || string(got) != before {
		t.Fatalf("config changed: err=%v before=%q after=%q", err, before, got)
	}
}

func TestDoctorResetLocalRefusesDaemonInSelectedLegacyRuntime(t *testing.T) {
	configPath, _ := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	selected := filepath.Join(localRoot, runtimeMarkerTestID)
	stale := filepath.Join(localRoot, runtimeMarkerOtherID)
	if err := os.MkdirAll(filepath.Join(selected, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := state.TryAcquireLock(context.Background(), filepath.Join(selected, "run"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	_, _, err = runDoctorReset(t, configPath)
	if err == nil || !strings.Contains(err.Error(), "cannot reset local runtime while this Orc instance is running") {
		t.Fatalf("selected legacy daemon reset err=%v", err)
	}
	for _, root := range []string{selected, stale} {
		if _, err := os.Stat(root); err != nil {
			t.Fatalf("legacy runtime changed after refusal: %s: %v", root, err)
		}
	}
}

func TestDoctorResetLocalProtectsMixedAndDamagedRuntimeLocks(t *testing.T) {
	tests := []struct {
		name   string
		lockAt string
		marker string
	}{
		{name: "legacy root direct lock", lockAt: "direct"},
		{name: "damaged direct marker selected child lock", lockAt: "selected", marker: "{"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath, before := doctorResetFixture(t, "")
			localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
			selected := filepath.Join(localRoot, runtimeMarkerTestID)
			if err := os.MkdirAll(selected, 0o700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(selected, "sentinel")
			if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.marker != "" {
				if err := os.WriteFile(filepath.Join(localRoot, runtimeMarkerFileName), []byte(test.marker), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			lockRoot := localRoot
			if test.lockAt == "selected" {
				lockRoot = selected
			}
			lock, err := state.TryAcquireLock(context.Background(), filepath.Join(lockRoot, "run"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Release()

			stdout, stderr, err := runDoctorReset(t, configPath)
			if err == nil || !strings.Contains(err.Error(), "cannot reset local runtime while this Orc instance is running") {
				t.Fatalf("reset err=%v stdout=%q stderr=%q", err, stdout, stderr)
			}
			if got, err := os.ReadFile(configPath); err != nil || string(got) != before {
				t.Fatalf("config changed after active-lock refusal: err=%v", err)
			}
			if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
				t.Fatalf("runtime changed after active-lock refusal: got=%q err=%v", got, err)
			}
		})
	}
}

func TestDoctorResetLocalSymlinkSafety(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require privileges on Windows")
	}
	t.Run("root", func(t *testing.T) {
		configPath, _ := doctorResetFixture(t, "")
		localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
		target := t.TempDir()
		sentinel := filepath.Join(target, "sentinel")
		if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, localRoot); err != nil {
			t.Fatal(err)
		}
		_, _, err := runDoctorReset(t, configPath)
		if err == nil {
			t.Fatal("reset accepted symlink runtime root")
		}
		if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
			t.Fatalf("symlink target changed: got=%q err=%v", got, err)
		}
	})
	t.Run("nested", func(t *testing.T) {
		configPath, _ := doctorResetFixture(t, "")
		localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
		if err := ensureRuntimeOwnership(localRoot, runtimeMarkerTestID, false); err != nil {
			t.Fatal(err)
		}
		target := t.TempDir()
		sentinel := filepath.Join(target, "sentinel")
		if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(localRoot, "nested-link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, _, err := runDoctorReset(t, configPath); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(link); !os.IsNotExist(err) {
			t.Fatalf("nested symlink remains: %v", err)
		}
		if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
			t.Fatalf("nested symlink target changed: got=%q err=%v", got, err)
		}
	})
}

func TestDoctorResetLocalExplicitRootRequiresOwnershipMarker(t *testing.T) {
	t.Run("matching owner", func(t *testing.T) {
		external := filepath.Join(t.TempDir(), "runtime")
		configPath, _ := doctorResetFixture(t, external)
		if err := ensureRuntimeOwnership(external, runtimeMarkerTestID, true); err != nil {
			t.Fatal(err)
		}
		if _, _, err := runDoctorReset(t, configPath); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(external); !os.IsNotExist(err) {
			t.Fatalf("owned external runtime remains: %v", err)
		}
	})
	t.Run("unowned", func(t *testing.T) {
		external := filepath.Join(t.TempDir(), "runtime")
		configPath, _ := doctorResetFixture(t, external)
		if err := os.MkdirAll(external, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(external, "sentinel"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, _, err := runDoctorReset(t, configPath)
		if err == nil || !strings.Contains(err.Error(), "without valid ownership evidence") {
			t.Fatalf("unowned external reset err=%v", err)
		}
		if got, err := os.ReadFile(filepath.Join(external, "sentinel")); err != nil || string(got) != "keep" {
			t.Fatalf("unowned external root changed: got=%q err=%v", got, err)
		}
	})
	t.Run("different owner", func(t *testing.T) {
		external := filepath.Join(t.TempDir(), "runtime")
		configPath, _ := doctorResetFixture(t, external)
		if err := ensureRuntimeOwnership(external, runtimeMarkerOtherID, true); err != nil {
			t.Fatal(err)
		}
		_, _, err := runDoctorReset(t, configPath)
		if err == nil || !strings.Contains(err.Error(), "not config ID") {
			t.Fatalf("mismatched external owner err=%v", err)
		}
		if _, err := os.Stat(filepath.Join(external, runtimeMarkerFileName)); err != nil {
			t.Fatalf("mismatched external root changed: %v", err)
		}
	})
}

func TestDoctorResetLocalIsIdempotent(t *testing.T) {
	configPath, _ := doctorResetFixture(t, "")
	stdout, stderr, err := runDoctorReset(t, configPath)
	if err != nil {
		t.Fatalf("reset absent runtime: %v stderr=%q", err, stderr)
	}
	if !strings.Contains(stdout, "already reset") {
		t.Fatalf("idempotent output=%q", stdout)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)); !os.IsNotExist(err) {
		t.Fatalf("reset eagerly created runtime: %v", err)
	}
}

func TestPlainDoctorDoesNotCreateOrChangeRuntime(t *testing.T) {
	configPath, before := doctorResetFixture(t, "")
	localRoot := filepath.Join(filepath.Dir(configPath), defaultRuntimeRootName)
	var stdout, stderr bytes.Buffer
	if err := executeDoctor(doctorConfig{configPath: configPath}, &stdout, &stderr, emptyEnv); err != nil {
		t.Fatalf("doctor err=%v stderr=%q", err, stderr.String())
	}
	if _, err := os.Lstat(localRoot); !os.IsNotExist(err) {
		t.Fatalf("plain doctor changed runtime: %v", err)
	}
	if got, err := os.ReadFile(configPath); err != nil || string(got) != before {
		t.Fatalf("plain doctor changed config: err=%v before=%q after=%q", err, before, got)
	}
}

func writeDoctorRegistration(t *testing.T, runtimeRoot, actor string) error {
	t.Helper()
	repositoryPath := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(repositoryPath, 0o700); err != nil {
		return err
	}
	home := t.TempDir()
	registration := state.SteerRegistration{
		RepositoryID:   "d659917f-5939-4e93-bfde-6346a0f2bc50",
		RepositoryPath: repositoryPath,
		Actor:          actor,
		Role:           "coder",
		Harness:        "codex",
		SessionID:      "session-" + actor,
		Transport:      state.SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": home}},
	}
	_, _, _, err := state.NewRegistrationStore(runtimeRoot).Join(context.Background(), registration)
	return err
}
