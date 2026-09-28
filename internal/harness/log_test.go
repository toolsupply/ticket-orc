package harness

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOpenRawLogPermissionsAndCollisionNaming(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	now := time.Date(2026, 9, 23, 12, 30, 45, 123, time.UTC)
	first, err := OpenRawLog("Codex", stateDir, "ticket", "coder", now, "worker")
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenRawLog("Pi", stateDir, "ticket", "coder", now, "worker")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	if first.Path() == second.Path() || !strings.HasSuffix(second.Path(), "-1.jsonl") {
		t.Fatalf("collision names = %q and %q", first.Path(), second.Path())
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{stateDir, filepath.Join(stateDir, "logs")} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat %s: %v", path, err)
			}
			if info.Mode().Perm() != 0o700 {
				t.Errorf("directory %s mode = %v, want 0700", path, info.Mode().Perm())
			}
		}
		info, err := os.Stat(first.Path())
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("log mode = %v, want 0600", info.Mode().Perm())
		}
	}
}

func TestOpenRawLogRejectsSymlinkStateDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	temp := t.TempDir()
	target := filepath.Join(temp, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(temp, "state")
	if err := os.Symlink(target, stateDir); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRawLog("Claude", stateDir, "ticket", "coder", time.Now()); err == nil || !strings.Contains(err.Error(), "Claude log directory") {
		t.Fatalf("symlink state directory error = %v", err)
	}
}

func TestOpenRawLogErrorsKeepProviderContext(t *testing.T) {
	_, err := OpenRawLog("Pi", t.TempDir(), "../ticket", "coder", time.Now())
	if err == nil || !strings.Contains(err.Error(), "Pi log ticket") {
		t.Fatalf("unsafe ticket error = %v", err)
	}
}
