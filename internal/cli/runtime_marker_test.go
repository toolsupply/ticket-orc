package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

const runtimeMarkerTestID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
const runtimeMarkerOtherID = "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"

func TestRuntimeMarkerIsCreatedLazilyAndBoundToConfigID(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, instanceConfigFileName)
	writeConfigFixture(t, configPath, fmt.Sprintf(`{"version":1,"id":%q}`, runtimeMarkerTestID))
	loaded, err := LoadFileConfig(configDir, configPath, true)
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := loaded.Instance.LocalDir
	if _, err := os.Lstat(runtimeRoot); !os.IsNotExist(err) {
		t.Fatalf("config load created runtime root: stat err=%v", err)
	}
	if err := ensureRuntimeOwnership(runtimeRoot, runtimeMarkerTestID, loaded.Instance.LocalDirConfigured); err != nil {
		t.Fatalf("create runtime ownership marker: %v", err)
	}
	markerPath := filepath.Join(runtimeRoot, runtimeMarkerFileName)
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"version":1`, `"layout":2`, `"instance_id":"` + runtimeMarkerTestID + `"`} {
		if !strings.Contains(string(marker), field) {
			t.Fatalf("marker %s does not contain %s", marker, field)
		}
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(markerPath)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("marker mode = %v, err=%v; want 0600", info, err)
		}
	}
	if err := ensureRuntimeOwnership(runtimeRoot, runtimeMarkerTestID, false); err != nil {
		t.Fatalf("accept matching marker: %v", err)
	}
	if err := ensureRuntimeOwnership(runtimeRoot, runtimeMarkerOtherID, false); err == nil || !strings.Contains(err.Error(), "runtime belongs to instance") {
		t.Fatalf("mismatching ID error = %v", err)
	}
	after, err := os.ReadFile(markerPath)
	if err != nil || string(after) != string(marker) {
		t.Fatalf("mismatching ID rewrote marker: err=%v before=%q after=%q", err, marker, after)
	}
}

func TestConcurrentRuntimeMarkerCreationCannotReplaceOwner(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, instanceID := range []string{runtimeMarkerTestID, runtimeMarkerOtherID} {
		workers.Add(1)
		go func(id string) {
			defer workers.Done()
			<-start
			results <- ensureRuntimeOwnership(root, id, false)
		}(instanceID)
	}
	close(start)
	workers.Wait()
	close(results)
	successes, mismatches := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if strings.Contains(err.Error(), "runtime belongs to instance") {
			mismatches++
		} else {
			t.Fatalf("concurrent marker creation error = %v", err)
		}
	}
	if successes != 1 || mismatches != 1 {
		t.Fatalf("concurrent ownership results = %d success, %d mismatch; want one each", successes, mismatches)
	}
	data, err := os.ReadFile(filepath.Join(root, runtimeMarkerFileName))
	if err != nil {
		t.Fatal(err)
	}
	var marker runtimeMarker
	if err := json.Unmarshal(data, &marker); err != nil || marker.InstanceID != runtimeMarkerTestID && marker.InstanceID != runtimeMarkerOtherID {
		t.Fatalf("published concurrent marker = %#v, err=%v", marker, err)
	}
}

func TestRuntimeMarkerRejectsInvalidOrUnsupportedRecordsWithoutRewrite(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{name: "malformed", data: `{`, want: "read runtime marker"},
		{name: "duplicate field", data: `{"version":1,"version":1,"layout":2,"instance_id":"` + runtimeMarkerTestID + `"}`, want: "duplicate key"},
		{name: "unknown field", data: `{"version":1,"layout":2,"instance_id":"` + runtimeMarkerTestID + `","extra":true}`, want: "unknown field"},
		{name: "unsupported version", data: `{"version":2,"layout":2,"instance_id":"` + runtimeMarkerTestID + `"}`, want: "unsupported Orc runtime marker"},
		{name: "unsupported layout", data: `{"version":1,"layout":3,"instance_id":"` + runtimeMarkerTestID + `"}`, want: "unsupported Orc runtime marker"},
		{name: "invalid ID", data: `{"version":1,"layout":2,"instance_id":"bad"}`, want: "invalid instance_id"},
		{name: "oversized", data: strings.Repeat(" ", runtimeMarkerMaxBytes+1), want: "exceeds"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			markerPath := filepath.Join(root, runtimeMarkerFileName)
			if err := os.WriteFile(markerPath, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			err = ensureRuntimeOwnership(root, runtimeMarkerTestID, false)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
			after, readErr := os.ReadFile(markerPath)
			if readErr != nil || string(after) != string(before) {
				t.Fatalf("invalid marker was changed: readErr=%v before=%q after=%q", readErr, before, after)
			}
		})
	}
}

func TestRuntimeMarkerRejectsUnsafePaths(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Run("symlink root", func(t *testing.T) {
			parent := t.TempDir()
			target := t.TempDir()
			root := filepath.Join(parent, ".local")
			if err := os.Symlink(target, root); err != nil {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			if err := ensureRuntimeOwnership(root, runtimeMarkerTestID, false); err == nil || !strings.Contains(err.Error(), "real directory") {
				t.Fatalf("symlink root error = %v", err)
			}
		})
		t.Run("symlink marker", func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "marker")
			if err := os.WriteFile(target, []byte(`{"version":1,"layout":2,"instance_id":"`+runtimeMarkerTestID+`"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(root, runtimeMarkerFileName)); err != nil {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			if err := ensureRuntimeOwnership(root, runtimeMarkerTestID, false); err == nil || !strings.Contains(err.Error(), "regular file") {
				t.Fatalf("symlink marker error = %v", err)
			}
		})
		t.Run("unsafe marker permissions", func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, runtimeMarkerFileName), []byte(`{"version":1,"layout":2,"instance_id":"`+runtimeMarkerTestID+`"}`), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := ensureRuntimeOwnership(root, runtimeMarkerTestID, false); err == nil || !strings.Contains(err.Error(), "unsafe runtime marker") {
				t.Fatalf("unsafe marker permissions error = %v", err)
			}
		})
	}
}
