package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestEnsureEndpointCapabilityMigratesLegacyConfigAfterPersistingKey(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	stateDir := filepath.Join(root, ".ticket-orc")
	legacyKey := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	original := `{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","defaults":{"output":"compact"},"supervisor":{"endpoint_key":"` + legacyKey + `","startup_groups":[]},"workers":{}}` + "\n"
	if err := os.WriteFile(configPath, []byte(original), 0o640); err != nil {
		t.Fatal(err)
	}

	key, err := ensureEndpointCapability(configPath, stateDir, legacyKey)
	if err != nil || key != legacyKey {
		t.Fatalf("ensure endpoint capability = %q, err=%v", key, err)
	}
	persisted, err := daemon.LoadEndpointKey(stateDir)
	if err != nil || persisted != legacyKey {
		t.Fatalf("persisted endpoint key = %q, err=%v", persisted, err)
	}
	updated, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(updated), legacyKey) || strings.Contains(string(updated), "endpoint_key") {
		t.Fatalf("migrated config still contains endpoint capability: %s", updated)
	}
	var decoded map[string]any
	if err := json.Unmarshal(updated, &decoded); err != nil {
		t.Fatalf("migrated config is invalid JSON: %v", err)
	}
	supervisor := decoded["supervisor"].(map[string]any)
	if _, exists := supervisor["endpoint_key"]; exists || len(supervisor["startup_groups"].([]any)) != 0 || decoded["defaults"].(map[string]any)["output"] != "compact" {
		t.Fatalf("migration discarded unrelated config values: %#v", decoded)
	}
	loaded, err := LoadFileConfig(root, configPath, true)
	if err != nil || loaded.Config.Defaults.Output != "compact" {
		t.Fatalf("migrated config is not valid: config=%#v err=%v", loaded.Config, err)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o640 {
		t.Fatalf("config mode = %04o, want 0640", info.Mode().Perm())
	}
}

func TestEnsureEndpointCapabilityKeepsLegacyConfigIfMigrationCannotComplete(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	stateDir := filepath.Join(root, ".ticket-orc")
	legacyKey := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	changedKey := "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	original := `{"version":1,"supervisor":{"endpoint_key":"` + changedKey + `"}}` + "\n"
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if key, err := ensureEndpointCapability(configPath, stateDir, legacyKey); err == nil || key != "" {
		t.Fatalf("ensure endpoint capability = %q, err=%v; want migration error", key, err)
	}
	persisted, err := daemon.LoadEndpointKey(stateDir)
	if err != nil || persisted != legacyKey {
		t.Fatalf("private key was not durable before migration failure: key=%q err=%v", persisted, err)
	}
	unchanged, err := os.ReadFile(configPath)
	if err != nil || string(unchanged) != original {
		t.Fatalf("failed migration changed config: data=%q err=%v", unchanged, err)
	}
}
