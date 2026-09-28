package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func ensureEndpointCapability(configPath, stateDir, legacyKey string) (string, error) {
	key, err := daemon.LoadOrCreateEndpointKey(stateDir, legacyKey)
	if err != nil {
		return "", err
	}
	if legacyKey != "" {
		if err := removeLegacyEndpointKey(configPath, legacyKey); err != nil {
			return "", err
		}
	}
	return key, nil
}

func removeLegacyEndpointKey(configPath, legacyKey string) error {
	if configPath == "" {
		return fmt.Errorf("cannot migrate legacy endpoint capability without a config path")
	}
	path, err := filepath.EvalSymlinks(configPath)
	if err != nil {
		return fmt.Errorf("resolve Orc config for endpoint capability migration: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect Orc config for endpoint capability migration: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("Orc config for endpoint capability migration must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read Orc config for endpoint capability migration: %w", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode Orc config for endpoint capability migration: %w", err)
	}
	var supervisor map[string]json.RawMessage
	if raw := document["supervisor"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &supervisor); err != nil {
			return fmt.Errorf("decode supervisor config for endpoint capability migration: %w", err)
		}
	}
	if len(supervisor) == 0 {
		return nil
	}
	rawKey, present := supervisor["endpoint_key"]
	if !present {
		return nil
	}
	var currentKey string
	if err := json.Unmarshal(rawKey, &currentKey); err != nil || currentKey != legacyKey {
		return fmt.Errorf("Orc config changed during endpoint capability migration")
	}
	delete(supervisor, "endpoint_key")
	encodedSupervisor, err := json.Marshal(supervisor)
	if err != nil {
		return fmt.Errorf("encode supervisor config without endpoint capability: %w", err)
	}
	document["supervisor"] = encodedSupervisor
	updated, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Orc config without endpoint capability: %w", err)
	}
	updated = append(updated, '\n')
	if err := state.WriteAtomicMode(path, updated, info.Mode()); err != nil {
		return fmt.Errorf("atomically remove endpoint capability from Orc config: %w", err)
	}
	return nil
}
