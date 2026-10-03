package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
	"github.com/toolsupply/ticket-orc/internal/state"
)

const (
	runtimeMarkerFileName        = "instance.json"
	defaultRuntimeRootName       = ".local"
	runtimeMarkerMaxBytes        = 4096
	runtimeMarkerVersion         = 1
	runtimeLayoutVersion         = 2
	obsoleteRuntimeMigrationName = ".local.migrate-v1"
)

type runtimeMarker struct {
	Version    int    `json:"version"`
	Layout     int    `json:"layout"`
	InstanceID string `json:"instance_id"`
}

func ensureLoadedRuntime(loaded LoadedFileConfig) error {
	return ensureRuntimeOwnershipIfKnown(loaded.Instance.LocalDir, loaded.Config.ID, loaded.Instance.LocalDirConfigured)
}

func ensureRuntimeOwnershipIfKnown(localDir, instanceID string, localDirConfigured bool) error {
	return ensureRuntimeOwnershipIfKnownWithGuard(context.Background(), localDir, instanceID, localDirConfigured, false)
}

func ensureRuntimeOwnershipIfKnownUnderGuard(localDir, instanceID string, localDirConfigured bool) error {
	return ensureRuntimeOwnershipIfKnownWithGuard(context.Background(), localDir, instanceID, localDirConfigured, true)
}

func ensureRuntimeOwnershipIfKnownWithGuard(ctx context.Context, localDir, instanceID string, localDirConfigured, guardHeld bool) error {
	if instanceID == "" {
		return nil
	}
	if !guardHeld {
		guard, err := acquireRuntimeGuard(ctx, localDir)
		if err != nil {
			return fmt.Errorf("acquire Orc instance runtime guard: %w", err)
		}
		defer guard.Release()
	}
	return ensureRuntimeOwnership(localDir, instanceID, localDirConfigured)
}

// ensureRuntimeOwnership creates a runtime root on first use and binds it to
// the selected config ID. Config loading stays side-effect free so config
// validation can inspect paths without creating runtime state.
func ensureRuntimeOwnership(localDir, instanceID string, localDirConfigured bool) error {
	if !isUUIDv4(instanceID) {
		return fmt.Errorf("runtime ownership requires a valid config instance ID")
	}
	if localDir == "" {
		return fmt.Errorf("runtime directory is empty")
	}
	info, err := os.Lstat(localDir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(localDir, 0o700); err != nil {
			return fmt.Errorf("create runtime directory %s: %w", localDir, err)
		}
		info, err = os.Lstat(localDir)
	}
	if err != nil {
		return fmt.Errorf("inspect runtime directory %s: %w", localDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("runtime directory must be a real directory: %s", localDir)
	}
	markerPath := filepath.Join(localDir, runtimeMarkerFileName)
	markerInfo, err := os.Lstat(markerPath)
	if os.IsNotExist(err) {
		if !localDirConfigured {
			entries, readErr := os.ReadDir(localDir)
			if readErr != nil {
				return fmt.Errorf("inspect runtime directory %s: %w", localDir, readErr)
			}
			if isLegacyRuntimeLayout(localDir, entries) {
				return fmt.Errorf("unsupported legacy ticket-orc runtime layout detected in %s\n\nThe configuration is valid, but its local runtime uses an older, unsupported layout.\n\nTo preserve config.json and reset only local runtime state, run:\n\n    ticket-orc doctor --reset-local\n\nTo replace both configuration and runtime with generated defaults, run:\n\n    ticket-orc init --force", localDir)
			}
			if len(entries) != 0 {
				if _, markerErr := os.Lstat(markerPath); markerErr == nil {
					return ensureRuntimeOwnership(localDir, instanceID, localDirConfigured)
				} else if !os.IsNotExist(markerErr) {
					return fmt.Errorf("inspect runtime marker %s: %w", markerPath, markerErr)
				}
				for _, entry := range entries {
					if !strings.HasPrefix(entry.Name(), ".state-") || !strings.HasSuffix(entry.Name(), ".tmp") {
						return fmt.Errorf("unmarked runtime directory %s is not empty; refusing to claim existing state", localDir)
					}
				}
			}
		}
		if err := writeRuntimeMarker(markerPath, runtimeMarker{Version: runtimeMarkerVersion, Layout: runtimeLayoutVersion, InstanceID: instanceID}); err != nil {
			if errors.Is(err, os.ErrExist) {
				return ensureRuntimeOwnership(localDir, instanceID, localDirConfigured)
			}
			return err
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect runtime marker %s: %w", markerPath, err)
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
		return fmt.Errorf("runtime marker must be a regular file, not a symlink or special file: %s", markerPath)
	}
	if err := validateRuntimeMarkerPermissions(markerInfo); err != nil {
		return fmt.Errorf("unsafe runtime marker %s: %w", markerPath, err)
	}
	marker, err := readRuntimeMarker(markerPath, markerInfo)
	if err != nil {
		return fmt.Errorf("read runtime marker %s: %w", markerPath, err)
	}
	if marker.Version != runtimeMarkerVersion || marker.Layout != runtimeLayoutVersion {
		return fmt.Errorf("unsupported Orc runtime marker version/layout %d/%d in %s", marker.Version, marker.Layout, markerPath)
	}
	if !isUUIDv4(marker.InstanceID) {
		return fmt.Errorf("runtime marker %s has an invalid instance_id", markerPath)
	}
	if marker.InstanceID != instanceID {
		return fmt.Errorf("Orc runtime belongs to instance %s, but config declares %s\n       runtime: %s\n       restore the original config ID or reset the instance with `ticket-orc init --force`", marker.InstanceID, instanceID, localDir)
	}
	return nil
}

func isLegacyRuntimeLayout(localDir string, entries []os.DirEntry) bool {
	for _, entry := range entries {
		if !isUUIDv4(entry.Name()) {
			continue
		}
		info, err := os.Lstat(filepath.Join(localDir, entry.Name()))
		if err == nil && (info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return true
		}
	}
	return false
}

func validateRuntimeMarkerPermissions(info os.FileInfo) error {
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		return fmt.Errorf("permissions are %04o, want 0600", info.Mode().Perm())
	}
	return nil
}

func writeRuntimeMarker(path string, marker runtimeMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode runtime marker: %w", err)
	}
	data = append(data, '\n')
	if err := state.WriteAtomicNewMode(path, data, 0o600); err != nil {
		return fmt.Errorf("write runtime marker %s: %w", path, err)
	}
	return nil
}

func readRuntimeMarker(path string, expected os.FileInfo) (runtimeMarker, error) {
	file, err := os.Open(path)
	if err != nil {
		return runtimeMarker{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return runtimeMarker{}, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(expected, opened) {
		return runtimeMarker{}, fmt.Errorf("marker changed while opening")
	}
	if opened.Size() > runtimeMarkerMaxBytes {
		return runtimeMarker{}, fmt.Errorf("marker exceeds %d bytes", runtimeMarkerMaxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, runtimeMarkerMaxBytes+1))
	if err != nil {
		return runtimeMarker{}, err
	}
	if len(data) > runtimeMarkerMaxBytes {
		return runtimeMarker{}, fmt.Errorf("marker exceeds %d bytes", runtimeMarkerMaxBytes)
	}
	var marker runtimeMarker
	if err := jsonx.Decode(data, &marker); err != nil {
		return runtimeMarker{}, err
	}
	return marker, nil
}
