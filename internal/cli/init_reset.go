package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func initRuntimeResetGuardRoot(instanceDir, configPath string) string {
	loaded, err := LoadFileConfig(instanceDir, configPath, true)
	if err == nil && loaded.Instance.LocalDirConfigured {
		return loaded.Instance.LocalDir
	}
	return filepath.Join(instanceDir, defaultRuntimeRootName)
}

// resetInitRuntime removes the known implicit runtime root, or an explicit
// runtime root whose ownership marker matches the config being replaced.
func resetInitRuntime(instanceDir, configPath string) error {
	plan, err := inspectInitRuntimeReset(instanceDir, configPath)
	if err != nil {
		return err
	}
	return plan.apply()
}

type initRuntimeResetPlan struct {
	cleanupRoots []string
	daemonRoots  []string
}

func inspectInitRuntimeReset(instanceDir, configPath string) (*initRuntimeResetPlan, error) {
	localRoot := filepath.Join(instanceDir, defaultRuntimeRootName)
	holding := filepath.Join(instanceDir, obsoleteRuntimeMigrationName)
	loaded, configErr := LoadFileConfig(instanceDir, configPath, true)
	plan := &initRuntimeResetPlan{}

	if configErr == nil && loaded.Instance.LocalDirConfigured {
		info, err := os.Lstat(loaded.Instance.LocalDir)
		if os.IsNotExist(err) {
			return plan, nil
		}
		if err != nil {
			return nil, fmt.Errorf("inspect configured runtime root %s: %w", loaded.Instance.LocalDir, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, fmt.Errorf("refusing --force: configured runtime root is not a real directory: %s", loaded.Instance.LocalDir)
		}
		marker, err := readAndValidateRuntimeMarker(loaded.Instance.LocalDir)
		if err != nil {
			return nil, fmt.Errorf("refusing --force: cannot prove ownership of configured runtime root %s: %w", loaded.Instance.LocalDir, err)
		}
		if marker.InstanceID != loaded.Config.ID {
			return nil, fmt.Errorf("refusing --force: configured runtime root %s belongs to instance %s, not config ID %s", loaded.Instance.LocalDir, marker.InstanceID, loaded.Config.ID)
		}
		plan.cleanupRoots = append(plan.cleanupRoots, loaded.Instance.LocalDir)
		plan.daemonRoots = append(plan.daemonRoots, loaded.Instance.LocalDir)
	} else {
		if err := validateResetDirectory(localRoot); err != nil {
			return nil, err
		}
		if exists, err := pathExists(localRoot); err != nil {
			return nil, fmt.Errorf("inspect runtime root %s: %w", localRoot, err)
		} else if exists {
			plan.cleanupRoots = append(plan.cleanupRoots, localRoot)
			var roots []string
			var err error
			if configErr == nil {
				roots, err = runtimeDaemonSafetyRoots(localRoot, loaded.Config.ID)
			} else {
				// With an unreadable config there is no selected ID. Protect every
				// real child before replacing the config and removing the root.
				roots, err = runtimeDaemonRoots(localRoot)
			}
			if err != nil {
				return nil, err
			}
			plan.daemonRoots = append(plan.daemonRoots, roots...)
		}

		if exists, err := pathExists(holding); err != nil {
			return nil, fmt.Errorf("inspect obsolete migration artifact %s: %w", holding, err)
		} else if exists {
			roots, err := validateObsoleteMigrationArtifact(holding, loaded.Config.ID)
			if err != nil {
				return nil, err
			}
			plan.cleanupRoots = append(plan.cleanupRoots, holding)
			plan.daemonRoots = append(plan.daemonRoots, roots...)
		}
	}
	return plan, nil
}

func (plan *initRuntimeResetPlan) apply() error {
	return removeRuntimeRoots(plan.cleanupRoots, plan.daemonRoots, func(root string) error {
		return fmt.Errorf("refusing --force: Orc instance is running (runtime: %s)", root)
	})
}

func validateResetDirectory(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect runtime root %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("refusing --force: runtime root is not a real directory: %s", path)
	}
	return nil
}

// runtimeDaemonRoots only examines the direct root and immediate real child
// directories used by both supported and obsolete layouts. Deletion remains
// anchored to the known root path; child names never become deletion targets.
func runtimeDaemonRoots(root string) ([]string, error) {
	roots := []string{root}
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect runtime directory %s: %w", root, err)
	}
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("inspect runtime entry %s: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if info.IsDir() {
			roots = append(roots, path)
		}
	}
	return roots, nil
}

// runtimeDaemonSafetyRoots always checks the direct runtime and the selected
// config-ID child, regardless of markers or other contents in the root. A
// selected child is inspected only when it is a real directory; deletion
// remains anchored to the direct root.
func runtimeDaemonSafetyRoots(root, configID string) ([]string, error) {
	roots := []string{root}
	if configID == "" {
		return roots, nil
	}
	selected := filepath.Join(root, configID)
	info, err := os.Lstat(selected)
	if os.IsNotExist(err) {
		return roots, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect selected runtime safety path %s: %w", selected, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return roots, nil
	}
	return append(roots, selected), nil
}

// validateObsoleteMigrationArtifact accepts only the old fixed holding path
// containing UUID-named directories and at least one valid marker for this
// config. Anything else may be unrelated user data and fails closed.
func validateObsoleteMigrationArtifact(path, configID string) ([]string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect obsolete migration artifact %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("refusing --force: obsolete migration artifact is not a real directory: %s", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("inspect obsolete migration artifact %s: %w", path, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("refusing --force: obsolete migration artifact is ambiguous: %s", path)
	}
	owned := false
	localRoot := filepath.Join(filepath.Dir(path), defaultRuntimeRootName)
	if marker, err := readAndValidateRuntimeMarker(localRoot); err == nil {
		if marker.InstanceID != configID {
			return nil, fmt.Errorf("refusing --force: direct runtime belongs to instance %s, not config ID %s", marker.InstanceID, configID)
		}
		owned = true
	} else if err != nil && !os.IsNotExist(err) {
		// A direct marker, when present, must itself be valid. An absent marker
		// is expected for an interrupted pre-promotion migration.
		if _, statErr := os.Lstat(filepath.Join(localRoot, runtimeMarkerFileName)); statErr == nil {
			return nil, fmt.Errorf("refusing --force: direct runtime has invalid ownership evidence: %s", localRoot)
		} else if !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("inspect direct runtime marker under %s: %w", localRoot, statErr)
		}
	}
	roots := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !isUUIDv4(entry.Name()) {
			return nil, fmt.Errorf("refusing --force: obsolete migration artifact contains an unrecognized entry %q", entry.Name())
		}
		child := filepath.Join(path, entry.Name())
		childInfo, err := os.Lstat(child)
		if err != nil || childInfo.Mode()&os.ModeSymlink != 0 || !childInfo.IsDir() {
			return nil, fmt.Errorf("refusing --force: obsolete migration entry is not a real runtime directory: %s", child)
		}
		roots = append(roots, child)
		markerPath := filepath.Join(child, runtimeMarkerFileName)
		_, err = os.Lstat(markerPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect obsolete migration marker %s: %w", markerPath, err)
		}
		marker, err := readAndValidateRuntimeMarker(child)
		if err != nil || marker.InstanceID != entry.Name() {
			return nil, fmt.Errorf("refusing --force: obsolete migration entry has invalid ownership evidence: %s", child)
		}
		if configID != "" && marker.InstanceID == configID {
			owned = true
		}
	}
	if !owned || configID == "" {
		return nil, fmt.Errorf("refusing --force: obsolete migration artifact has no valid runtime marker for this config: %s", path)
	}
	return roots, nil
}

func activeRuntimeDaemonRoot(runtimeRoots []string) (string, error) {
	for _, root := range runtimeRoots {
		runDir := filepath.Join(root, "run")
		info, err := os.Lstat(runDir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("inspect daemon runtime directory %s: %w", runDir, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("daemon runtime path is not a real directory: %s", runDir)
		}
		lockPath := filepath.Join(runDir, "lock")
		lockInfo, err := os.Lstat(lockPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("inspect daemon lock %s: %w", lockPath, err)
		}
		if lockInfo.Mode()&os.ModeSymlink != 0 || !lockInfo.Mode().IsRegular() {
			return "", fmt.Errorf("daemon lock is not a regular file: %s", lockPath)
		}
		lock, err := state.TryAcquireLock(context.Background(), runDir)
		if errors.Is(err, state.ErrLockTimeout) {
			return root, nil
		}
		if err != nil {
			return "", fmt.Errorf("check daemon lock %s: %w", lockPath, err)
		}
		if err := lock.Release(); err != nil {
			return "", fmt.Errorf("release daemon lock %s: %w", lockPath, err)
		}
	}
	return "", nil
}

// removeRuntimeRoots shares daemon protection, final path checks, and deletion
// for explicit local resets and init --force. Callers hold the runtime guard.
func removeRuntimeRoots(cleanupRoots, daemonRoots []string, runningError func(string) error) error {
	activeRoot, err := activeRuntimeDaemonRoot(daemonRoots)
	if err != nil {
		return err
	}
	if activeRoot != "" {
		return runningError(activeRoot)
	}
	for _, root := range cleanupRoots {
		if err := removeRuntimeRoot(root); err != nil {
			return err
		}
	}
	return nil
}

func removeRuntimeRoot(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect runtime root %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("refusing runtime reset: runtime root is not a real directory: %s", path)
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove Orc runtime state %s: %w", path, err)
	}
	return nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func readAndValidateRuntimeMarker(root string) (runtimeMarker, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return runtimeMarker{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return runtimeMarker{}, fmt.Errorf("runtime root is not a real directory")
	}
	markerPath := filepath.Join(root, runtimeMarkerFileName)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		return runtimeMarker{}, err
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
		return runtimeMarker{}, fmt.Errorf("runtime marker is not a regular file")
	}
	if err := validateRuntimeMarkerPermissions(markerInfo); err != nil {
		return runtimeMarker{}, fmt.Errorf("unsafe runtime marker: %w", err)
	}
	marker, err := readRuntimeMarker(markerPath, markerInfo)
	if err != nil {
		return runtimeMarker{}, err
	}
	if marker.Version != runtimeMarkerVersion || marker.Layout != runtimeLayoutVersion || !isUUIDv4(marker.InstanceID) {
		return runtimeMarker{}, fmt.Errorf("unsupported or invalid runtime marker")
	}
	return marker, nil
}
