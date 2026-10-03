package cli

import (
	"fmt"
	"os"
	"path/filepath"
)

func validateDoctorResetRoot(instanceDir, localRoot string, explicit bool, instanceID string) error {
	if !explicit {
		want := filepath.Join(instanceDir, defaultRuntimeRootName)
		if filepath.Clean(localRoot) != filepath.Clean(want) {
			return fmt.Errorf("implicit runtime root is not the expected instance path: %s", localRoot)
		}
	}
	info, err := os.Lstat(localRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect local runtime root %s: %w", localRoot, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("cannot reset local runtime: runtime root is not a real directory: %s", localRoot)
	}
	if explicit {
		marker, err := readAndValidateRuntimeMarker(localRoot)
		if err != nil {
			return fmt.Errorf("cannot reset external local runtime without valid ownership evidence at %s: %w", localRoot, err)
		}
		if marker.InstanceID != instanceID {
			return fmt.Errorf("cannot reset external local runtime %s: marker belongs to instance %s, not config ID %s", localRoot, marker.InstanceID, instanceID)
		}
	}
	return nil
}

// doctorRuntimeResetPaths returns one recursive deletion root. It selects a
// legacy child only for the daemon-lock and registration checks, never deletion.
func doctorRuntimeResetPaths(loaded LoadedFileConfig) (cleanupRoots, daemonRoots []string, registrationRoot string, exists bool, err error) {
	localRoot := loaded.Instance.LocalDir
	if err := validateDoctorResetRoot(loaded.Instance.InstanceDir, localRoot, loaded.Instance.LocalDirConfigured, loaded.Config.ID); err != nil {
		return nil, nil, "", false, err
	}
	info, err := os.Lstat(localRoot)
	if os.IsNotExist(err) {
		return []string{localRoot}, nil, "", false, nil
	}
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("inspect local runtime root %s: %w", localRoot, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, nil, "", false, fmt.Errorf("cannot reset local runtime: runtime root is not a real directory: %s", localRoot)
	}

	if loaded.Instance.LocalDirConfigured {
		return []string{localRoot}, []string{localRoot}, localRoot, true, nil
	}
	daemonRoots, err = runtimeDaemonSafetyRoots(localRoot, loaded.Config.ID)
	if err != nil {
		return nil, nil, "", false, err
	}
	entries, err := os.ReadDir(localRoot)
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("inspect local runtime root %s: %w", localRoot, err)
	}
	markerPath := filepath.Join(localRoot, runtimeMarkerFileName)
	_, markerErr := os.Lstat(markerPath)
	if markerErr == nil {
		return []string{localRoot}, daemonRoots, localRoot, true, nil
	}
	if !os.IsNotExist(markerErr) {
		return nil, nil, "", false, fmt.Errorf("inspect runtime marker %s: %w", markerPath, markerErr)
	}
	if isLegacyRuntimeLayout(localRoot, entries) {
		if loaded.Config.ID == "" {
			return []string{localRoot}, daemonRoots, "", true, nil
		}
		active := filepath.Join(localRoot, loaded.Config.ID)
		activeInfo, activeErr := os.Lstat(active)
		if os.IsNotExist(activeErr) {
			return []string{localRoot}, daemonRoots, "", true, nil
		}
		if activeErr != nil {
			return nil, nil, "", false, fmt.Errorf("inspect selected legacy runtime %s: %w", active, activeErr)
		}
		if activeInfo.Mode()&os.ModeSymlink != 0 || !activeInfo.IsDir() {
			return nil, nil, "", false, fmt.Errorf("selected legacy runtime is not a real directory: %s", active)
		}
		return []string{localRoot}, daemonRoots, active, true, nil
	}
	return []string{localRoot}, daemonRoots, localRoot, true, nil
}
