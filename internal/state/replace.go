package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func writeStateAtomic(path string, data []byte) error {
	return writeStateAtomicMode(path, data, 0o600)
}

func writeStateAtomicMode(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state for %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	keepTemporary := true
	defer func() {
		if keepTemporary {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := securePrivateFile(tmp, tmpPath, mode); err != nil {
		tmp.Close()
		return fmt.Errorf("secure temporary state %s: %w", tmpPath, err)
	}
	written, writeErr := tmp.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = errors.New("short write")
	}
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("write temporary state for %s: %w", path, writeErr)
	}

	if err := publishReplace(path, tmpPath); err != nil {
		applied, recoveryErr := recoverFailedReplacement(path, tmpPath)
		if recoveryErr != nil {
			return fmt.Errorf("publish state %s: %w; recovery failed: %v", path, err, recoveryErr)
		}
		if !applied {
			return fmt.Errorf("publish state %s: %w", path, err)
		}
	}
	keepTemporary = false
	if err := verifyPrivateFilePath(path, mode); err != nil {
		return fmt.Errorf("verify state file %s: %w", path, err)
	}
	return nil
}

// WriteAtomic writes data to path using a restrictive temporary file and a
// complete-file replacement. It is used by runtime metadata that must never
// be observed partially written.
func WriteAtomic(path string, data []byte) error {
	return writeStateAtomic(path, data)
}

// WriteAtomicMode replaces path atomically while applying the requested
// permission bits to the new file.
func WriteAtomicMode(path string, data []byte, mode os.FileMode) error {
	return writeStateAtomicMode(path, data, mode)
}

func recoverFailedReplacement(target, tmp string) (bool, error) {
	if _, err := os.Lstat(target); err == nil {
		if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	if err := os.Rename(tmp, target); err != nil {
		return false, err
	}
	return true, nil
}
