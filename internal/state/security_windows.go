//go:build windows

package state

import (
	"fmt"
	"os"
)

// Windows state files inherit the profile directory's ACL. Mode bits do not
// provide a privacy boundary there, so state operations validate structure
// without attempting POSIX permission changes.
func securePrivateDirectory(string) error { return nil }

func securePrivateFile(file *os.File, path string, _ os.FileMode) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("state file is not a regular file: %s", path)
	}
	return nil
}

func privateDirectoryModeMatches(os.FileInfo, os.FileMode) bool { return true }

func verifyPrivateFilePath(path string, _ os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("state path is not a regular file: %s", path)
	}
	return nil
}
