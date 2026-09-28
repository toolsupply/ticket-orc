//go:build unix

package harness

import (
	"fmt"
	"os"
)

func secureLogDirectory(path string) error {
	if err := os.Chmod(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("final mode is not 0700")
	}
	return nil
}

func logDirectoryModeMatches(info os.FileInfo) bool { return info.Mode().Perm() == 0o700 }

func secureLogFile(file *os.File, _ string) error {
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("final mode is not 0600 on a regular file")
	}
	return nil
}
