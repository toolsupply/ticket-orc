//go:build unix

package cli

import (
	"fmt"
	"os"
)

func secureInstanceDirectory(path string) error {
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

func instanceDirectoryModeMatches(info os.FileInfo) bool { return info.Mode().Perm() == 0o700 }

func secureInstanceFile(file *os.File, _ string, mode os.FileMode) error {
	if err := file.Chmod(mode.Perm()); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("final mode is not %04o on a regular file", mode.Perm())
	}
	return nil
}

func verifyInstanceFile(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("final file mode is not %04o on a regular file", mode.Perm())
	}
	return nil
}
