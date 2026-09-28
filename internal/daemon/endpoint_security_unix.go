//go:build unix

package daemon

import (
	"fmt"
	"os"
)

func secureEndpointDirectory(path string) error {
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

func endpointDirectoryModeMatches(info os.FileInfo) bool { return info.Mode().Perm() == 0o700 }

func secureEndpointFile(file *os.File, _ string, mode os.FileMode) error {
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
