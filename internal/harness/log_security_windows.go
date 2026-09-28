//go:build windows

package harness

import (
	"fmt"
	"os"
)

// Raw logs inherit the user's profile ACL on Windows. Mode bits do not
// provide a privacy boundary there, so validate structure only.
func secureLogDirectory(string) error          { return nil }
func logDirectoryModeMatches(os.FileInfo) bool { return true }

func secureLogFile(file *os.File, path string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("raw log path is not a regular file: %s", path)
	}
	return nil
}
