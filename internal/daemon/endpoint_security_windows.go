//go:build windows

package daemon

import (
	"fmt"
	"os"
)

// Endpoint files inherit the user's profile ACL on Windows. Mode bits do not
// provide a privacy boundary there, so validate file structure only.
func secureEndpointDirectory(string) error          { return nil }
func endpointDirectoryModeMatches(os.FileInfo) bool { return true }

func secureEndpointFile(file *os.File, path string, _ os.FileMode) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("endpoint path is not a regular file: %s", path)
	}
	return nil
}
