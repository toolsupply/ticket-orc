//go:build windows

package executionlog

import "os"

// Windows log privacy comes from the user's profile ACL, as for raw log writers.
func privateModeMatches(os.FileInfo, os.FileMode) bool { return true }
