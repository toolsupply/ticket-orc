//go:build unix

package executionlog

import "os"

func privateModeMatches(info os.FileInfo, mode os.FileMode) bool {
	return info.Mode().Perm() == mode
}
