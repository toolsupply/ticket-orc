//go:build linux || darwin

package state

import "os"

func publishReplace(target, replacement string) error {
	return os.Rename(replacement, target)
}
