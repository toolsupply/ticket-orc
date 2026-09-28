//go:build unix

package cli

import "os"

func replaceInitConfig(replacement, target string) error {
	return os.Rename(replacement, target)
}
