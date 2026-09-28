//go:build !linux && !darwin && !windows

package cli

import "os"

func isTerminalFile(_ *os.File) bool {
	return false
}
