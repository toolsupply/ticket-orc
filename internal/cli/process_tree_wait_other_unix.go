//go:build !linux && !darwin && !windows

package cli

import "os"

// Unsupported Unix targets retain the conservative pre-existing behavior.
func waitForChildExitBeforeReap(_ *os.Process) bool { return false }
