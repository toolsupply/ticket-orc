//go:build !linux && !darwin && !windows

package cli

import "os"

// Unsupported Unix targets retain the conservative pre-existing behavior.
func waitForChildExitBeforeReap(_ *os.Process) error { return os.ErrInvalid }
