//go:build !windows

package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// roleSignalContext cancels a role on the interactive interrupt signal and on
// the termination signal used by service managers on Unix systems.
func roleSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
