//go:build windows

package cli

import (
	"context"
	"os"
	"os/signal"
)

// roleSignalContext handles Ctrl-C on Windows. Windows has no portable
// SIGTERM equivalent in the standard library.
func roleSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt)
}
