//go:build !windows

package state

import (
	"context"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestReadRejectsSpecialStateFileBeforeOpening(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, stateFileName)
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAdministrative(dir).Read(context.Background()); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("special state file accepted: %v", err)
	}
}
