package orc

import (
	"path/filepath"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func newTestStateStore(t *testing.T) *state.Store {
	t.Helper()
	dir := t.TempDir()
	return newTestStateStoreAt(t, dir)
}

func newTestStateStoreAt(t *testing.T, dir string) *state.Store {
	t.Helper()
	return state.NewForRepository(dir, filepath.Join(dir, "repo"))
}
