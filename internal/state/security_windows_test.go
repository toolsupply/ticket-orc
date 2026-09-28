//go:build windows

package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsStateOperationsUseRegularFilesUnderProfile(t *testing.T) {
	stateDir := t.TempDir()
	repository := filepath.Join(stateDir, "repo")
	store := NewForRepository(stateDir, repository)
	want := Session{Repository: repository, Ticket: "ticket", Role: "coder", Harness: "codex", ID: "session"}
	if err := store.SetSession(context.Background(), want); err != nil {
		t.Fatalf("write state under profile ACL: %v", err)
	}
	snapshot, err := store.Read(context.Background())
	if err != nil || len(snapshot.Sessions) != 1 || snapshot.Sessions[0] != want {
		t.Fatalf("read state = %#v, %v", snapshot, err)
	}
	for _, name := range []string{stateFileName, lockFileName} {
		path := filepath.Join(stateDir, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("state path %s info=%v err=%v; want regular file", path, info, err)
		}
	}
	statePath := filepath.Join(stateDir, stateFileName)
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(statePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(context.Background()); err == nil || !strings.Contains(err.Error(), statePath) {
		t.Fatalf("read non-regular state path error=%v; want rejection identifying %s", err, statePath)
	}
}
