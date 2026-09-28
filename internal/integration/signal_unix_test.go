//go:build !windows

package integration

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestExecutableSIGTERMCancelsChildrenWithoutRespawn(t *testing.T) {
	binary, helperDir := buildFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(stateDir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"id":"1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa","local_dir":"."}`), 0o600); err != nil {
		t.Fatal(err)
	}
	codexLog := filepath.Join(t.TempDir(), "codex.jsonl")
	ticketLog := filepath.Join(t.TempDir(), "ticket.log")
	ticketPIDLog := filepath.Join(t.TempDir(), "ticket.pid")
	cmd := exec.Command(binary, "coder", "--actor", "signal-coder", "--config", configPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = testEnv(helperDir, map[string]string{
		"TICKET_ORC_FAKE_TICKET_STATE":   "review",
		"TICKET_ORC_FAKE_CLAIM_STATE":    "open",
		"TICKET_ORC_FAKE_TICKET_LOG":     ticketLog,
		"TICKET_ORC_FAKE_TICKET_PID_LOG": ticketPIDLog,
		"TICKET_ORC_FAKE_CODEX_LOG":      codexLog,
		"TICKET_ORC_FAKE_CODEX_BLOCK":    "1",
		"TICKET_ORC_FAKE_TICKET_ID":      integrationTicketID,
	}, map[string]string{"TICKET_ORC_ACTOR": "signal-coder"})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start role: %v", err)
	}
	waitForFile(t, codexLog)
	calls := readCodexCalls(t, codexLog)
	if len(calls) != 1 {
		t.Fatalf("Codex calls before signal = %#v", calls)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	waitErr := waitCommand(t, cmd)
	if waitErr == nil {
		t.Fatal("role exited successfully after SIGTERM")
	}
	if got := len(readCodexCalls(t, codexLog)); got != 1 {
		t.Fatalf("Codex calls after signal = %d, want one; stderr=%q", got, stderr.String())
	}
	pidText, err := os.ReadFile(ticketPIDLog)
	if err != nil {
		t.Fatalf("read ticket PID: %v", err)
	}
	ticketPID, err := strconv.Atoi(string(bytes.TrimSpace(pidText)))
	if err != nil {
		t.Fatalf("ticket PID %q: %v", pidText, err)
	}
	waitProcessGone(t, calls[0].PID)
	waitProcessGone(t, ticketPID)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitCommand(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("role did not exit after SIGTERM")
		return nil
	}
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	if pid <= 0 {
		t.Fatalf("invalid child PID %d", pid)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("child process %d is still alive", pid)
}
