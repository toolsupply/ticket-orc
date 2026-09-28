package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestAppendConsoleWatchActivityAppendsNDJSON(t *testing.T) {
	stateDir := t.TempDir()
	want := []daemon.Event{
		{Seq: 1, Type: "ticket.claim", Ticket: "20260926-00001"},
		{Seq: 2, Type: "ticket.repository_changed", Ticket: "20260926-00002", RepositoryID: joinTestRepositoryID, RepositoryKey: "project"},
	}
	for _, event := range want {
		if err := appendConsoleWatchActivity(stateDir, event); err != nil {
			t.Fatal(err)
		}
	}

	file, err := os.Open(filepath.Join(stateDir, "logs", "activity.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for i := range want {
		if !scanner.Scan() {
			t.Fatalf("activity line %d missing: %v", i+1, scanner.Err())
		}
		var got daemon.Event
		if err := json.Unmarshal(scanner.Bytes(), &got); err != nil {
			t.Fatalf("activity line %d is invalid JSON: %v", i+1, err)
		}
		if got != want[i] {
			t.Fatalf("activity line %d = %#v, want %#v", i+1, got, want[i])
		}
	}
	if scanner.Scan() {
		t.Fatalf("unexpected extra activity line: %q", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendConsoleWatchActivityBoundsHistory(t *testing.T) {
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "logs", "activity.jsonl")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, bytes.Repeat([]byte{'x'}, consoleActivityLogMaxBytes-1), 0o600); err != nil {
		t.Fatal(err)
	}
	want := []daemon.Event{
		{Seq: 3, Type: "ticket.claim", Ticket: "20260926-00003"},
		{Seq: 4, Type: "ticket.claim", Ticket: "20260926-00004"},
	}
	for _, event := range want {
		if err := appendConsoleWatchActivity(stateDir, event); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > consoleActivityLogMaxBytes {
		t.Fatalf("activity log size=%d exceeds limit=%d", len(data), consoleActivityLogMaxBytes)
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for i, event := range want {
		if !scanner.Scan() {
			t.Fatalf("bounded activity line %d missing: %v", i+1, scanner.Err())
		}
		var got daemon.Event
		if err := json.Unmarshal(scanner.Bytes(), &got); err != nil {
			t.Fatalf("bounded activity line %d is invalid JSON: %v", i+1, err)
		}
		if got != event {
			t.Fatalf("bounded activity line %d = %#v, want %#v", i+1, got, event)
		}
	}
	if scanner.Scan() {
		t.Fatalf("bounded activity log retained unexpected data: %q", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendConsoleWatchActivityConcurrentProcessesStayBounded(t *testing.T) {
	const writers = 8
	stateDir := t.TempDir()
	commands := make([]*exec.Cmd, 0, writers)
	waited := make([]bool, 0, writers)
	outputs := make([]bytes.Buffer, writers)
	t.Cleanup(func() {
		for i, command := range commands {
			if waited[i] {
				continue
			}
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			_ = command.Wait()
			waited[i] = true
		}
	})
	for i := 0; i < writers; i++ {
		command := exec.Command(os.Args[0], "-test.run=^TestAppendConsoleWatchActivityWriterProcess$")
		command.Stdout = &outputs[i]
		command.Stderr = &outputs[i]
		command.Env = append(os.Environ(),
			"TICKET_ORC_ACTIVITY_LOG_TEST_STATE="+stateDir,
			"TICKET_ORC_ACTIVITY_LOG_TEST_WRITER="+strconv.Itoa(i),
		)
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
		waited = append(waited, false)
	}
	for i, command := range commands {
		err := command.Wait()
		waited[i] = true
		if err != nil {
			t.Fatalf("activity writer process failed: %v\n%s", err, outputs[i].String())
		}
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "logs", "activity.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > consoleActivityLogMaxBytes {
		t.Fatalf("concurrent activity log size=%d exceeds limit=%d", len(data), consoleActivityLogMaxBytes)
	}
	for i, line := range bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'}) {
		var event daemon.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("concurrent activity line %d is invalid JSON: %v", i+1, err)
		}
	}
}

func TestAppendConsoleWatchActivityWriterProcess(t *testing.T) {
	stateDir := os.Getenv("TICKET_ORC_ACTIVITY_LOG_TEST_STATE")
	if stateDir == "" {
		return
	}
	writer := os.Getenv("TICKET_ORC_ACTIVITY_LOG_TEST_WRITER")
	event := daemon.Event{Seq: 1, Type: "ticket.claim", Worker: writer + ":" + strings.Repeat("x", consoleActivityLogMaxBytes/4)}
	if err := appendConsoleWatchActivity(stateDir, event); err != nil {
		t.Fatal(err)
	}
}

func TestAppendConsoleWatchActivityRejectsOversizedRecord(t *testing.T) {
	stateDir := t.TempDir()
	event := daemon.Event{Type: "ticket.claim", Worker: string(bytes.Repeat([]byte{'x'}, consoleActivityLogMaxBytes))}
	if err := appendConsoleWatchActivity(stateDir, event); err == nil {
		t.Fatal("oversized activity record was accepted")
	}
	if _, err := os.Stat(filepath.Join(stateDir, "logs", "activity.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("oversized record created activity log: err=%v", err)
	}
}

func TestConsoleWatchRendersEventWhenActivityLogFails(t *testing.T) {
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "logs", "activity.jsonl")
	if err := os.MkdirAll(logPath, 0o700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	event := daemon.Event{Type: "ticket.claim", Worker: "coder", Ticket: "20260926-00003"}
	renderConsoleWatchActivity(&output, &consoleWatchRenderer{}, stateDir, event, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	text := output.String()
	if !strings.Contains(text, "activity log: failed to append watch event") || !strings.Contains(text, "coder") || !strings.Contains(text, event.Ticket) || !strings.Contains(text, "claimed") {
		t.Fatalf("watch output after log failure=%q", text)
	}
	if strings.Contains(text, "daemon request failed") {
		t.Fatalf("filesystem failure was misreported as daemon failure: %q", text)
	}
}
