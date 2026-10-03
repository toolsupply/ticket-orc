package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	switch strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe") {
	case "ticket":
		runTicket()
	case "codex":
		runCodex()
	case "pi":
		runPi()
	case "claude":
		runClaude()
	default:
		os.Exit(2)
	}
}

func runPi() {
	call := map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "args": os.Args[1:], "pid": os.Getpid()}
	data, _ := json.Marshal(call)
	appendLine(os.Getenv("TICKET_ORC_FAKE_PI_LOG"), string(data))
	fmt.Fprintln(os.Stdout, `{"type":"session","id":"fake-session-a"}`)
	fmt.Fprintln(os.Stdout, `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"pi done"}]}}`)
	fmt.Fprintln(os.Stdout, `{"type":"turn_end"}`)
}

func runClaude() {
	call := map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "args": os.Args[1:], "pid": os.Getpid()}
	data, _ := json.Marshal(call)
	appendLine(os.Getenv("TICKET_ORC_FAKE_CLAUDE_LOG"), string(data))
	fmt.Fprintln(os.Stdout, `{"type":"system","subtype":"init","session_id":"fake-session-b"}`)
	fmt.Fprintln(os.Stdout, `{"type":"assistant","session_id":"fake-session-b","message":{"role":"assistant","content":[{"type":"text","text":"claude done"}]}}`)
	fmt.Fprintln(os.Stdout, `{"type":"result","subtype":"success","session_id":"fake-session-b"}`)
}

func runTicket() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Fprintln(os.Stdout, "ticket 0.2.3 (api 2, storage 3)")
		return
	}
	logPath := os.Getenv("TICKET_ORC_FAKE_TICKET_LOG")
	if watchArgs := ticketWatchArgs(os.Args[1:]); len(watchArgs) != 0 {
		runTicketWatch(logPath, watchArgs)
		return
	}
	if len(os.Args) > 1 {
		args := os.Args[1:]
		switch {
		case equalArgs(args, "actor", "-j"):
			appendLine(logPath, strings.Join(args, " "))
			actor := os.Getenv("TICKET_ORC_FAKE_ACTOR")
			if actor == "" {
				actor = os.Getenv("TICKET_ACTOR")
			}
			_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"actor": actor})
			return
		case equalArgs(args, "check", "--active", "-j"):
			appendLine(logPath, strings.Join(args, " "))
			if os.Getenv("TICKET_ORC_FAKE_ACTIVE_FAIL") == "1" {
				fmt.Fprintln(os.Stderr, "fake active metadata failure")
				os.Exit(1)
			}
			fmt.Fprintln(os.Stdout, `{"ok":true}`)
			return
		}
	}
	for _, arg := range os.Args[1:] {
		if arg != "info" {
			continue
		}
		appendLine(logPath, strings.Join(os.Args[1:], " "))
		if os.Getenv("TICKET_ORC_FAKE_INFO_ERROR") == "1" {
			fmt.Fprintln(os.Stderr, "fake Ticket info failure")
			os.Exit(1)
		}
		repository := os.Getenv("TICKET_ORC_FAKE_INFO_PATH")
		if repository == "" {
			repository = "/fake/tickets"
		}
		repositoryID := strings.TrimSpace(os.Getenv("TICKET_ORC_FAKE_REPOSITORY_ID"))
		if repositoryID == "" {
			repositoryID = fakeRepositoryID(repository)
		}
		repositoryName := strings.TrimSpace(os.Getenv("TICKET_ORC_FAKE_INFO_NAME"))
		if repositoryName == "" {
			repositoryName = filepath.Base(filepath.Clean(repository))
		}
		result := map[string]any{"path": repository, "name": repositoryName, "id": repositoryID, "format_version": 1, "storage_version": 1}
		for offset := 1; offset+1 < len(os.Args); offset++ {
			if os.Args[offset] == "--scope" {
				result["scope"] = os.Args[offset+1]
				break
			}
		}
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return
	}
	appendLine(os.Getenv("TICKET_ORC_FAKE_TICKET_PID_LOG"), fmt.Sprintf("%d", os.Getpid()))
	ticketID := os.Getenv("TICKET_ORC_FAKE_TICKET_ID")
	state := os.Getenv("TICKET_ORC_FAKE_TICKET_STATE")
	claimState := os.Getenv("TICKET_ORC_FAKE_CLAIM_STATE")
	stateSequence := strings.Split(os.Getenv("TICKET_ORC_FAKE_TICKET_STATE_SEQUENCE"), ",")
	stateObservations := 0
	claimed := false
	readyObservations := 0
	activeObservations := 0
	queue := os.Getenv("TICKET_ORC_FAKE_QUEUE")
	if queue == "" {
		queue = "open"
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			Args []string `json:"args"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			return
		}
		if len(request.Args) > 0 {
			appendLine(logPath, strings.Join(request.Args, " "))
		}
		activeQueue := activeObservationQueue(request.Args)
		switch {
		case ticketClaimQueue(request.Args) != "":
			if os.Getenv("TICKET_ORC_FAKE_NO_CLAIM") == "1" {
				_ = encoder.Encode(map[string]any{"error": map[string]any{"code": "queue-empty", "message": "fake queue exhausted"}})
				continue
			}
			if claimed {
				_ = encoder.Encode(map[string]any{"error": map[string]any{"code": "queue-empty", "message": "fake queue exhausted"}})
				continue
			}
			claimed = true
			observedClaimState := claimState
			if ticketClaimQueue(request.Args) == "review" && os.Getenv("TICKET_ORC_FAKE_REVIEW_CLAIM_STATE") != "" {
				observedClaimState = os.Getenv("TICKET_ORC_FAKE_REVIEW_CLAIM_STATE")
			}
			_ = encoder.Encode(map[string]any{"item": map[string]any{
				"id": ticketID, "state": observedClaimState, "assignee": os.Getenv("TICKET_ACTOR"),
			}})
		case len(request.Args) == 2 && request.Args[0] == "show" && request.Args[1] == ticketID:
			if os.Getenv("TICKET_ORC_FAKE_TICKET_SHOW_BLOCK") == "1" {
				for {
					time.Sleep(time.Hour)
				}
			}
			observedState := state
			if os.Getenv("TICKET_ACTOR") == "reviewer" && os.Getenv("TICKET_ORC_FAKE_REVIEW_TICKET_STATE") != "" {
				observedState = os.Getenv("TICKET_ORC_FAKE_REVIEW_TICKET_STATE")
			}
			if len(stateSequence) > 0 && stateSequence[0] != "" {
				if stateObservations < len(stateSequence) {
					observedState = stateSequence[stateObservations]
				} else {
					observedState = stateSequence[len(stateSequence)-1]
				}
				stateObservations++
			}
			_ = encoder.Encode(map[string]any{"id": ticketID, "state": observedState, "assignee": os.Getenv("TICKET_ACTOR")})
		case exactObservationArgs(request.Args, "ready", os.Getenv("TICKET_ORC_FAKE_QUEUE")):
			ready := sequenceValue("TICKET_ORC_FAKE_READY_SEQUENCE", readyObservations, os.Getenv("TICKET_ORC_FAKE_READY") == "1")
			readyObservations++
			if ready {
				_ = encoder.Encode(map[string]any{"items": []any{map[string]any{"id": ticketID, "state": queue}}})
			} else {
				_ = encoder.Encode(map[string]any{"items": []any{}})
			}
		case activeQueue != "":
			active := sequenceValue("TICKET_ORC_FAKE_ACTIVE_SEQUENCE", activeObservations, os.Getenv("TICKET_ORC_FAKE_ACTIVE") == "1")
			activeObservations++
			if active {
				_ = encoder.Encode(map[string]any{"items": []any{map[string]any{"id": ticketID, "state": activeQueue, "assignee": os.Getenv("TICKET_ACTOR")}}})
			} else {
				_ = encoder.Encode(map[string]any{"items": []any{}})
			}
		case ownedObservationArgs(request.Args, os.Getenv("TICKET_ORC_FAKE_QUEUE")):
			active := sequenceValue("TICKET_ORC_FAKE_ACTIVE_SEQUENCE", activeObservations, os.Getenv("TICKET_ORC_FAKE_ACTIVE") == "1")
			activeObservations++
			if active {
				_ = encoder.Encode(map[string]any{"items": []any{map[string]any{"id": ticketID, "state": queue, "assignee": os.Getenv("TICKET_ACTOR")}}, "more": false})
			} else {
				_ = encoder.Encode(map[string]any{"items": []any{}, "more": false})
			}
		case len(request.Args) == 2 && request.Args[0] == "release":
			_ = encoder.Encode(map[string]any{"id": request.Args[1], "changed": true, "state": "open"})
		default:
			_ = encoder.Encode(map[string]any{"error": map[string]any{"code": "unsupported", "message": "unsupported fake ticket command"}})
		}
	}
}

func ticketWatchArgs(args []string) []string {
	for i := range args {
		if equalArgs(args[i:], "watch", "-j", "--ready") {
			return args[i:]
		}
	}
	return nil
}

func runTicketWatch(logPath string, args []string) {
	appendLine(logPath, strings.Join(args, " "))
	repositoryID := os.Getenv("TICKET_ORC_FAKE_REPOSITORY_ID")
	if repositoryID == "" {
		repository := os.Getenv("TICKET_ORC_FAKE_INFO_PATH")
		if repository == "" {
			repository = "/fake/tickets"
		}
		repositoryID = fakeRepositoryID(repository)
	}
	ticketID := os.Getenv("TICKET_ORC_FAKE_TICKET_ID")
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(map[string]string{"type": "ready", "repository_id": repositoryID}); err != nil {
		return
	}
	eventPath := os.Getenv("TICKET_ORC_FAKE_WATCH_EVENT_FILE")
	if eventPath == "" {
		for {
			time.Sleep(time.Hour)
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if _, err := os.Stat(eventPath); err != nil {
			continue
		}
		_ = encoder.Encode(map[string]string{"ticket": ticketID, "event": "edited", "state": "open"})
		for {
			time.Sleep(time.Hour)
		}
	}
}

func fakeRepositoryID(repository string) string {
	// Keep fixture identities stable and distinct for repositories that use
	// different paths, matching Ticket's stable repository ID contract.
	hash := sha256.Sum256([]byte(filepath.Clean(repository)))
	identifier := append([]byte(nil), hash[:16]...)
	identifier[6] = identifier[6]&0x0f | 0x40
	identifier[8] = identifier[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", identifier[0:4], identifier[4:6], identifier[6:8], identifier[8:10], identifier[10:16])
}

func sequenceValue(name string, index int, fallback bool) bool {
	values := strings.Split(strings.TrimSpace(os.Getenv(name)), ",")
	if len(values) == 0 || values[0] == "" {
		return fallback
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return strings.TrimSpace(values[index]) == "1" || strings.EqualFold(strings.TrimSpace(values[index]), "true")
}

func exactObservationArgs(args []string, command, queue string) bool {
	if queue == "" {
		queue = "open"
	}
	if command != "ready" || len(args) < 2 || args[0] != command || args[1] != queue {
		return false
	}
	index := 2
	for index < len(args) && (args[index] == "--tag" || args[index] == "--without-tag") {
		if index+1 >= len(args) || args[index+1] == "" {
			return false
		}
		index += 2
	}
	if len(args)-index != 4 || args[index] != "--limit" || args[index+2] != "--fields" {
		return false
	}
	if args[index+1] != "1" && args[index+1] != "256" {
		return false
	}
	return args[index+3] == "id,title,state,assignee,priority" || args[index+3] == "id"
}

func ticketClaimQueue(args []string) string {
	if len(args) < 2 || args[len(args)-1] != "--claim" || (args[0] != "wait" && args[0] != "next") {
		return ""
	}
	queue := "open"
	index := 1
	if index < len(args)-1 && args[index] == "review" {
		queue = "review"
		index++
	}
	for index < len(args)-1 {
		if (args[index] != "--tag" && args[index] != "--without-tag") || index+1 >= len(args)-1 || args[index+1] == "" {
			return ""
		}
		index += 2
	}
	return queue
}

func activeObservationQueue(args []string) string {
	for _, queue := range []string{"open", "review"} {
		if equalArgs(args, "list", "--state", queue, "--assignee", os.Getenv("TICKET_ACTOR"), "--limit", "1", "--fields", "id,title,state,assignee,priority") || equalArgs(args, "list", "--state", queue, "--assignee", os.Getenv("TICKET_ACTOR"), "--limit", "1", "--fields", "id") {
			return queue
		}
	}
	return ""
}

func ownedObservationArgs(args []string, queue string) bool {
	if queue == "" {
		queue = "open"
	}
	return equalArgs(args, "list", "--state", queue, "--assignee", os.Getenv("TICKET_ACTOR"), "--limit", "256", "--fields", "id,state,assignee")
}

func runCodex() {
	if len(os.Args) > 1 && os.Args[1] != "exec" {
		prompt := ""
		args := os.Args[1:]
		for i, arg := range args {
			if arg == "--message" && i+1 < len(args) {
				prompt = args[i+1]
			}
		}
		call := map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "args": args, "prompt": prompt, "pid": os.Getpid()}
		data, _ := json.Marshal(call)
		appendLine(os.Getenv("TICKET_ORC_FAKE_CODEX_LOG"), string(data))
		blockQueue := os.Getenv("TICKET_ORC_FAKE_CODEX_BLOCK_QUEUE") == "1"
		if prompt := os.Getenv("TICKET_ORC_FAKE_CODEX_BLOCK_QUEUE_PROMPT"); prompt != "" && strings.Contains(callPrompt(args), prompt) {
			blockQueue = true
		}
		if marker := os.Getenv("TICKET_ORC_FAKE_CODEX_BLOCK_QUEUE_FILE"); marker != "" {
			if _, err := os.Stat(marker); err == nil {
				blockQueue = true
			}
		}
		isHelp := len(args) == 2 && args[0] == "queue" && args[1] == "--help"
		if !isHelp {
			var descendant *exec.Cmd
			if pidPath := os.Getenv("TICKET_ORC_FAKE_CODEX_DESCENDANT_PID_LOG"); pidPath != "" {
				descendant = exec.Command("sh", "-c", "trap '' INT; sleep 30")
				if err := descendant.Start(); err == nil {
					appendLine(pidPath, strconv.Itoa(descendant.Process.Pid))
				}
			}
			if blockQueue {
				for {
					time.Sleep(time.Hour)
				}
			}
			if descendant != nil && os.Getenv("TICKET_ORC_FAKE_CODEX_DESCENDANT_SURVIVE") != "1" {
				defer func() {
					_ = descendant.Process.Kill()
					_ = descendant.Wait()
				}()
			}
		}
		return
	}
	prompt := ""
	for _, arg := range os.Args {
		if strings.Contains(arg, " ticket ") || strings.HasPrefix(arg, "Work ") || strings.HasPrefix(arg, "Review ") {
			prompt = arg
		}
	}
	call := map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "args": os.Args[1:], "prompt": prompt, "pid": os.Getpid()}
	data, _ := json.Marshal(call)
	appendLine(os.Getenv("TICKET_ORC_FAKE_CODEX_LOG"), string(data))
	if os.Getenv("TICKET_ORC_FAKE_CODEX_FAIL_BEFORE_SESSION") == "1" {
		fmt.Fprintln(os.Stderr, "example startup failure")
		os.Exit(7)
	}
	fmt.Fprintln(os.Stdout, `{"type":"thread.started","thread_id":"fake-thread"}`)
	fmt.Fprintln(os.Stdout, `{"type":"turn.completed"}`)
	if os.Getenv("TICKET_ORC_FAKE_CODEX_FAIL") == "1" {
		os.Exit(7)
	}
	if os.Getenv("TICKET_ORC_FAKE_CODEX_BLOCK") == "1" {
		for {
			time.Sleep(time.Hour)
		}
	}
}

func callPrompt(args []string) string {
	for i, arg := range args {
		if arg == "--message" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func appendLine(path, line string) {
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = fmt.Fprintln(file, line)
}

func equalArgs(args []string, want ...string) bool {
	if len(args) != len(want) {
		return false
	}
	for i := range want {
		if args[i] != want[i] {
			return false
		}
	}
	return true
}
