package cli

import (
	"fmt"
	"strings"
)

type consoleCommand struct {
	name string
	args []string
}

// parseConsoleCommand accepts the deliberately small, completion-friendly
// console grammar.
func parseConsoleCommand(line string) (consoleCommand, error) {
	if len(line) > consoleMaxLine {
		return consoleCommand{}, fmt.Errorf("console command exceeds %d bytes", consoleMaxLine)
	}
	line = strings.TrimLeft(line, " \t\r\n")
	if line == "" {
		return consoleCommand{}, nil
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return consoleCommand{}, nil
	}
	command := consoleCommand{name: fields[0], args: fields[1:]}
	if command.name == "list" || command.name == "ls" {
		command.name = "workers"
	}
	if command.name == "help" || command.name == "?" {
		if len(command.args) > 1 || (len(command.args) == 1 && !consoleHelpTopics[command.args[0]]) {
			return consoleCommand{}, fmt.Errorf("usage: help [COMMAND]")
		}
		if command.name == "?" {
			command.name = "help"
		}
		return command, nil
	}
	if topic, ok := consoleContextualHelp(command); ok {
		return consoleCommand{name: "help", args: []string{topic}}, nil
	}
	switch command.name {
	case "watch":
		if len(command.args) != 0 {
			return consoleCommand{}, fmt.Errorf("usage: watch")
		}
	case "exit", "quit", "q", "reload", "doctor":
		if len(command.args) != 0 {
			return consoleCommand{}, fmt.Errorf("%s accepts no arguments", command.name)
		}
	case "status", "workers":
		if len(command.args) > 1 {
			return consoleCommand{}, fmt.Errorf("usage: %s [NAME|verbose|detail]", command.name)
		}
	case "queue":
		if len(command.args) != 0 {
			return consoleCommand{}, fmt.Errorf("usage: queue")
		}
	case "shutdown":
		if len(command.args) > 1 || (len(command.args) == 1 && command.args[0] != "confirm") {
			return consoleCommand{}, fmt.Errorf("usage: shutdown [confirm]")
		}
	case "daemon":
		if len(command.args) != 1 || (command.args[0] != "pause" && command.args[0] != "resume" && command.args[0] != "abort") {
			return consoleCommand{}, fmt.Errorf("usage: daemon pause|resume|abort")
		}
	case "group":
		if len(command.args) != 2 || (command.args[0] != "start" && command.args[0] != "stop") {
			return consoleCommand{}, fmt.Errorf("usage: group start|stop NAME")
		}
	case "worker":
		if len(command.args) == 0 || !consoleWorkerOperations[command.args[0]] {
			return consoleCommand{}, fmt.Errorf("usage: worker status [NAME|verbose|detail] or worker start|stop|restart|pause|resume NAME")
		}
		if command.args[0] == "status" {
			if len(command.args) > 2 {
				return consoleCommand{}, fmt.Errorf("usage: worker %s [NAME|verbose|detail]", command.args[0])
			}
			break
		}
		if len(command.args) != 2 {
			return consoleCommand{}, fmt.Errorf("usage: worker %s NAME", command.args[0])
		}
	case "start", "stop", "restart", "pause", "resume":
		if len(command.args) != 1 {
			return consoleCommand{}, fmt.Errorf("usage: %s NAME", command.name)
		}
	default:
		return consoleCommand{}, fmt.Errorf("unknown console command %q", command.name)
	}
	if len(command.args) > consoleMaxArgs {
		return consoleCommand{}, fmt.Errorf("too many command arguments")
	}
	return command, nil
}

var consoleWorkerOperations = map[string]bool{
	"status": true, "start": true, "stop": true, "restart": true, "pause": true, "resume": true,
}

var consoleHelpTopics = map[string]bool{
	"worker": true, "group": true, "daemon": true, "status": true, "workers": true, "start": true, "stop": true,
	"restart": true, "pause": true, "resume": true, "queue": true,
	"watch": true, "doctor": true, "reload": true, "shutdown": true,
	"exit": true, "quit": true, "q": true,
}

func consoleContextualHelp(command consoleCommand) (string, bool) {
	isHelp := func(value string) bool { return value == "help" || value == "-h" || value == "--help" }
	switch command.name {
	case "worker":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return "worker", true
		}
		if len(command.args) == 2 && consoleWorkerOperations[command.args[0]] && isHelp(command.args[1]) {
			return command.args[0], true
		}
	case "group":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return "group", true
		}
		if len(command.args) == 2 && (command.args[0] == "start" || command.args[0] == "stop") && isHelp(command.args[1]) {
			return "group", true
		}
	case "daemon":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return "daemon", true
		}
		if len(command.args) == 2 && (command.args[0] == "pause" || command.args[0] == "resume" || command.args[0] == "abort") && isHelp(command.args[1]) {
			return "daemon", true
		}
	case "status", "workers":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return command.name, true
		}
	case "start", "stop", "restart", "pause", "resume":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return command.name, true
		}
	case "queue", "watch", "doctor", "reload", "shutdown", "exit", "quit", "q":
		if len(command.args) == 1 && isHelp(command.args[0]) {
			return command.name, true
		}
	}
	return "", false
}

func consoleSelectionChange(current string, command consoleCommand, actionOK bool) (string, bool) {
	if !actionOK {
		return current, false
	}
	worker, ok := consoleCommandWorker(command)
	if !ok || worker == current {
		return current, false
	}
	return worker, true
}

func consoleCommandWorker(command consoleCommand) (string, bool) {
	switch command.name {
	case "worker":
		if len(command.args) == 2 && command.args[1] != "" && command.args[1] != "all" {
			return command.args[1], true
		}
	case "start", "stop", "restart", "pause", "resume":
		if len(command.args) == 1 && command.args[0] != "" && command.args[0] != "all" {
			return command.args[0], true
		}
	}
	return "", false
}
