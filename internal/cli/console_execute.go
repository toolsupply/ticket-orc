package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
)

func executeConsoleCommand(ctx context.Context, client *daemonclient.Client, command consoleCommand, output, errorOutput io.Writer) bool {
	done, _ := executeConsoleCommandResult(ctx, client, command, output, errorOutput)
	return done
}

// executeConsoleCommandResult reports whether the command ended the console
// and whether its daemon action succeeded. The latter controls human-facing
// selection state, so failed actions never change the terminal title.
func executeConsoleCommandResult(ctx context.Context, client *daemonclient.Client, command consoleCommand, output, errorOutput io.Writer) (done, actionOK bool) {
	return executeConsoleCommandResultWithConfig(ctx, client, command, "", output, errorOutput)
}

func executeConsoleCommandResultWithConfig(ctx context.Context, client *daemonclient.Client, command consoleCommand, configPath string, output, errorOutput io.Writer) (done, actionOK bool) {
	if command.name == "help" || command.name == "?" {
		if len(command.args) == 1 {
			writeConsoleTopicHelp(output, command.args[0])
		} else {
			writeConsoleHelp(output)
		}
		return false, true
	}
	if command.name == "exit" || command.name == "quit" || command.name == "q" {
		return true, true
	}
	if command.name == "workers" {
		if len(command.args) == 0 {
			status, err := client.Status(ctx)
			if err != nil {
				writeConsoleError(errorOutput, err)
				return false, false
			}
			renderConsoleWorkers(output, status)
			return false, true
		}
		return executeConsoleCommandResult(ctx, client, consoleCommand{name: "status", args: command.args}, output, errorOutput)
	}
	if command.name == "worker" && len(command.args) >= 1 && command.args[0] == "status" {
		return executeConsoleCommandResult(ctx, client, consoleCommand{name: "status", args: command.args[1:]}, output, errorOutput)
	}
	if command.name == "status" {
		status, err := client.Status(ctx)
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		if len(command.args) == 1 && command.args[0] == "detail" {
			renderConsoleDetailStatus(output, status)
		} else if len(command.args) == 1 && command.args[0] == "verbose" {
			renderConsoleVerboseStatus(output, status)
		} else if len(command.args) == 1 {
			for _, worker := range status.Workers {
				if worker.Name == command.args[0] {
					renderConsoleDetailStatus(output, daemon.Status{Mode: status.Mode, Workers: []daemon.WorkerStatus{worker}})
					return false, true
				}
			}
			fmt.Fprintf(errorOutput, "error: unknown worker %s; usage: status NAME|verbose|detail\n", command.args[0])
			return false, false
		} else {
			renderConsoleStatus(output, status)
		}
		return false, true
	}
	if command.name == "queue" {
		status, err := queueStatus(ctx, client)
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		values := map[string]string{}
		if configPath != "" {
			values["config"] = configPath
		}
		loaded, err := loadInvocationConfig(values, os.LookupEnv)
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		queueCtx, cancel := context.WithTimeout(ctx, steerCommandTimeout)
		defer cancel()
		if err := writeQueueForecast(queueCtx, output, false, loaded, status, openLocalTicketReader); err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		return false, true
	}
	if command.name == "daemon" {
		var result daemon.DaemonControlResult
		var err error
		switch command.args[0] {
		case "pause":
			result, err = client.Pause(ctx)
		case "resume":
			result, err = client.Resume(ctx)
		case "abort":
			result, err = client.Abort(ctx)
		}
		if command.args[0] == "abort" {
			if err == nil || result.Mode != "" || len(result.Targets) > 0 {
				renderDaemonAbortCompact(output, result)
			}
		} else if err == nil {
			fmt.Fprintf(output, "daemon mode=%s mutation_applied=%t\n", result.Mode, result.Applied)
		}
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		return false, true
	}
	if command.name == "shutdown" {
		if len(command.args) != 1 || command.args[0] != "confirm" {
			fmt.Fprintln(output, "shutdown requires confirmation: shutdown confirm")
			return false, false
		}
		if err := client.Shutdown(ctx); err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		fmt.Fprintln(output, "shutdown requested")
		return true, true
	}
	if command.name == "reload" {
		result, err := client.Reload(ctx)
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		if result.Applied {
			fmt.Fprintln(output, "configuration reloaded")
		} else {
			fmt.Fprintln(output, "configuration unchanged")
		}
		return false, true
	}
	if command.name == "doctor" {
		result, err := client.Doctor(ctx)
		if err != nil {
			writeConsoleError(errorOutput, err)
			return false, false
		}
		renderConsoleDoctor(output, result)
		return false, true
	}
	if command.name == "group" {
		if command.args[1] == "all" {
			return executeConsoleAllWorkers(ctx, client, command.args[0], output, errorOutput)
		}
		result, err := client.Group(ctx, command.args[1], command.args[0])
		if err != nil {
			if result.Group != "" {
				renderConsoleGroup(output, result, command.args[0])
			}
			writeConsoleError(errorOutput, err)
			return false, false
		}
		renderConsoleGroup(output, result, command.args[0])
		return false, true
	}
	operation, name := command.name, ""
	if command.name == "worker" {
		operation, name = command.args[0], command.args[1]
	} else {
		name = command.args[0]
	}
	if (operation == "start" || operation == "stop" || operation == "restart" || operation == "pause" || operation == "resume") && name == "all" {
		return executeConsoleAllWorkers(ctx, client, operation, output, errorOutput)
	}
	result, err := client.Worker(ctx, name, operation)
	if err != nil {
		if result.Worker != "" {
			renderConsoleMutation(output, result, operation)
		}
		writeConsoleError(errorOutput, err)
		return false, false
	}
	renderConsoleMutation(output, result, operation)
	if operation == "pause" || operation == "resume" {
		if status, statusErr := client.Status(ctx); statusErr == nil {
			for _, worker := range status.Workers {
				if worker.Name == name {
					fmt.Fprintf(output, "worker %s is %s (%s)\n", consoleSummaryName(worker.Name), consoleField(worker.State), consoleWorkerSummary(worker))
				}
			}
		}
	}
	return false, true
}

func executeConsoleAllWorkers(ctx context.Context, client *daemonclient.Client, operation string, output, errorOutput io.Writer) (bool, bool) {
	workers, err := client.Workers(ctx)
	if err != nil {
		writeConsoleError(errorOutput, err)
		return false, false
	}
	sort.SliceStable(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })
	if len(workers) == 0 {
		fmt.Fprintf(output, "%s all: no configured workers\n", operation)
		return false, true
	}
	succeeded, failed := 0, 0
	for _, worker := range workers {
		if worker.Name == "" {
			continue
		}
		result, workerErr := client.Worker(ctx, worker.Name, operation)
		if result.Worker != "" {
			renderConsoleMutation(output, result, operation)
		}
		if workerErr != nil {
			failed++
			if result.Worker == "" {
				fmt.Fprintf(output, "worker %s request failed\n", consoleSummaryName(worker.Name))
			}
			writeConsoleError(errorOutput, workerErr)
			continue
		}
		succeeded++
	}
	fmt.Fprintf(output, "%s all: %d succeeded, %d failed\n", operation, succeeded, failed)
	return false, failed == 0
}
