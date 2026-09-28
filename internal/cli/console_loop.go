package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"io"
	"os"
	"time"
)

func executeAttach(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	options, positional, help, err := parseDaemonCommandOptions(args, lookupEnv, false)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		writeAttachHelp(stdout)
		return 0
	}
	if len(positional) != 0 {
		return usageError(stderr, "attach accepts no positional arguments")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runInteractiveConsoleWithOptions(ctx, options, os.Stdin, stdout, stderr); err != nil {
		return daemonCommandError(stderr, err)
	}
	return 0
}

// runConsole serves both run -i and attach. All mutations go through the
// authenticated daemon client; the console never touches manager state or
// worker processes directly.
func runConsole(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer) error {
	return runConsoleLoopWithSelection(ctx, stateDir, input, output, errorOutput, nil, "", true)
}

func runConsoleLoop(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer, interrupts <-chan os.Signal) error {
	return runConsoleLoopWithSelection(ctx, stateDir, input, output, errorOutput, interrupts, "", true)
}

func runConsoleLoopWithSelection(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer, interrupts <-chan os.Signal, initialSelection string, fallbackTitle bool) error {
	return runConsoleLoopWithSelectionPolicy(ctx, stateDir, input, output, errorOutput, interrupts, initialSelection, fallbackTitle, nil)
}

func runConsoleLoopWithSelectionPolicy(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer, interrupts <-chan os.Signal, initialSelection string, fallbackTitle bool, onConsoleExit func()) error {
	return runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{localDir: stateDir, environmentURL: os.Getenv("TICKET_ORC_ENDPOINT")}, input, output, errorOutput, interrupts, initialSelection, fallbackTitle, onConsoleExit)
}

func runConsoleLoopWithEndpointOptions(ctx context.Context, options daemonCommandOptions, input io.Reader, output, errorOutput io.Writer, interrupts <-chan os.Signal, initialSelection string, fallbackTitle bool, onConsoleExit func()) error {
	if ctx == nil {
		return errors.New("console context must not be nil")
	}
	client, err := daemonclient.NewWithEndpoint(options.localDir, options.endpoint, options.environmentURL)
	if err != nil {
		return err
	}
	consoleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	lines := make(chan string, 1)
	events := make(chan daemon.Event, 16)
	eventErrors := make(chan error, 1)
	renderer := newConsoleRenderer(output, errorOutput)
	watchRenderer := &consoleWatchRenderer{}
	go readConsoleLinesWithPartial(consoleCtx, input, lines, renderer.setPartialInput)

	renderConsoleBanner(renderer.writer())
	tracker := newConsoleEventTracker()
	if status, statusErr := client.Status(consoleCtx); statusErr == nil {
		tracker.seed(status)
		watchRenderer.seedRepositories(status.Repositories)
		renderConsoleStatus(renderer.writer(), status)
	} else {
		writeConsoleError(renderer.errorWriter(), statusErr)
	}
	go readConsoleEvents(consoleCtx, client, events, eventErrors)
	setConsoleStartupTitle(initialSelection, fallbackTitle)
	renderer.promptLine()
	selectedWorker := initialSelection
	watching := false
	var pendingErrors []string
	var lastInterrupt time.Time
	for {
		select {
		case <-consoleCtx.Done():
			return nil
		case <-interrupts:
			if watching {
				watching = false
				lastInterrupt = time.Time{}
				fmt.Fprintln(renderer.writer(), "watch stopped")
				renderer.promptLine()
			} else if onConsoleExit != nil && !lastInterrupt.IsZero() && time.Since(lastInterrupt) <= consoleInterruptWindow {
				fmt.Fprintln(renderer.writer(), "stopping supervisor")
				onConsoleExit()
				return nil
			} else {
				lastInterrupt = time.Now()
				message := "^C"
				if onConsoleExit != nil {
					message = "^C (press Ctrl-C again within 1s to stop)"
				}
				renderer.interruptPrompt(message)
			}
		case event := <-events:
			event = tracker.prepare(event)
			accepted := tracker.accept(event)
			if watching && accepted && consoleWatchEvent(event) {
				renderConsoleWatchActivity(renderer.asyncWriter(), watchRenderer, options.localDir, event, time.Now())
			}
		case eventErr := <-eventErrors:
			if eventErr != nil && !errors.Is(eventErr, context.Canceled) && !errors.Is(eventErr, io.EOF) {
				message := "event stream: " + safeConsoleError(eventErr)
				if watching {
					fmt.Fprintf(renderer.asyncWriter(), "%s\n", message)
				} else {
					// Canonical terminals do not expose partially typed bytes until
					// Enter. Queue stream failures until the next command boundary so
					// no asynchronous write can split the operator's input line.
					pendingErrors = append(pendingErrors, message)
				}
			}
		case line, ok := <-lines:
			if !ok {
				if onConsoleExit != nil {
					fmt.Fprintln(renderer.writer(), "stopping supervisor")
					onConsoleExit()
				} else {
					fmt.Fprintln(renderer.writer(), "detached")
				}
				return nil
			}
			if watching {
				continue
			}
			renderer.commandStart()
			lastInterrupt = time.Time{}
			for _, message := range pendingErrors {
				fmt.Fprintln(renderer.errorWriter(), message)
			}
			pendingErrors = nil
			command, parseErr := parseConsoleCommand(line)
			if parseErr != nil {
				fmt.Fprintf(renderer.errorWriter(), "command: %s\n", parseErr)
				renderer.promptLine()
				continue
			}
			if command.name == "" {
				renderer.promptLine()
				continue
			}
			if command.name == "watch" {
				watching = true
				watchRenderer.date = ""
				fmt.Fprintln(renderer.writer())
				fmt.Fprintln(renderer.writer(), "Watching activity. Press Ctrl-C to return.")
				fmt.Fprintln(renderer.writer())
				fmt.Fprintln(renderer.writer(), consoleWatchHeader())
				continue
			}
			done, actionOK := executeConsoleCommandResultWithConfig(consoleCtx, client, command, options.configPath, renderer.writer(), renderer.errorWriter())
			if done {
				if onConsoleExit != nil && (command.name == "exit" || command.name == "quit" || command.name == "q") {
					fmt.Fprintln(renderer.writer(), "stopping supervisor")
					onConsoleExit()
				}
				return nil
			}
			if nextWorker, changed := consoleSelectionChange(selectedWorker, command, actionOK); changed {
				selectedWorker = nextWorker
				setConsoleWorkerTitle(nextWorker)
			}
			renderer.promptLine()
		}
	}
}

func renderConsoleBanner(out io.Writer) {
	fmt.Fprintln(out, "[ticket-orc] interactive console; type help for commands.")
}

func setConsoleWorkerTitle(worker string) {
	// Attach and run -i share this gate so piped console input never receives
	// terminal control bytes, even when stdout happens to be a TTY.
	if !foregroundTerminalEligible() {
		return
	}
	setTerminalTitle("ticket-orc — " + worker)
}

func setConsoleFallbackTitle() {
	if !foregroundTerminalEligible() {
		return
	}
	setTerminalTitle("ticket-orc — supervisor")
}

func setConsoleStartupTitle(initialSelection string, fallbackTitle bool) {
	if fallbackTitle && initialSelection == "" {
		setConsoleFallbackTitle()
	}
}

// runInteractiveConsole gives Ctrl-C to the console first. The signal cancels
// only the current console wait; the foreground supervisor remains alive and
// continues to own its workers until an explicit shutdown or service signal.
func runInteractiveConsole(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer) error {
	interrupts, stop := consoleSignalChannel()
	defer stop()
	return runConsoleLoopWithSelection(ctx, stateDir, input, output, errorOutput, interrupts, "", true)
}

func runInteractiveConsoleWithOptions(ctx context.Context, options daemonCommandOptions, input io.Reader, output, errorOutput io.Writer) error {
	interrupts, stop := consoleSignalChannel()
	defer stop()
	return runConsoleLoopWithEndpointOptions(ctx, options, input, output, errorOutput, interrupts, "", true, nil)
}

func runInteractiveConsoleWithSelection(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer, initialSelection string) error {
	interrupts, stop := consoleSignalChannel()
	defer stop()
	return runConsoleLoopWithSelection(ctx, stateDir, input, output, errorOutput, interrupts, initialSelection, false)
}

func runInteractiveSupervisorConsoleWithSelection(ctx context.Context, stateDir string, input io.Reader, output, errorOutput io.Writer, initialSelection string, onConsoleExit func()) error {
	interrupts, stop := consoleSignalChannel()
	defer stop()
	return runConsoleLoopWithEndpointOptions(ctx, daemonCommandOptions{localDir: stateDir}, input, output, errorOutput, interrupts, initialSelection, false, onConsoleExit)
}

func readConsoleLines(ctx context.Context, input io.Reader, lines chan<- string) {
	readConsoleLinesWithPartial(ctx, input, lines, nil)
}

func readConsoleLinesWithPartial(ctx context.Context, input io.Reader, lines chan<- string, partial func(string)) {
	defer close(lines)
	if input == nil {
		return
	}
	reader := bufio.NewReader(input)
	line := make([]byte, 0, 256)
	for {
		value, err := reader.ReadByte()
		if err != nil {
			if len(line) > 0 {
				select {
				case lines <- string(line):
				case <-ctx.Done():
					return
				}
			}
			return
		}
		if value == '\r' {
			continue
		}
		if value == '\n' {
			if partial != nil {
				partial("")
			}
			select {
			case lines <- string(line):
				line = line[:0]
			case <-ctx.Done():
				return
			}
			continue
		}
		if len(line) <= consoleMaxLine {
			line = append(line, value)
		}
		if partial != nil {
			partial(string(line))
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
