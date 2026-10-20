package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/daemonclient"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

type watchOptions struct {
	selector string
	verbose  bool
	help     bool
}

func (options watchOptions) outputMode() codex.OutputMode {
	if options.verbose {
		return codex.OutputCompact
	}
	return codex.OutputQuiet
}

func parseWatchOptions(args []string) (watchOptions, error) {
	options := watchOptions{selector: "all"}
	selected := false
	for _, arg := range args {
		switch {
		case arg == "--help" || arg == "-h":
			options.help = true
		case arg == "--verbose":
			if options.verbose {
				return watchOptions{}, fmt.Errorf("duplicate option --verbose")
			}
			options.verbose = true
		case strings.HasPrefix(arg, "-"):
			return watchOptions{}, fmt.Errorf("unknown option %s", arg)
		default:
			if selected || arg == "" {
				return watchOptions{}, fmt.Errorf("usage: watch [orc|all|WORKER] [--verbose]")
			}
			options.selector, selected = arg, true
		}
	}
	return options, nil
}

const watchHelp = `
Usage: ticket-orc watch [orc|all|WORKER] [--verbose] [--endpoint URL] [-c|--config PATH]

Bare watch defaults to all: Orc activity and all local managed-worker output.
Rows use [repo:orc] for Orc and [repo:actor] for workers (worker name if no actor);
instance-wide events use [orc]. Explicit watch orc keeps the legacy Orc-only table.
watch WORKER selects one worker. Worker output requires the local daemon; for a
remote endpoint use explicit watch orc. Ctrl-C stops the viewer.
All rows use local viewer observation time. By default, show agent messages,
diagnostics and turn summaries. --verbose also shows readable commands and file changes.

`

func executeWatchCommand(args []string, stdout, stderr io.Writer, lookupEnv envLookup) int {
	options, positional, help, err := parseDaemonOptions(args, lookupEnv, false, true)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	if help {
		fmt.Fprint(stdout, watchHelp)
		return 0
	}
	watchOptions, err := parseWatchOptions(positional)
	if err != nil {
		return usageError(stderr, "%v", err)
	}
	selector := watchOptions.selector
	if options.endpoint != "" || options.environmentURL != "" || options.localDir == "" {
		if selector != "orc" {
			fmt.Fprintln(stderr, "error: "+remoteWorkerWatchError)
			return 1
		}
	}
	client, err := daemonclient.NewWithEndpoint(options.localDir, options.endpoint, options.environmentURL)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	status, err := client.Status(ctx)
	if err != nil {
		return daemonCommandError(stderr, err)
	}
	if selector != "orc" && selector != "all" {
		if err := runStandaloneWorkerWatch(ctx, options.localDir, status, watchOptions, stdout); err != nil {
			return daemonCommandError(stderr, err)
		}
		return 0
	}
	if selector == "all" {
		watch, watchErr := newWorkerLogWatch(options.localDir, status, watchOptions, stdout)
		if watchErr != nil {
			return daemonCommandError(stderr, watchErr)
		}
		fmt.Fprintln(stdout, "Watching Orc activity and local worker output. Press Ctrl-C to stop.")
		return runUnifiedStandaloneWatch(ctx, client, watch, status, stdout, stderr)
	}
	return runStandaloneOrcWatch(ctx, client, status, stdout, stderr)
}

func runStandaloneWorkerWatch(ctx context.Context, localDir string, status daemon.Status, options watchOptions, out io.Writer) error {
	watch, err := newWorkerLogWatch(localDir, status, options, out)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Watching worker %s output. Press Ctrl-C to stop.\n", terminaltext.Sanitize(options.selector, false))
	return runWorkerWatch(ctx, watch)
}

func runStandaloneOrcWatch(ctx context.Context, client *daemonclient.Client, status daemon.Status, out, errorsOut io.Writer) int {
	fmt.Fprintln(out, "Watching Orc activity. Press Ctrl-C to stop.")
	return runStandaloneEvents(ctx, client, nil, status, out, errorsOut)
}

func runUnifiedStandaloneWatch(ctx context.Context, client *daemonclient.Client, watch *workerLogWatch, status daemon.Status, out, errorsOut io.Writer) int {
	defer watch.Close()
	return runStandaloneEvents(ctx, client, watch, status, out, errorsOut)
}

func runStandaloneEvents(ctx context.Context, client *daemonclient.Client, watch *workerLogWatch, status daemon.Status, out, errorsOut io.Writer) int {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	renderer := &consoleWatchRenderer{unified: watch != nil}
	renderer.seedRepositories(status.Repositories)
	tracker := newConsoleEventTracker()
	tracker.seed(status)
	if watch == nil {
		if err := writeWatchOutput(out, consoleWatchHeader()+"\n"); err != nil {
			return daemonCommandError(errorsOut, err)
		}
	}
	events := make(chan daemon.Event, 16)
	eventErrors := make(chan error, 1)
	go readConsoleEvents(ctx, client, events, eventErrors)
	ticker := time.NewTicker(workerWatchPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0
		case now := <-ticker.C:
			if watch != nil {
				if err := watch.poll(now); err != nil {
					_ = writeWatchLines(errorsOut, time.Now(), "orc", "worker output: "+boundedWatchDiagnostic(err.Error()))
					return 1
				}
			}
		case event := <-events:
			repositoryID := event.RepositoryID
			if repositoryID == "" && event.Worker != "" {
				repositoryID = tracker.workers[event.Worker].RepositoryID
			}
			if event.Type == "config.reloaded" || (repositoryID != "" && !renderer.hasRepository(repositoryID)) || (event.Worker != "" && tracker.workers[event.Worker].RepositoryID == "") {
				if refreshed, statusErr := client.Status(ctx); statusErr == nil {
					renderer.seedRepositories(refreshed.Repositories)
					tracker.refreshWorkers(refreshed)
					if watch != nil {
						watch.refresh(refreshed)
					}
				}
			}
			event = tracker.prepare(event)
			if tracker.accept(event) && consoleWatchEvent(event) {
				if err := renderer.render(out, event, time.Now()); err != nil {
					return daemonCommandError(errorsOut, err)
				}
			}
		case eventErr := <-eventErrors:
			if ctx.Err() != nil {
				return 0
			}
			if eventErr != nil {
				_ = writeWatchLines(errorsOut, time.Now(), "orc", "event stream: "+safeConsoleError(eventErr))
				return 1
			}
		}
	}
}
