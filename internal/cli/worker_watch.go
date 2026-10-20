package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

const workerWatchPollInterval = executionlog.PollInterval

type workerLogWatch struct {
	follower *executionlog.Follower
	renderer consoleWatchRenderer
	workers  map[string]daemon.WorkerStatus
}

func (watch *workerLogWatch) refresh(status daemon.Status) {
	watch.renderer.seedRepositories(status.Repositories)
	for _, worker := range status.Workers {
		if previous, ok := watch.workers[worker.Name]; ok && previous.RepositoryID == worker.RepositoryID {
			watch.workers[worker.Name] = worker
		}
	}
}

func newWorkerLogWatch(localDir string, status daemon.Status, options watchOptions, out io.Writer) (*workerLogWatch, error) {
	watch := &workerLogWatch{workers: make(map[string]daemon.WorkerStatus)}
	watch.renderer.seedRepositories(status.Repositories)
	var catalog []executionlog.Worker
	for _, worker := range status.Workers {
		watch.workers[worker.Name] = worker
		catalog = append(catalog, executionlog.Worker{Name: worker.Name, Role: worker.Role, Harness: worker.Harness, RepositoryID: worker.RepositoryID})
	}
	var selected []string
	if options.selector != "all" {
		selected = []string{options.selector}
	}
	if out == nil {
		out = io.Discard
	}
	follower, err := executionlog.New(localDir, catalog, selected, func(observation executionlog.Observation) error {
		worker := watch.workers[observation.Worker]
		return renderWorkerObservation(out, options, observation, watch.renderer.workerLabel(worker))
	})
	if err != nil {
		return nil, err
	}
	watch.follower = follower
	return watch, nil
}

func (watch *workerLogWatch) Close()                                  { watch.follower.Close() }
func (watch *workerLogWatch) poll(now time.Time) error                { return watch.follower.Poll(now) }
func runWorkerWatch(ctx context.Context, watch *workerLogWatch) error { return watch.follower.Run(ctx) }

func renderWorkerObservation(out io.Writer, options watchOptions, observation executionlog.Observation, label string) error {
	var rendered bytes.Buffer
	record := observation.Record
	if record.Code == "invalid_event" || record.Code == "event_too_large" {
		provider := map[string]string{"codex": "Codex", "claude": "Claude", "pi": "Pi"}[observation.Harness]
		fmt.Fprintf(&rendered, "[%s log event skipped: %s]\n", provider, terminaltext.Sanitize(record.Text, false))
	} else {
		var err error
		switch observation.Harness {
		case "codex":
			err = codex.RenderRecord(options.outputMode(), record, &rendered)
		case "claude":
			err = claude.RenderRecord(options.outputMode(), record, &rendered)
		case "pi":
			err = pi.RenderRecord(options.outputMode(), record, &rendered)
		default:
			return fmt.Errorf("unsupported worker log harness %q", observation.Harness)
		}
		if err != nil {
			return err
		}
	}
	return writeWatchLines(out, observation.ObservedAt, label, rendered.String())
}
