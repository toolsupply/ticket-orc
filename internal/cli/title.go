package cli

import (
	"os"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/terminaltitle"
)

var (
	setTerminalTitle = terminaltitle.Set
	// foregroundTerminalEligible is kept injectable so title policy can be
	// tested without requiring a PTY. terminaltitle.Set performs the stdout
	// and TERM checks; this seam covers the corresponding stdin requirement.
	foregroundTerminalEligible = func() bool {
		info, err := os.Stdin.Stat()
		return err == nil && info != nil && info.Mode()&os.ModeCharDevice != 0
	}
)

func setSupervisorTitleIfEligible(interactive bool, workers []supervisor.RunWorker) bool {
	if !foregroundTerminalEligible() {
		return false
	}
	setSupervisorTitle(interactive, workers)
	return true
}

func setSupervisorTitle(interactive bool, workers []supervisor.RunWorker) {
	for _, worker := range workers {
		// JSON is a machine-readable stream. An OSC sequence written to the
		// process terminal would corrupt that stream when stdout is a terminal.
		if worker.Config.Output == OutputJSON {
			return
		}
	}
	if interactive && len(workers) == 1 {
		if workers[0].Name != "" {
			setTerminalTitle("ticket-orc — " + workers[0].Name)
		}
		return
	}
	setTerminalTitle("ticket-orc — supervisor")
}
