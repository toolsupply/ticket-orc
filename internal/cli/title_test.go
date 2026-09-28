package cli

import (
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func TestSetSupervisorTitleChoosesForegroundIdentity(t *testing.T) {
	var titles []string
	previous := setTerminalTitle
	setTerminalTitle = func(title string) { titles = append(titles, title) }
	t.Cleanup(func() { setTerminalTitle = previous })
	setSupervisorTitle(false, []supervisor.RunWorker{{Name: "coder"}})
	setSupervisorTitle(true, []supervisor.RunWorker{{Name: "coder"}})
	setSupervisorTitle(true, []supervisor.RunWorker{{Name: "coder"}, {Name: "reviewer"}})
	if got, want := len(titles), 3; got != want {
		t.Fatalf("title attempts=%d, want %d", got, want)
	}
	if titles[0] != "ticket-orc — supervisor" || titles[1] != "ticket-orc — coder" || titles[2] != "ticket-orc — supervisor" {
		t.Fatalf("titles=%q", titles)
	}
}

func TestSetSupervisorTitleSkipsMachineReadableOutput(t *testing.T) {
	var calls int
	previous := setTerminalTitle
	setTerminalTitle = func(string) { calls++ }
	t.Cleanup(func() { setTerminalTitle = previous })

	setSupervisorTitle(false, []supervisor.RunWorker{{Name: "coder", Config: supervisor.RoleConfig{Output: OutputJSON}}})
	if calls != 0 {
		t.Fatalf("title attempts=%d, want 0 for json output", calls)
	}
}

func TestSupervisorTitleRequiresForegroundInput(t *testing.T) {
	var calls int
	previousTitle := setTerminalTitle
	previousEligible := foregroundTerminalEligible
	setTerminalTitle = func(string) { calls++ }
	foregroundTerminalEligible = func() bool { return false }
	t.Cleanup(func() {
		setTerminalTitle = previousTitle
		foregroundTerminalEligible = previousEligible
	})

	if setSupervisorTitleIfEligible(false, []supervisor.RunWorker{{Name: "coder"}}) {
		t.Fatal("ineligible stdin accepted")
	}
	if calls != 0 {
		t.Fatalf("title attempts=%d, want 0", calls)
	}
}

func TestConsoleSelectionChangesOnlyAfterSuccessfulNewWorkerAction(t *testing.T) {
	command := consoleCommand{name: "start", args: []string{"coder"}}
	if got, changed := consoleSelectionChange("coder", command, true); changed || got != "coder" {
		t.Fatalf("repeated selection got=%q changed=%t", got, changed)
	}
	if got, changed := consoleSelectionChange("", command, false); changed || got != "" {
		t.Fatalf("failed selection got=%q changed=%t", got, changed)
	}
	if got, changed := consoleSelectionChange("coder", consoleCommand{name: "pause", args: []string{"reviewer"}}, true); !changed || got != "reviewer" {
		t.Fatalf("new selection got=%q changed=%t", got, changed)
	}
}

func TestConsoleWorkerTitleSkipsIneligibleForeground(t *testing.T) {
	var calls int
	previousTitle := setTerminalTitle
	previousEligible := foregroundTerminalEligible
	setTerminalTitle = func(string) { calls++ }
	foregroundTerminalEligible = func() bool { return false }
	t.Cleanup(func() {
		setTerminalTitle = previousTitle
		foregroundTerminalEligible = previousEligible
	})

	setConsoleWorkerTitle("coder")
	if calls != 0 {
		t.Fatalf("title attempts=%d, want 0", calls)
	}
}

func TestConsoleFallbackTitleRequiresEligibleAttach(t *testing.T) {
	var titles []string
	previousTitle := setTerminalTitle
	previousEligible := foregroundTerminalEligible
	setTerminalTitle = func(title string) { titles = append(titles, title) }
	foregroundTerminalEligible = func() bool { return true }
	t.Cleanup(func() {
		setTerminalTitle = previousTitle
		foregroundTerminalEligible = previousEligible
	})

	setConsoleStartupTitle("", true)
	if len(titles) != 1 || titles[0] != "ticket-orc — supervisor" {
		t.Fatalf("fallback titles=%q", titles)
	}
	titles = nil
	setConsoleStartupTitle("", false)
	if len(titles) != 0 {
		t.Fatalf("run-i startup titles=%q, want none", titles)
	}
	setConsoleStartupTitle("coder", true)
	if len(titles) != 0 {
		t.Fatalf("selected startup titles=%q, want none", titles)
	}
	foregroundTerminalEligible = func() bool { return false }
	setConsoleStartupTitle("", true)
	if len(titles) != 0 {
		t.Fatalf("ineligible attach titles=%q, want none", titles)
	}
}

func TestConsoleCommandWorkerSelection(t *testing.T) {
	for _, test := range []struct {
		command consoleCommand
		want    string
		ok      bool
	}{
		{command: consoleCommand{name: "start", args: []string{"coder"}}, want: "coder", ok: true},
		{command: consoleCommand{name: "pause", args: []string{"reviewer"}}, want: "reviewer", ok: true},
		{command: consoleCommand{name: "worker", args: []string{"pause", "coder"}}, want: "coder", ok: true},
		{command: consoleCommand{name: "status"}, ok: false},
	} {
		got, ok := consoleCommandWorker(test.command)
		if got != test.want || ok != test.ok {
			t.Fatalf("command=%#v got=%q ok=%t, want %q %t", test.command, got, ok, test.want, test.ok)
		}
	}
}
