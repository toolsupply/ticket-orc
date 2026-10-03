package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/columnrender"
	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

const topLevelHelp = `
A small agent orchestrator from https://github.com/toolsupply/ticket-orc

Usage:
  ticket-orc <command> [options]

Getting started:
  init	Create a minimal Orc instance
  join	Register the current Ticket actor and Codex session with a role
  leave	Remove the current Ticket actor registration
  whoami	Show current Ticket and Orc session identity
  state	Show current session work and delivery status
  next	Inspect current session's active or ready Ticket work

Worker management:
  coder	Run one coder role
  reviewer	Run one reviewer role
  run	Run selected workers as a foreground supervisor
  doctor	Inspect configuration and local runtime health
  status	Show daemon status
  worker	Control one daemon worker
  group	Control a daemon worker group

Queue control:
  queue	Forecast work for configured and registered owners

Daemon control:
  endpoint	Print the local daemon capability URL
  attach	Attach an interactive console to a running daemon
  pause	Pause daemon-wide work dispatch
  resume	Resume daemon-wide work dispatch
  abort	Abort daemon-wide work dispatch
  reload	Reload daemon configuration
  shutdown	Stop the daemon

Utilities:
  gc	Remove retained state for terminal tickets
  report review	Report aggregate Ticket review-flow outcomes
  config	Validate configuration
  help	Show help for a command
  version	Show the ticket-orc version

Run 'ticket-orc help <command>' for command-specific help.
Run 'ticket-orc help options' for global options.

Normal commands select an instance from TICKET_ORC, then an existing
./.ticket-orc, then an existing ~/.ticket-orc. Relative TICKET_ORC paths use
the current directory. A selected instance error does not fall back. Explicit
config files take precedence; TICKET_ORC_ENDPOINT overrides daemon transport
without changing local registration or state identity.

`

const endpointHelp = `
Usage:
  ticket-orc endpoint [-c|--config FILE]

Print the complete URL for the local daemon, including its endpoint key. The
key authorizes access to Orc's control API. Treat this URL as sensitive and
share it only with trusted clients. Routine status output omits the key.

The API uses unencrypted HTTP. Protect the URL and its network path if you
provide it to another machine or through a tunnel.

`

const helpHelp = `
Usage:
  ticket-orc help [COMMAND]

Show the main help page or detailed help for COMMAND. Use
"ticket-orc help config check" for configuration validation help.

`

const versionHelp = `
Usage:
  ticket-orc version

Print the ticket-orc version and, when available, its build commit.

`

const optionsHelp = `
Usage:
  ticket-orc help options

Global options:
  -h, --help       Show help for ticket-orc or a command
  -v, --version    Show the ticket-orc version

`

const initHelp = `
Usage:
  ticket-orc init [--global] [--force]

Create a minimal instance in .ticket-orc under the current directory. TICKET_ORC
selects another directory for this command; relative paths are based on the
current directory. Use --global to create the current user's ~/.ticket-orc,
even when TICKET_ORC is set. Existing config files are preserved unless
--force is supplied. Force reset refuses a running instance, removes its
owned runtime state, and creates a fresh config ID; an external local_dir is
removed only when its ownership marker matches the existing config. The
generated .gitignore keeps mutable instance data untracked while allowing
config.json to be tracked. The generated reviewer role closes accepted
reviews; hand-written configurations keep the signoff default unless they set
review_completion explicitly.

`

const joinHelp = `
Usage:
  ticket-orc join [role] [--harness NAME --session ID --transport spool] [--json] [-c|--config FILE]

Register the current Ticket actor and interactive session with this Orc instance.
Without endpoint flags, the current Codex session is discovered automatically.
Supply all endpoint flags together to register an extension-backed session.
ROLE may be omitted. The role is selected from, in order:
  1. explicit ROLE
  2. TICKET_ORC_ROLE
  3. the current Ticket actor, when it matches a configured Orc role
  4. default_role from the Orc configuration

`

const leaveHelp = `
Usage:
  ticket-orc leave [--harness NAME --session ID --transport spool --registration-id ID --incarnation-id ID] [--json] [-c|--config FILE]

Remove the registration for the current Ticket repository, actor, and session.
Explicit endpoint mode requires the exact registration and incarnation IDs
returned by join. A stale or replaced registration is harmless.

`

const whoamiHelp = `
Usage:
  ticket-orc whoami [-j] [--harness NAME --session ID --transport spool] [-c|--config FILE]

Show the current Orc instance, Ticket repository and actor, session, and
whether this session is registered. Without endpoint flags, Codex is discovered
automatically. This command does not use HTTP.

`

const nextHelp = `
Usage:
  ticket-orc next [-j] [--harness NAME --session ID --transport spool] [-c|--config FILE]

Show the current joined session's active Ticket claim, or inspect Ticket's
ready frontier without claiming or notifying a worker.

`

const queueHelp = `
Usage:
  ticket-orc queue [-j] [--endpoint URL] [-c|--config FILE]

Show the current Ticket frontier and the configured or registered owners that
would receive work. This is a read-only forecast and remains available when
daemon dispatch is paused or aborted; output identifies the dispatch mode.
JSON output includes daemon_mode, dispatch_inhibited, and owners.

`

func writeEndpointHelp(w io.Writer) { fmt.Fprint(w, endpointHelp) }

const roleHelp = `
Usage:
  ticket-orc coder [options] or ticket-orc reviewer [options]

Options:
  --worker NAME             Select a named configured worker
  --harness NAME            Harness implementation (default: codex)
  --actor NAME              Ticket actor identity (default: role actor or role name)
  --model MODEL             Optional harness model
  --reasoning LEVEL         low|medium|high|xhigh|max|ultra
  --max-bounces N           Review-return limit (default: 6)
  --session-policy POLICY   ticket|fresh (default: ticket)
  --session-cleanup POLICY  delete|archive|keep (Codex default: delete; Pi/Claude: keep)
  --minimum-reuse-context-percent N  0-100; default: 20
  --review-completion MODE  signoff|close (reviewer default: signoff)
  --codex-sandbox MODE      Codex-specific sandbox setting
  --pi-provider NAME        Pi-specific provider setting
  --claude-permission-mode MODE  Claude-specific permission setting
  --output MODE             compact|quiet|json (default: compact)
  --ticket-prompt TEXT      Exact-ticket prompt template
  -c, --config FILE          Configuration file (default: TICKET_ORC/config.json, then ./.ticket-orc/config.json, then ~/.ticket-orc/config.json)
  -h, --help                Show this help

Environment:
  TICKET_ORC_WORKER
  TICKET_ORC_HARNESS
  TICKET_ORC_ACTOR
  TICKET_ORC_MODEL
  TICKET_ORC_REASONING
  TICKET_ORC_MAX_BOUNCES
  TICKET_ORC_SESSION_POLICY
  TICKET_ORC_SESSION_CLEANUP
  TICKET_ORC_MINIMUM_REUSE_CONTEXT_PERCENT
  TICKET_ORC_REVIEW_COMPLETION
  TICKET_ORC_CODEX_SANDBOX
  TICKET_ORC_PI_PROVIDER
  TICKET_ORC_CLAUDE_PERMISSION_MODE
  TICKET_ORC_OUTPUT
  TICKET_ORC_TICKET_PROMPT

Command options override environment variables, which override defaults.

Configuration may set review.skip_tags to a list of canonical Ticket tags.
Matching tags bypass Orc's automatic reviewer route; explicit Ticket actions
remain available and the normal route is used when no tag matches.

`

const stateHelp = `
Usage:
  ticket-orc state [options]

Show the current session's ready work, active claim, and notification status.

Options:
  --output MODE             compact|quiet|json (json emits structured state)
  --harness NAME            Select a registered extension-backed session
  --session ID              Pair with --harness and --transport
  --transport spool         Select the generic filesystem spool endpoint
  -c, --config FILE          Configuration file (default: TICKET_ORC/config.json, then ./.ticket-orc/config.json, then ~/.ticket-orc/config.json)
  -h, --help                Show this help

`

const gcHelp = `
Usage:
  ticket-orc gc [options]

Remove retained orchestration state for tickets confirmed as closed or rejected.

Options:
  --worker NAME             Configured worker that owns the Ticket target
  --actor NAME              Optional actor assertion for the selected worker
  --session-cleanup POLICY  delete|archive|keep (default: delete)
  -c, --config FILE          Configuration file (default: TICKET_ORC/config.json, then ./.ticket-orc/config.json, then ~/.ticket-orc/config.json)
  -h, --help                Show this help

Environment:
  TICKET_ORC_ACTOR
  TICKET_ORC_SESSION_CLEANUP

Command options override environment variables, which override defaults.

`

const runCommandHelp = `
Usage:
  ticket-orc run [options]

Run the selected named workers as a foreground supervisor. A bare run may
remain idle when no startup groups are configured; it never means --all.

Options:
  --worker NAME             Select a worker (repeatable)
  --group NAME              Select a worker group (repeatable)
  -a, --all                 Select every configured worker
  -i, --interactive         Run the foreground supervisor with an operator console
  --listen ADDRESS          IP literal to bind (default: %s)
  --port PORT               Listen port (default: %d, OS-selected; set a fixed port explicitly)
  -c, --config FILE          Configuration file (default: TICKET_ORC/config.json, then ./.ticket-orc/config.json, then ~/.ticket-orc/config.json)
  -h, --help                Show this help

Environment:
  TICKET_ORC_LISTEN
  TICKET_ORC_PORT
  TICKET_ORC_ENDPOINT

The configuration file may set supervisor.listen and supervisor.port. Bind
addresses must be IP literals; hostnames are rejected. Ports 1-1023 are
rejected. Command-line values override environment variables, which override
the corresponding configuration fields. The client endpoint is kept separate
from the bind address. The default listener uses an OS-selected port; set
--port explicitly to use a fixed port. ticket-orc endpoint prints the complete
local client URL,
including the authorization capability. Routine status output omits the key.
TICKET_ORC_ENDPOINT overrides the client URL and does not change the listener.
The control API uses unencrypted HTTP; protect traffic if you route it beyond
the local host.

The top-level local_dir config field optionally selects the exact root for
Orc-generated runtime data. Relative paths are resolved from the configuration
file. If omitted, Orc uses .local beside the configuration file.

Worker groups select Orc workers; they do not filter Ticket tickets or tags.

`

const attachHelp = `
Usage:
  ticket-orc attach [options]

Attach an interactive operator console to an already-running foreground daemon.
EOF or 'exit', 'quit', or 'q' leaves an attached daemon running. Use 'shutdown confirm' to stop it.

Options:
  -h, --help        Show this help

`

const doctorHelp = `
Usage:
  ticket-orc doctor [--config FILE]
  ticket-orc doctor --reset-local [--config FILE]

Validate the selected configuration and inspect its local runtime without
starting workers or changing runtime state. Use --reset-local to discard local
runtime state while preserving config.json and its instance ID. Any currently
joined steered sessions must run ticket-orc join again after a reset; managed
workers remain configured.

Options:
  --config FILE     Worker configuration file
  --reset-local     Remove local runtime state and preserve configuration
  -h, --help        Show this help

`

const configCheckHelp = `
Usage:
  ticket-orc config check [options]

Validate the complete configuration without claiming tickets, launching a
harness, contacting Ticket, or changing orchestration state.

Role configuration may set roles.<name>.ticket_tags; every listed tag is
required. review.skip_tags adds exclusions for review roles. A committed reload
updates dynamic steering immediately, while running managed workers keep their
effective selector until restarted through the normal worker lifecycle.

Options:
  --output MODE             compact|quiet|json (default: compact)
  -c, --config FILE          Configuration file (default: TICKET_ORC/config.json, then ./.ticket-orc/config.json, then ~/.ticket-orc/config.json)
  -h, --help                Show this help

`

const reviewReportHelp = `
Usage:
  ticket-orc report review [options]

Report bounded review-flow evidence from Ticket's public Work log projection.
The report counts only exact, explicitly marked entries; it never infers a
lifecycle transition from arbitrary prose.

Options:
  --since DURATION       Window such as 30d or 720h (default: 30d; maximum: 365d)
  --actor NAME           Ticket actor used for the read-only JSON session
  --repository PATH      Explicit Ticket repository target
  --limit N              Maximum tickets to inspect (default: 512)
  --output MODE          compact|json (default: compact)
  -h, --help             Show this help

The report is bounded by Ticket's public list JSON response and each selected
Work log. To contribute evidence, a Work log message must be exactly one of:
"ticket-orc review: submitted", "ticket-orc review: approved", or
"ticket-orc review: returned" (with Ticket's normal timestamp and actor prefix).
Missing, truncated, malformed, or out-of-order markers are reported as
unknown or incomplete. The report does not score actors, infer remote-session
liveness, or judge code quality.

`

const consoleHelp = `
ticket-orc interactive console

Worker management:
  status  	Show managed workers, external sessions, and repository health
  workers 	List workers and show their status
  start   	Start a worker
  stop    	Stop a worker
  restart 	Restart a worker
  pause   	Pause a worker
  resume  	Resume a worker
  group   	Control worker groups
  doctor  	Diagnose and recover workers

Queue control:
  queue   	Show the current scheduling forecast
  watch   	Show live Orc activity until Ctrl-C

Daemon control:
  daemon  	Pause, resume, or abort daemon-wide work dispatch
  reload  	Reload worker configuration
  shutdown	Stop the daemon
  quit    	End this interactive session; leave the daemon running in attach mode; stop the foreground supervisor in run --interactive mode
  help    	Show this help page

`

func writeHelp(w io.Writer) { writeColumnHelp(w, topLevelHelp) }

func writeHelpTopic(topic []string, out io.Writer) bool {
	switch len(topic) {
	case 0:
		writeHelp(out)
	case 1:
		switch topic[0] {
		case "help":
			fmt.Fprint(out, helpHelp)
		case "options":
			fmt.Fprint(out, optionsHelp)
		case "init":
			fmt.Fprint(out, initHelp)
		case "join":
			fmt.Fprint(out, joinHelp)
		case "leave":
			fmt.Fprint(out, leaveHelp)
		case "whoami":
			fmt.Fprint(out, whoamiHelp)
		case "next":
			fmt.Fprint(out, nextHelp)
		case "queue":
			fmt.Fprint(out, queueHelp)
		case string(RoleCoder), string(RoleReviewer):
			writeRoleHelp(out, supervisor.Role(topic[0]))
		case "state":
			writeStateHelp(out)
		case "gc":
			writeGCHelp(out)
		case "run":
			writeRunHelp(out)
		case "doctor":
			writeDoctorHelp(out)
		case "attach":
			writeAttachHelp(out)
		case "endpoint":
			writeEndpointHelp(out)
		case "version":
			fmt.Fprint(out, versionHelp)
		case "status", "worker", "group", "pause", "resume", "abort", "reload", "shutdown":
			writeDaemonHelp(out, topic[0])
		default:
			return false
		}
	case 2:
		switch {
		case topic[0] == "config" && topic[1] == "check":
			writeConfigCheckHelp(out)
		case topic[0] == "report" && topic[1] == "review":
			writeReviewReportHelp(out)
		default:
			return false
		}
	default:
		return false
	}
	return true
}

func writeRoleHelp(w io.Writer, role supervisor.Role) {
	help := strings.Replace(roleHelp, "ticket-orc coder", "ticket-orc "+string(role), 1)
	fmt.Fprint(w, help)
}

func writeStateHelp(w io.Writer) { fmt.Fprint(w, stateHelp) }

func writeGCHelp(w io.Writer) { fmt.Fprint(w, gcHelp) }
func writeRunHelp(w io.Writer) {
	fmt.Fprintf(w, runCommandHelp, daemon.DefaultListenAddress, daemon.DefaultListenPort)
}
func writeAttachHelp(w io.Writer)       { fmt.Fprint(w, attachHelp) }
func writeDoctorHelp(w io.Writer)       { fmt.Fprint(w, doctorHelp) }
func writeReviewReportHelp(w io.Writer) { fmt.Fprint(w, reviewReportHelp) }
func writeConfigCheckHelp(w io.Writer)  { fmt.Fprint(w, configCheckHelp) }
func writeConsoleHelp(w io.Writer)      { writeColumnHelp(w, consoleHelp) }

func writeColumnHelp(w io.Writer, help string) {
	var rows [][]string
	flush := func() {
		if len(rows) == 0 {
			return
		}
		columnrender.Render(w, rows, nil, "  ")
		rows = nil
	}
	lines := strings.Split(help, "\n")
	for i, line := range lines {
		if i == len(lines)-1 && line == "" {
			continue
		}
		if strings.Contains(line, "\t") {
			rows = append(rows, strings.Split(line, "\t"))
			continue
		}
		flush()
		_, _ = fmt.Fprintln(w, line)
	}
	flush()
}

func writeConsoleTopicHelp(w io.Writer, topic string) {
	switch topic {
	case "queue":
		fmt.Fprint(w, "\nInteractive console help: queue\n\n  queue  Show the current scheduling forecast.\n")
	case "worker":
		fmt.Fprint(w, "\nInteractive console help: worker\n\n  worker status [NAME|verbose|detail]\n  worker start|stop|restart|pause|resume NAME\n  NAME may be all for recovery operations.\n")
	case "group":
		fmt.Fprint(w, "\nInteractive console help: group\n\n  group start|stop NAME\n  group start all starts every configured worker.\n")
	case "daemon":
		fmt.Fprint(w, "\nInteractive console help: daemon\n\n  daemon pause|resume|abort  Control daemon-wide work dispatch.\n  Daemon pause is distinct from worker pause; worker pause and pause all retain their worker-level meaning.\n  Abort stops managed workers and sends best-effort requests to relevant steer sessions; remote termination is not guaranteed.\n")
	case "status":
		fmt.Fprint(w, "\nInteractive console help: status\n\n  status [NAME|verbose|detail]  Show fleet or one worker status.\n  Use workers for worker-focused help and listing.\n")
	case "workers":
		fmt.Fprint(w, "\nInteractive console help: workers\n\n  workers [NAME|verbose|detail]  List workers or show one worker's status.\n  Use verbose or detail for expanded fleet status.\n  Use workers -h or workers --help to show this help.\n")
	case "start", "stop", "restart", "pause", "resume":
		fmt.Fprintf(w, "\nInteractive console help: %s\n\n  %s NAME  Operate on one worker; NAME may be all.\n", topic, topic)
	case "watch":
		fmt.Fprint(w, "\nInteractive console help: watch\n\n  watch  Show live Orc activity until Ctrl-C returns to the console.\n")
	case "doctor":
		fmt.Fprint(w, "\nInteractive console help: doctor\n\n  doctor  Reload configuration and start every safely recoverable configured worker. Reports workers that still need manual action.\n")
	case "reload":
		fmt.Fprint(w, "\nInteractive console help: reload\n\n  reload  Apply the current configuration to the running daemon. Use doctor to start workers that need recovery.\n")
	case "shutdown":
		fmt.Fprint(w, "\nInteractive console help: shutdown\n\n  shutdown confirm  Stop the running daemon and its managed workers.\n")
	case "exit", "quit", "q":
		fmt.Fprint(w, "\nInteractive console help: quit\n\n  quit  End this interactive session. In attach mode the daemon keeps running; in run --interactive mode the foreground supervisor stops.\n")
	default:
		fmt.Fprintf(w, "\nInteractive console help: %s\n\n  No command-specific help is available.\n", topic)
	}
	fmt.Fprintln(w)
}
