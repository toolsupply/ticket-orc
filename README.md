# ticket-orc

[![CI](https://github.com/toolsupply/ticket-orc/actions/workflows/ci.yml/badge.svg)](https://github.com/toolsupply/ticket-orc/actions/workflows/ci.yml)
[![GitHub License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/toolsupply/ticket-orc/LICENSE)
[![Go version](https://img.shields.io/github/go-mod/go-version/toolsupply/ticket-orc)](https://github.com/toolsupply/ticket-orc/blob/main/go.mod)
[![Dependencies](https://img.shields.io/badge/dependencies-stdlib_only-blue)](https://github.com/toolsupply/ticket-orc/blob/main/go.mod)

A small AI agent orchestrator backed by the [ticket](https://github.com/toolsupply/ticket) task manager.

## Key features

- Ticket-based work orchestration for coding agents
- Launches and supervises managed Codex CLI, Claude Code, and Pi workers
- Steers existing interactive Codex CLI sessions
- Supports multiple repositories, workers, roles, and agent harnesses
- Single Linux, macOS, or Windows binary
- No third-party dependencies
- MIT licensed

## Use cases

Using orchestration to notify agents of changes in `ticket` repositories keeps your agents busy when you are not watching.

Let an agent turn your specification or idea into tickets — `ticket-orc` picks up the tickets and hands them out to your
agentic workforce for implementation and review.

### Keep your agent busy

`ticket-orc` is useful even with a single `ticket` repository and a lone Codex CLI session. In this setup, `ticket-orc`
watches the repository for actionable work and notifies the agent when there is something to do.

This lets you assign work and monitor progress through the `ticket` interface without having to repeatedly
return to the agent console.

### Multiple agents with dedicated roles

Separate *coder* and *reviewer* roles can improve code quality by keeping one context focused on implementation and
another focused on reviewing the work being produced.

`ticket-orc` has native support and example configurations for multiple agent roles that can be assigned to
different `ticket` lifecycle queues.

### Scale out across a worker fleet

`ticket-orc` can distribute feature-tagged tickets across a fleet of coding agents working concurrently
on different parts of a project.

This makes it possible to dedicate agents to particular areas of the codebase, repositories, or toolchains and have
them receive only the work relevant to their role.

## Disclaimer

Before installing or running `ticket-orc`, be aware that this software was **written entirely by AI agents** with
the sole purpose of controlling other AI agents. It may contain errors and exhibit unexpected behavior.

Agentic workloads can consume substantial resources when they behave incorrectly or enter unintended loops. Running
`ticket-orc` may result in increased API or token charges, CPU and memory usage, disk usage, filesystem or repository
changes, data loss, or other unintended consequences.

You are responsible for monitoring the agents, services, credentials, usage limits, and environments you connect to
`ticket-orc`. Do not run it unattended against systems or data you cannot afford to modify or lose.

This software is provided without warranty. See the MIT License for the applicable warranty and liability terms.

## Installation

### Prerequisite

Install the latest [ticket](https://github.com/toolsupply/ticket) first and make sure `ticket` is on `PATH`:

```sh
ticket --version
```

### Linux

Install the latest Linux amd64 release into `~/.local/bin`:

```sh
mkdir -p "$HOME/.local/bin"
curl -fsSL "https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-linux-amd64.tar.gz" | tar -xz -C "$HOME/.local/bin" ticket-orc
```

### Manual installation

Release archives are available for the supported platforms:

| Platform | Archive |
| --- | --- |
| Linux x86-64 | [ticket-orc-linux-amd64.tar.gz](https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-linux-amd64.tar.gz) |
| Linux ARM64 | [ticket-orc-linux-arm64.tar.gz](https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-linux-arm64.tar.gz) |
| macOS Intel | [ticket-orc-darwin-amd64.tar.gz](https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-darwin-amd64.tar.gz) |
| macOS Apple Silicon | [ticket-orc-darwin-arm64.tar.gz](https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-darwin-arm64.tar.gz) |
| Windows x86-64 | [ticket-orc-windows-amd64.zip](https://github.com/toolsupply/ticket-orc/releases/latest/download/ticket-orc-windows-amd64.zip) |

Download `SHA256SUMS` with the archive and verify the checksum before extracting it. Place `ticket-orc` or `ticket-orc.exe` somewhere on `PATH`.

Release archives also have GitHub artifact attestations:

```sh
gh attestation verify ticket-orc-linux-amd64.tar.gz -R toolsupply/ticket-orc
```

### Agent Skills

For effective work management, install both the `ticket-tasks` and `ticket-orc-worker` Skills.

In Codex:

```text
$skill-installer install https://github.com/toolsupply/ticket/tree/main/skills/ticket-tasks
$skill-installer install https://github.com/toolsupply/ticket-orc/tree/main/skills/ticket-orc-worker
```

`ticket-tasks` teaches the Ticket workflow. `ticket-orc-worker` adds the conventions used by orc-managed workers.

The `ticket-orc-worker` Skill is also published as `ticket-orc-worker.zip` on the [releases page](https://github.com/toolsupply/ticket-orc/releases).

## Quick start

This example uses two existing interactive Codex CLI sessions as a coder/reviewer pair.

Start from a project that already has an initialized `ticket` repository.

Create a small default orc instance with config in ~/.ticket-orc/config.json:

```sh
ticket-orc init --global
```

Start Orc with its interactive operator console:

```sh
ticket-orc run -i
```

In two other terminals, start Codex with different Ticket actors:

```sh
TICKET_ACTOR=coder codex --no-daemon
```

```sh
TICKET_ACTOR=reviewer codex --no-daemon
```

In each Codex session, join the current thread to Orc:

```sh
!ticket-orc join
```

The configuration created by `ticket-orc init` makes the reviewer close accepted work, allowing dependent tickets to become actionable automatically. Change:

```json
"review_completion": "close"
```

to:

```json
"review_completion": "signoff"
```

if you prefer accepted tickets to wait for manual final closure.

When a reviewer finds a problem, the ticket is returned to implementation with review findings and can be picked up by the coder again.

## How it works

`ticket-orc` monitors `ticket` repositories and decides when an agent should be started or notified.

There are two main ways to use it.

### Interactive session steering

A running Codex CLI session can report in for duty and sign up for work:

```sh
!ticket-orc join
```

When work becomes actionable, `ticket-orc` sends a nudge to the existing Codex thread. The agent then uses `ticket` to select, claim, work, submit, review, or close tickets.

You can watch the progress, co-steer and control the harness while the session is joined.

Useful commands for the current interactive session include:

```sh
!ticket-orc whoami
!ticket-orc state
!ticket-orc next
!ticket-orc leave
```

### Managed sessions

Managed workers are harness processes launched and supervised by `ticket-orc`.

Workers can be grouped and started together. `ticket-orc` currently provides managed harness adapters for Codex CLI, Claude Code, and Pi.

A small managed coder/reviewer configuration may look like this:

```json
{
  "version": 1,
  "id": "7e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
  "default_role": "coder",
  "roles": {
    "coder": {
      "ticket_queue": "open",
      "nudge_prompt": "Coding work is available. Process actionable Ticket work for your current actor until none remains."
    },
    "reviewer": {
      "ticket_queue": "review",
      "nudge_prompt": "Review work is available. Perform substantive review of actionable Ticket work for your current actor until none remains.",
      "review_completion": "close"
    }
  },
  "defaults": {
    "harness": "codex",
    "session_policy": "ticket",
    "session_cleanup": "delete"
  },
  "workers": {
    "coder": {
      "role": "coder",
      "actor": "coder",
      "groups": ["default"]
    },
    "reviewer": {
      "role": "reviewer",
      "actor": "reviewer",
      "groups": ["default"]
    }
  },
  "supervisor": {
    "startup_groups": ["default"]
  }
}
```

Start the configured startup groups:

```sh
ticket-orc run -c managed-fleet.json
```

Start every configured worker and open the interactive operator console:

```sh
ticket-orc run -c managed-fleet.json --all -i
```

### Daemon control

Useful commands:

```sh
ticket-orc status
ticket-orc queue
ticket-orc doctor
ticket-orc pause
ticket-orc resume
```

In interactive sessions, use `watch` to monitor repository and worker dispatch events.

In case of misaligned agent behavior, use `ticket-orc abort` as an emergency stop. It
immediately pauses further work dispatch, terminates active managed workers, and sends
a stop request to active steered sessions.

## Building

Toolchain: Go `1.26.8`. No CGO or third-party Go dependencies are required.

```sh
make && ./bin/ticket-orc version
```

## Tests

```sh
go test ./... && go vet ./... && go test -race ./...
```

## License

MIT
