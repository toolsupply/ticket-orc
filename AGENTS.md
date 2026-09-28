# AGENTS.md

## Project intent

`ticket-orc` orchestrates Ticket workers while keeping Ticket itself authoritative
for work ownership and lifecycle state.

Keep Orc small and explicit:

* one static Go binary;
* Go standard library only;
* configured managed workers and dynamically registered external steer sessions
  may run in the same Orc instance;
* Ticket remains the source of truth for ticket state;
* Orc state records orchestration state only;
* worker-facing protocol should be smaller than Orc's internal model;
* prefer deletion and explicit state machines over compatibility layers,
  retries, heuristics, or speculative abstractions.

Do not turn Orc into a second scheduler, workflow database, agent framework, or
general-purpose RPC platform.

## Architecture contracts

Preserve these invariants:

* `ticketclient` is the boundary to Ticket. Do not reconstruct Ticket lifecycle,
  selection, or claim semantics elsewhere.
* `state` owns durable Orc orchestration state and its repository namespacing.
* `orc` owns worker workflow decisions.
* `supervisor` owns process/worker lifecycle and runtime coordination.
* A dynamic steer manager owns external registrations and notification
  scheduling, separate from configured managed worker lifecycle.
* `daemon` owns the local control/API transport and translates domain results at
  the boundary; domain packages must not depend on daemon DTOs.
* `harness` and provider adapters own harness execution/queue mechanics.
* Harness, model, and provider names are mutable execution settings, not Orc
  identities. Stable worker names and durable identity/state/logging keys must
  not depend on them; model changes must not alter those keys. Dynamic steer
  display identity is the repository, role, Ticket actor, and session.
* `cli` is composition, configuration, commands, and presentation; do not move
  domain policy into command handlers.
* Repository-scoped reads and mutations must fail closed when a new persisted
  collection has not been given an explicit namespace policy.
* Once Ticket authoritatively reports a terminal ticket, terminal cleanup is
  ticket-wide in that repository, not worker- or owner-scoped.
* Persisted state is replaced atomically under the existing locking discipline.
  Preserve `internal/supervisor/LOCKING.md` and state-lock ordering.

These are change contracts, not claims that every current code path already
complies. When existing code conflicts with a contract, treat it as a defect or
tracked cleanup item; do not weaken the contract to match the implementation.

Do not introduce a generic `common`, `shared`, or `types` package merely to
avoid choosing which package owns a concept.

## Worker ownership

### Managed workers

Managed workers are configured processes. Orc owns their sessions and
exact-ticket assignments.

Keep managed workers deliberately oblivious to Orc internals:

* establish Ticket actor/routing in the child execution environment;
* send small, exact-ticket task instructions;
* do not narrate Orc role, repository routing, or actor identity in prompts when
  the environment already establishes them;
* keep reviewer completion policy in the concrete review instruction when it is
  genuinely task-specific;
* managed workers should need the Ticket Skill, not a persistent Orc protocol
  Skill merely because Orc launched them.

Managed session reuse, replacement, and cleanup are Orc lifecycle concerns, not
Ticket lifecycle state.

### Dynamic steer sessions

Steer sessions register dynamically with local `ticket-orc join [role]`. The
external harness owns its Ticket identity, routing, and session lifecycle. The
selected Orc role is scheduling policy, not Ticket identity. Local registration
is local state, not an HTTP bootstrap operation. HTTP is the authenticated
service and federation plane.

Orc must not install or reinforce that identity through messages:

* never instruct the worker to set `TICKET_ACTOR`, `TICKET_REPOSITORY`,
  `TICKET_SCOPE`, or `TICKET_CONFIG`;
* if a worker needs its effective identity, Ticket is authoritative
  (`ticket actor -j`);
* steer messages should contain only the minimum operational notification or
  control information needed.

Address a Codex steer session with its recorded `CODEX_HOME` and thread ID.
Harness working directory is not part of steer routing. Orc may set Ticket
actor and repository in its own Ticket subprocess environment to observe that
registration's queue; it must not set them in the external Codex session.

Passive steer is intentionally conservative:

* an accepted wake is not proof of consumption;
* do not enqueue another wake merely because time passed or the actionable
  frontier changed;
* an active claim is positive Ticket evidence that a queued wake was consumed;
* once that claim ends, if Ticket reports no active claim and more actionable
  work, retire the consumed wake and enqueue one fresh wake;
* uncertain queue outcomes remain outstanding until explicit reconciliation;
* do not add retry/renudge behavior to compensate for missing delivery
  acknowledgement.

If an active steer protocol is ever added, require an explicit command/ACK
contract rather than inferring acknowledgement from timeouts.

## Worker-facing protocol

Keep internal configuration abstractions inside Orc.

The operator may choose a role with `join [role]`. An external worker should
not need to know about:

* state generations;
* initialization/reorientation epochs;
* delivery frontiers;
* repository namespace keys;
* session-retention bookkeeping;
* supervisor implementation details.

The Ticket Skill defines Ticket command mechanics and lifecycle safety.
Any Orc steer Skill should contain only the small additional behavior required
to turn an already-correct Ticket agent into a passive Orc-steered worker.

Do not duplicate Ticket command semantics across README, Orc prompts, Orc Skill,
and source code.

## State and compatibility

The first public state model should be simple and explicit.

* Every persisted ticket-bound record must have unambiguous repository identity.
* Dynamic steer registration and daemon delivery state are separate from
  managed/general state. Registration ownership is keyed by Ticket repository
  ID and actor; a changed registration generation invalidates old delivery.
* Reject malformed or unsupported state with a precise error; do not silently
  reinterpret it.
* Do not preserve compatibility with obsolete **unreleased** Orc protocols,
  state shapes, aliases, or broken steer behavior unless a ticket explicitly
  requires that compatibility.
* Once an interface or state format has shipped publicly, compatibility changes
  must be deliberate: migrate, version, or document the break.
* Old fields that no longer drive supported behavior should be deleted rather
  than retained indefinitely "just in case".
* Keep historical state bounded. Diagnostics are not an excuse for an
  unbounded event log.

State should record facts Orc needs to resume or diagnose behavior, not a second
copy of Ticket.

## Recovery and failure handling

Recovery must be based on explicit evidence.

Prefer:

* typed errors and structured failure envelopes;
* idempotent operations;
* durable state transitions;
* positive acknowledgement or authoritative Ticket state;
* clear operator-visible failure when certainty is unavailable.

Avoid:

* parsing error-message strings for control flow;
* timeout-based assumptions that a queued command vanished;
* automatic retries after an operation may already have been accepted;
* reconstructing identity or routing from conversation text;
* "repair" code that mutates Ticket merely to make Orc state look consistent.

When an operation is uncertain, preserve uncertainty and stop or require
reconciliation rather than manufacturing certainty.

## Concurrency and lifecycle

Concurrency behavior must be explicit and testable.

* Follow the documented supervisor lock ordering.
* Do not hold locks across harness execution, network/control waits, or Ticket
  subprocess work unless the existing contract explicitly requires it.
* Keep worker/session ownership transitions atomic from Orc's perspective.
* A terminal Ticket invalidates all Orc state for that Ticket in the repository.
* A superseded managed session must never become current again after restart.
* Cleanup failure must not silently resurrect logically-dead state.

For concurrency-sensitive changes, add a focused regression and run the race
detector for the affected packages.

## Lean design discipline

Before adding a type, interface, package, state field, callback, retry, or
compatibility branch, answer:

1. Which concrete supported behavior requires it?
2. Which package owns that behavior?
3. Can an existing primitive express it?
4. What state or code can be deleted if this is added?

Prefer:

* concrete types until multiple real implementations require an interface;
* functions and small structs over framework-style service layers;
* one canonical path for each lifecycle transition;
* direct Make/Go/GitHub Actions commands over project-specific shell wrappers
  when the commands are short and readable;
* explicit fields and switches over reflection in production paths when the
  domain is finite;
* tests that enforce architectural invariants over runtime self-inspection.

Avoid speculative flexibility. Do not add extension points for hypothetical
providers, transports, modes, or recovery strategies.

A cleanup that only moves code, renames abstractions, or introduces another
layer without deleting complexity is probably not a cleanup.

## Configuration

Keep configuration boring and deterministic.

* One setting should have one canonical meaning.
* Do not retain prerelease aliases without a concrete compatibility obligation.
* Environment variables prefixed `TICKET_ORC_` configure Orc itself.
  `TICKET_ORC` selects an Orc instance directory; it is not a Ticket target.
* Ticket execution variables configure Ticket workers and must respect the
  managed/steer ownership boundaries above.
* Validation should fail early with actionable errors.
* Do not duplicate defaults independently across config parsing, generated
  config, help text, prompts, and tests.

## Ticket and repository boundaries

* Ticket is authoritative for lifecycle, queue selection, and ownership. Use
  its public CLI JSON interface with explicit full IDs; do not rebuild Ticket
  scheduling from observational list output.
* Use the repository-local Ticket CLI for managed ticket state. Do not use
  filesystem tools to create, edit, delete, rename, or enumerate managed ticket
  metadata or `TASK.md` files.
* Treat `../ticket` as a read-only reference for Ticket's public protocol and
  `../ticket-ui` as a read-only consumer reference for control API integration.
  Do not edit sibling repositories, import their source, or make them build
  dependencies. When bundled docs or Orc assumptions conflict with Ticket's
  current protocol, follow `../ticket`.

## Go toolchain and caches

Use the Go version required by `go.mod` and the `go` command available in your
environment. Keep validation offline, reuse one bounded, stable set of
user-local Go cache directories across checkouts and commands, and do not
create a separate cache for each task:

```sh
go_cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/ticket-orc/go"
mkdir -p "$go_cache_root/build" "$go_cache_root/path" "$go_cache_root/mod"
export GOTOOLCHAIN=local GOPROXY=off
export GOCACHE="$go_cache_root/build"
export GOPATH="$go_cache_root/path"
export GOMODCACHE="$go_cache_root/mod"
go test ./...
```

Use the same cache paths for formatting, tests, race tests, vet, package
listing, and builds. Go caches support concurrent processes. Use a unique
temporary directory only when a specific test requires isolation, then remove
it when that test finishes.

Do not download or install toolchains, modules, dependencies, linters,
validators, or other executables unless the user explicitly authorizes that
download. If an installed tool cannot perform a check offline, report the
limitation instead of fetching anything. Do not flush shared Go caches unless
diagnostics identify a specific cache defect and the user explicitly
authorizes the repair. For disk cleanup, preserve paths used by active Go
processes and remove only stale temporary cache directories.

## Documentation policy

Keep documentation layered.

### README

`README.md` is manually curated. Do not edit it unless the user explicitly asks
for a README change. When implementation work changes documented behavior,
report the needed README update to the user without changing the file.

Document user-facing behavior only:

* what Orc does;
* configured managed workers and dynamic steer sessions;
* installation and configuration;
* operator workflows;
* concise examples;
* supported environment variables and observable behavior.

Do not explain internal state machines, lock ordering, compatibility history,
delivery bookkeeping, or agent-specific safety rationale unless users need it
to operate Orc.

### CLI help

Document exact syntax and observable behavior.

Help text is part of the interface. Keep it concise and consistent with actual
configuration defaults.

### Skills

The Ticket Skill owns Ticket mechanics.

An Orc Skill should contain only the extra behavior an externally-steered worker
must follow. Do not use Skills to compensate for missing environment setup or
to teach agents Orc's internal architecture.

### Code/tests

Implementation contracts, concurrency rules, state-schema invariants, recovery
semantics, and security/safety details belong in code comments and regression
tests.

## Change discipline

Before adding a new command or concept, check whether an existing primitive
already owns that responsibility.

When fixing a bug:

* identify the violated invariant;
* add the smallest regression test that proves it;
* fix the owning layer;
* remove obsolete recovery or compatibility logic made unnecessary by the fix.

Do not broaden scope during release-polish work.

Do not preserve code solely because it survived previous prerelease iterations.
Git history is the archive.

After the public-release cleanup is complete, stop architectural refactoring
unless a concrete defect, measurable maintenance problem, or new supported
requirement justifies it.

## Build and release

Keep build and release mechanics visible.

* `go.mod` is authoritative for the Go toolchain version.
* normal builds should remain ordinary `go build` commands;
* release packaging may use Make and GitHub Actions directly;
* avoid shell-wrapper layers unless they remove real duplicated complexity;
* keep version/commit injection centralized and reproducible;
* release validation must run before publication;
* keep the standard-library-only dependency policy unless explicitly changed by
  project decision.

## Validation

During normal ticket development, do not run the full race suite as a routine
per-change check. Run the focused tests for the changed behavior; for
concurrency-sensitive changes, run race tests for the affected packages when
practical. Reserve the full uncached race suite for final pre-release
validation, or when a concrete issue requires it.

For normal changes, run:

```sh
go fmt ./...
go test ./...
go vet ./...
```

For concurrency-sensitive changes, run focused race tests for affected packages
when practical.

Before a public release, run the complete uncached race suite:

```sh
go test -race -count=1 ./...
```

Also preserve repository build/release checks and verify:

```sh
git diff --check
```

For architecture-sensitive changes, add targeted checks for the invariant being
changed rather than relying only on the full suite.
