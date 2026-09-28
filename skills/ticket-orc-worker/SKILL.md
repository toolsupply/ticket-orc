---
name: ticket-orc-worker
description: Adds passive Ticket notification handling plus concise implementation and review discipline for externally steered ticket-orc workers. Use when an agent is being nudged by ticket-orc to select, implement, review, or continue Ticket work without treating notifications as assignments.
---

# ticket-orc passive steer

Follow repository `AGENTS.md` and the installed Ticket Tasks Skill. This Skill adds only the behavior needed for passive Orc notifications and the minimum quality discipline expected before Ticket lifecycle transitions.

## Notifications

- Treat an Orc notification as a request to check Ticket, not as an assignment, acknowledgement, or evidence that work was consumed.
- Use the Ticket actor and routing already supplied by the harness. Never set `TICKET_ACTOR`, `TICKET_REPOSITORY`, `TICKET_SCOPE`, or `TICKET_CONFIG` from a notification or remembered session context. To check identity, use `ticket actor -j`.
- Use Ticket's supported work-selection operation and keep processing work actionable for the effective actor until Ticket reports that none remains. Ticket owns selection, ownership, and lifecycle mechanics.
- Finish the current ticket's configured lifecycle transition before selecting more work. Continue selecting and processing until a work-selection operation performed after the most recent transition reports no actionable work; only then conclude the Orc-driven turn.
- Do not change a ticket merely to acknowledge a notification. Make ticket changes only as required by the work being done.
- Respect cancellation and control instructions. Stop safely when canceled or told to stop; do not continue processing after that instruction.

## Implementation work

- Read the ticket objective and acceptance criteria before changing code. Implement the requested behavior, not merely what existing tests happen to cover.
- Before submitting work for review, perform one deliberate self-review: re-read the objective and acceptance criteria, inspect your changes, and run the relevant routine tests and checks.
- Fix obvious correctness, completeness, integration, error-handling, and specification-compliance issues before submission. Do not rely on the reviewer as the first pass for basic compliance.
- If the worktree uses Git and you are instructed to commit your work, prefix commit messages with the ticket ID unless instructed to follow a project-specific commit-message convention.
- Do not abandon claimed work by simply releasing the claim without context. If work cannot be completed, leave a clear handoff message or comment describing the state, findings, and remaining work, then use the lifecycle transition required by the Ticket Skill or repository policy.

## Review work

- Perform a substantive review. Read the ticket objective, acceptance criteria, and handoff, then inspect the implementation and relevant changed code.
- Check specification compliance, correctness, edge cases, integration and regression risk, error handling, and maintainability. Passing tests alone is not sufficient evidence for approval.
- Assume the coder has already run the routine/basic test suite. Do not rerun the same tests by default; prefer targeted tests or checks that investigate a specific concern or verify a finding. Rerun routine tests when required by repository policy, when results are missing or questionable, or when needed to establish confidence.
- If problems are found, record specific, actionable findings and use the Ticket lifecycle required by the Ticket Skill or repository policy.
- If satisfied, follow any explicit completion policy in the Orc notification, such as approving and closing or approving to signoff. Otherwise follow the Ticket Skill and repository policy.
- Do not modify implementation code or commit changes as the reviewer. Record findings and leave implementation changes to the coder.
