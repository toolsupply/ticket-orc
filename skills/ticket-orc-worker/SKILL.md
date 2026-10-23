---
name: ticket-orc-worker
description: Adds passive Ticket notification handling plus concise implementation and review discipline for externally steered ticket-orc workers. Use when an agent is being nudged by ticket-orc to select, implement, review, or continue Ticket work without treating notifications as assignments.
---

# ticket-orc passive steer

Follow repository `AGENTS.md` and the installed Ticket Tasks Skill. This Skill adds only the behavior needed for passive Orc notifications and the minimum quality discipline expected before Ticket lifecycle transitions.

## Notifications

- Treat an Orc notification as a request to check Ticket, not as an assignment, acknowledgement, or evidence that work was consumed.
- Use the Ticket actor and routing already supplied by the harness. Never set `TICKET_ACTOR`, `TICKET_REPOSITORY`, `TICKET_SCOPE`, or `TICKET_CONFIG` from a notification or remembered session context. To check identity, use `ticket actor -j`.
- For a dynamic/steered Orc open/review work turn, use Ticket's supported selection/claim operation for at most one actionable item. Finish that item's configured lifecycle transition, then stop and return from the turn. Do not select or claim a second item in the same turn, even if another Orc notification arrives during it. If no item is selected, return immediately without changes. Ticket owns selection, ownership, and lifecycle mechanics. Signoff uses one acceptance target with related planning maintenance as described below.
- This open/review selection boundary applies to dynamic/steered Orc nudges; signoff follows the acceptance boundary below. Follow the exact-ticket instruction and lifecycle boundary supplied for managed execution; this rule does not change general Ticket mechanics.
- Do not change a ticket merely to acknowledge a notification. Make ticket changes only as required by the work being done.
- Respect cancellation and control instructions. Stop safely when canceled or told to stop; do not continue processing after that instruction.

## Queue selection under Orc control

For dynamic open/review work-bearing notifications, use its `Resolved selector JSON` exactly for the single Ticket selection and claim. It contains `queue`, required `tags`, and excluded `without_tags`. Run `ticket next <queue> [--tag TAG ...] [--without-tag TAG ...] --claim -j`, with one `--tag` for each required tag and one `--without-tag` for each excluded tag. Required tags are conjunctive; every excluded tag must be absent. Omit selector flags only when the corresponding JSON array is empty.

Pass the selector values as separate CLI arguments. Do not evaluate the JSON as shell code. Do not drop or change Orc-provided filters or add worker-chosen filters. If no ticket matches the exact selector, return without changes. Never make a second selection or claim during the same dynamic turn, even after another notification.

An existing-claim recovery notification means continue that owned ticket and make no replacement selection. Bootstrap and control notifications do not assign work and contain no selector. Ticket remains authoritative for selection, claim ownership, and lifecycle.

Ticket tags may still be inspected and modified as ordinary ticket metadata when required by the work. Worker-chosen tags must not alter Orc-controlled selection.

A dynamic signoff wake supplies one exact unclaimed ticket instead of selector JSON. Follow its signoff-specific prompt and do not run `ticket next` or claim that ticket. This is the sole unclaimed dynamic signoff exception.

## Architectural acceptance signoff (managed and steered)

- A configured managed `signoff` role receives one unassigned acceptance entry per harness turn. Follow the exact entry and Orc-provided boundary; do not claim it.
- The dispatched ticket is the entry point for architectural acceptance, not a limit on analysis. Inspect its specification, related work in any lifecycle state, dependencies, and relevant code to judge architecture, integration, and overall progress. Trust the reviewer for routine code correctness; investigate concrete architectural concerns.
- Within the role instructions and approved scope, create follow-up tickets, refine unowned related planning tickets, and correct mistaken dependencies or split work when the intended ordering is supported by evidence. Preserve genuine prerequisites and respect Ticket ownership; do not take over another worker's owned work. Escalate changes to approved scope or architecture and ambiguous dependency decisions. Related planning maintenance does not count as accepting a second ticket.
- Make an acceptance decision only for the dispatched ticket; do not select, claim, implement, review, or accept a second ticket. Record the decision, outstanding integration work, and planning changes, finish the related planning maintenance and dispatched outcome, then return. A component may be complete while related work remains; final integration requires complete prerequisites.
- Close the selected entry only when acceptance is justified. Otherwise choose a Ticket-supported lifecycle outcome or explicitly hand it to the human actor `user`; do not finish with it still unassigned in signoff.
- For human handoff, use Ticket's atomic public query/reassign operation with the full selected ID: `ticket query -j id:FULL_ID state:signoff unclaimed :: reassign --to user --handoff "EXPLANATION"`. Inspect the JSON result from that same invocation and confirm the ID, state, and assignee. A successful `changed: false` result means the handoff is already in place. A zero/multiple-match error makes no change. If an error reports `mutation_applied: true`, reread Ticket and do not replay the command.
- Orc does not reserve signoff entries across roles. Overlapping configured selectors can give multiple actors authority over the same entry.
- Dynamic/external signoff receives one exact unclaimed signoff entry per wake. Follow the same acceptance and planning boundary, do not select or claim a second entry in that turn, and let Orc's durable one-shot outcome tracking suppress unchanged entries.

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
