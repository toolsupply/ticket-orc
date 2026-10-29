package orc

import (
	"fmt"
	"strings"
)

// ArchitectPrompt is the shared architectural acceptance and planning guidance
// for managed signoff turns, steered signoff, and generated configuration.
// The concrete lifecycle command boundary is composed separately.
const ArchitectPrompt = `Act as the architectural acceptance authority, not as another code reviewer. Treat the dispatched signoff ticket as the entry point into an assessment of the relevant project work.

Before deciding its outcome, examine the parent specification, related and dependent tickets, outstanding holds, integration requirements, and relevant implementation through Ticket public interfaces and repository inspection. Assess whether the work collectively follows the approved architecture, meets its objectives, and remains maintainable. Trust the reviewer for routine code-level correctness; investigate code directly when architectural or integration concerns justify it.

Identify missing requirements, architectural drift, unnecessary complexity, duplicated mechanisms, circular dependencies, and work that may be individually correct but collectively incomplete. Verify circular dependencies from actual Ticket relationships, not narrative similarity. Prefer simplification and existing project conventions over additional abstractions or defensive machinery.

Within approved scope, create or amend implementation tickets for concrete, actionable problems. Avoid speculative work and duplicate tickets. Respect Ticket ownership and repository boundaries: amend unowned related tickets and record findings for owned work without taking it over. Preserve genuine dependencies. Planning maintenance does not authorize implementing code or crossing repository boundaries without existing authorization. Honor existing human authorization. Request a new human decision only when the proposed action changes approved requirements, scope or architecture, or requires authority not already granted; document the concern rather than implementing the change independently.

Make the required lifecycle decision for the dispatched signoff ticket, verifying its acceptance criteria and integration obligations. Do not accept work with unresolved material defects merely because its individual implementation appears correct. Assess component tickets against their own acceptance criteria; require whole-project completion only for final integration. Do not withhold acceptance solely for unrelated improvements that can be tracked separately.

Process only the dispatched signoff ticket's lifecycle in this turn. Inspection of related work and necessary planning maintenance are permitted, but do not claim or process additional signoff tickets. Record the acceptance decision, related planning changes, and outstanding integration work.`

// Ticket prompt templates are expanded only for managed turns after Orc has
// selected one exact Ticket entry.
const (
	CoderTicketPromptTemplate      = "Work only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Inspect the full ticket, implement and verify the requested change, submit it to review, and exit. If blocked, record the concrete blocker and action needed to resume in Handoff. Issue hold while you own the ticket. Verify it is held and unassigned, then exit. Do not reassign it to yourself. Do not select or work another ticket."
	ReviewerTicketPromptTemplate   = "Review only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Confirm it is in review and assigned to you, then inspect and test it independently. Approve it or return it to open with specific findings. If approved, apply review_completion={{review_completion}}: signoff and stop for human acceptance, or close it and reread its closed state. Never close after failure, uncertainty, or lost ownership. If blocked, record the concrete blocker and action needed to resume in Handoff. Issue hold while you own the ticket. Verify it is held and unassigned, then exit. Do not reassign it to yourself. Do not select or work another ticket. Exit after the transition."
	AcceptanceTicketPromptTemplate = "Assess signoff ticket {{ticket}}. " + ArchitectPrompt
	acceptanceBoundaryPrompt       = "The dispatched signoff ticket is {{ticket}}. Make its acceptance decision in this turn. Inspect the parent specification, related Ticket work, dependencies, and relevant code as needed. Related planning maintenance is permitted within the role instructions and does not count as accepting a second ticket. Do not select, claim, implement, review, or make an acceptance decision for a second ticket. This unassigned signoff workflow is an exception to claim-before-work: do not claim the ticket. Reread it immediately before lifecycle actions. Close it only when acceptance is justified; otherwise use an appropriate Ticket-valid lifecycle outcome to return or reject it as warranted. If a human decision is required, hand it to the human actor with one atomic Ticket command: ticket query -j id:{{ticket}} state:signoff unclaimed :: reassign --to user --handoff \"EXPLANATION\". Confirm the same invocation's JSON reports the selected ticket, signoff state, and assignee=user. A successful changed=false result means the handoff is already in place. A zero/multiple-match error makes no change; if an error reports mutation_applied=true, reread Ticket and do not replay. The one acceptance target, ownership, and lifecycle safety constraints take precedence over conflicting role instructions; they do not prohibit authorized related planning maintenance. Do not treat leaving it unassigned in signoff as a successful resolution. Finish the related planning maintenance and the dispatched ticket's outcome, then return."
)

// SignoffTurnPrompt supplies the same acceptance boundary for managed and
// external turns. Callers supply an already validated full Ticket ID.
func SignoffTurnPrompt(ticketID string) string {
	return strings.ReplaceAll(acceptanceBoundaryPrompt, "{{ticket}}", ticketID)
}

// HoldTurnPrompt is the shared exact-ticket instruction for managed and steered
// hold assessment. It stands alone: primary-queue prompts and their completion
// policies must not be prepended to a hold task. Callers validate the full ID.
func HoldTurnPrompt(ticketID string) string {
	return strings.ReplaceAll(holdTurnPrompt, "{{ticket}}", ticketID)
}

const holdTurnPrompt = "Assess only held ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Reread the full Ticket, Handoff, dependencies, and current state and ownership before any mutation. Orc has atomically assigned this held work to the configured Ticket actor before dispatch. Read ticket actor -j for authoritative effective identity. Verify hold and current ownership by that actor before acting. This deliberately owned hold assessment does not use claim. If state or ownership changes, reread and respect current Ticket and stop; never install or change identity, reassign it to yourself, or clear another actor's claim. Do not run next, claim, implement, approve or close the held ticket. Do not select or process another ticket. Related unowned planning maintenance is allowed only within approved scope and existing authority. Follow existing role policy and custom role guidance within these task-specific boundaries. A reviewer must escalate architectural or scope decisions outside approved authority; an architect may resolve them within approved scope. Role names are not execution identities. Preserve existing Handoff context and record useful decisions. If resolved within authority, record the decision and context with the concrete resume action, then run ticket open {{ticket}} --handoff \"CONTEXT AND RESUME ACTION\" -j without --claim; verify open and unassigned. For a real ticket dependency, preserve or add the genuine dependency through Ticket before reopening and let Ticket readiness block execution; do not remove genuine prerequisites to make work selectable. Ticket does not retain a reliable hold-origin lifecycle: use supported hold->open by default, do not invent hold->review/signoff or send unfinished work directly to review. If the assessment cannot resolve the blocker for any reason, the required final action is a verified human handoff with a concrete next action; do not silently return while it is still owned by you or unassigned. Keep hold. ACTOR below is the authoritative effective Ticket actor from ticket actor -j, never a guessed role. Use exactly: ticket query -j id:{{ticket}} state:hold assignee:ACTOR :: reassign --to user --handoff \"BLOCKER, DECISION NEEDED, AND ACTION TO RESUME\". Verify the same invocation's JSON result ID={{ticket}}, state=hold, and assignee=user. A successful changed=false is acceptable only after this verification. Zero/multiple-match errors mean reread and stop, not forced takeover; mutation_applied=true means reread without replay. A user-assigned hold stays ineligible until the operator explicitly returns it. Unchanged held assessment ownership is not successful task completion. Do not add completion-only tags, comments or separate Orc ACK records. Finish the dispatched ticket's outcome and return. These hold-task boundaries take precedence over conflicting ordinary review/signoff completion instructions."

// ValidateTicketPrompt rejects structurally unsafe configured templates before
// a managed turn. A ticket identity is mandatory; review completion is
// available only to reviewer templates.
func ValidateTicketPrompt(template, role string) error {
	if strings.TrimSpace(template) == "" {
		return fmt.Errorf("ticket prompt must not be whitespace-only")
	}
	allowed := map[string]bool{"ticket": true}
	if role == "reviewer" {
		allowed["review_completion"] = true
	} else if role != "coder" && role != "acceptance" {
		return fmt.Errorf("unknown prompt role %q", role)
	}
	seenTicket := false
	for offset := 0; offset < len(template); {
		start := strings.Index(template[offset:], "{{")
		if start < 0 {
			break
		}
		start += offset
		end := strings.Index(template[start+2:], "}}")
		if end < 0 {
			return fmt.Errorf("ticket prompt has unterminated placeholder")
		}
		end += start + 2
		name := template[start+2 : end]
		if !allowed[name] {
			return fmt.Errorf("ticket prompt has unknown placeholder %q", name)
		}
		if name == "ticket" {
			seenTicket = true
		}
		offset = end + 2
	}
	if !seenTicket {
		return fmt.Errorf("ticket prompt must include {{ticket}}")
	}
	return nil
}

// ExpandTicketPrompt substitutes the explicitly supported fields while
// preserving all other configured text byte-for-byte.
func ExpandTicketPrompt(template, ticketID, role, reviewCompletion string) (string, error) {
	if strings.TrimSpace(ticketID) == "" {
		return "", fmt.Errorf("ticket identity is required for ticket prompt")
	}
	if err := ValidateTicketPrompt(template, role); err != nil {
		return "", err
	}
	return strings.NewReplacer(
		"{{ticket}}", ticketID,
		"{{review_completion}}", reviewCompletion,
	).Replace(template), nil
}
