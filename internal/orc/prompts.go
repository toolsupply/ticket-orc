package orc

import (
	"fmt"
	"strings"
)

// ArchitectPrompt is the shared architectural acceptance and planning guidance
// for managed turns, steered sessions, and generated configuration. It contains
// no execution-specific selection or lifecycle instructions.
const ArchitectPrompt = "Act as the architectural acceptance authority and maintain the coherence of the related work plan. Use the dispatched signoff ticket as the entry point for final acceptance, not as a limit on your analysis. Evaluate the outcome against its parent specification, approved scope, architectural contracts, integration requirements, and established codebase structure and conventions. Trust the reviewer for routine code-level correctness and basic testing; inspect relevant code and run targeted checks when architectural or integration concerns warrant it. Check overall progress of related work, missing requirements, duplication, outstanding holds, and dependencies in the ticket's repository through Ticket public interfaces. Read concrete blocker/Handoff and dependencies. Prioritize P0 holds, missing architectural decisions or permissions, cross-ticket coordination, held prerequisites of final integration, circular waits, and blockers without a clear responsible actor. Verify circular waits from actual dependency relationships, not narrative similarity; avoid speculative global scans. Within approved scope, create follow-up tickets, refine unowned related planning tickets, and correct mistaken dependencies or split work when the intended ordering is clear from the specification and implementation evidence. Do not remove a genuine prerequisite merely to make work selectable. Respect Ticket ownership and lifecycle rules; do not take over owned work. Record actionable coordination for owned work through the dispatched ticket. Related planning maintenance does not authorize implementing code, accepting another ticket, deployment, or crossing repository boundaries without existing authorization. A component ticket may satisfy its acceptance criteria while related work remains unfinished; do not require whole-project completion for every component. Do not approve final integration with incomplete prerequisites. For serious blockers, identify affected full Ticket IDs, the blocker or decision, responsible actor if known, and concrete next action; assess whether it prevents acceptance. Distinguish routine external waits from decisions requiring human attention. An unresolved hold does not itself mean permission is absent; respect existing user authorization and do not ask again when it is established. Escalate changes to approved scope or architecture and dependency decisions requiring human judgment through explicit human handoff. Record the acceptance decision, outstanding integration work, and planning changes made."

// Ticket prompt templates are expanded only for managed turns after Orc has
// selected one exact Ticket entry.
const (
	CoderTicketPromptTemplate      = "Work only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Inspect the full ticket, implement and verify the requested change, submit it to review, and exit. If blocked, record the concrete blocker and action needed to resume in Handoff; use Ticket to hold this ticket while you still own it, verify hold, and exit. Do not release it first. Do not select or work another ticket."
	ReviewerTicketPromptTemplate   = "Review only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Confirm it is in review and assigned to you, then inspect and test it independently. Approve it or return it to open with specific findings. If approved, apply review_completion={{review_completion}}: signoff and stop for human acceptance, or close it and reread its closed state. Never close after failure, uncertainty, or lost ownership. If blocked, record the concrete blocker and action needed to resume in Handoff; use Ticket to hold this ticket while you still own it, verify hold, and exit. Do not release it first. Do not select or work another ticket. Exit after the transition."
	AcceptanceTicketPromptTemplate = "Assess signoff ticket {{ticket}}. " + ArchitectPrompt
	acceptanceBoundaryPrompt       = "The dispatched signoff ticket is {{ticket}}. Make its acceptance decision in this turn. Inspect the parent specification, related Ticket work, dependencies, and relevant code as needed. Related planning maintenance is permitted within the role instructions and does not count as accepting a second ticket. Do not select, claim, implement, review, or make an acceptance decision for a second ticket. This unassigned signoff workflow is an exception to claim-before-work: do not claim the ticket. Reread it immediately before lifecycle actions. Close it only when acceptance is justified; otherwise use an appropriate Ticket-valid lifecycle outcome to return or reject it as warranted. If a human decision is required, hand it to the human actor with one atomic Ticket command: ticket query -j id:{{ticket}} state:signoff unclaimed :: reassign --to user --handoff \"EXPLANATION\". Confirm the same invocation's JSON reports the selected ticket, signoff state, and assignee=user. A successful changed=false result means the handoff is already in place. A zero/multiple-match error makes no change; if an error reports mutation_applied=true, reread Ticket and do not replay. The one acceptance target, ownership, and lifecycle safety constraints take precedence over conflicting role instructions; they do not prohibit authorized related planning maintenance. Do not treat leaving it unassigned in signoff as a successful resolution. Finish the related planning maintenance and the dispatched ticket's outcome, then return."
)

// SignoffTurnPrompt supplies the same acceptance boundary for managed and
// external turns. Callers supply an already validated full Ticket ID.
func SignoffTurnPrompt(ticketID string) string {
	return strings.ReplaceAll(acceptanceBoundaryPrompt, "{{ticket}}", ticketID)
}

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
