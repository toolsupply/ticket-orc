package orc

import (
	"fmt"
	"strings"
)

// Ticket prompt templates are intentionally small. They are expanded only
// for managed turns after Orc has selected one exact Ticket entry.
const (
	CoderTicketPromptTemplate      = "Work only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Inspect the full ticket, implement and verify the requested change, submit it to review, and exit. If blocked, record the concrete blocker and action needed to resume in Handoff; use Ticket to hold this ticket while you still own it, verify hold, and exit. Do not release it first. Do not select or work another ticket."
	ReviewerTicketPromptTemplate   = "Review only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Confirm it is in review and assigned to you, then inspect and test it independently. Approve it or return it to open with specific findings. If approved, apply review_completion={{review_completion}}: signoff and stop for human acceptance, or close it and reread its closed state. Never close after failure, uncertainty, or lost ownership. If blocked, record the concrete blocker and action needed to resume in Handoff; use Ticket to hold this ticket while you still own it, verify hold, and exit. Do not release it first. Do not select or work another ticket. Exit after the transition."
	AcceptanceTicketPromptTemplate = "Assess signoff ticket {{ticket}} for final acceptance. Inspect related Ticket work as needed and determine whether acceptance is justified."
	acceptanceBoundaryPrompt       = "Resolve only selected signoff ticket {{ticket}} in this turn. This unassigned signoff workflow is an exception to claim-before-work: do not claim the ticket. Reread it immediately before lifecycle actions. Inspect related Ticket work as needed. Close it only when acceptance is justified; otherwise use an appropriate Ticket-valid lifecycle outcome to return or reject it as warranted. If a human decision is required, hand it to the human actor with one atomic Ticket command: ticket query -j id:{{ticket}} state:signoff unclaimed :: reassign --to user --handoff \"EXPLANATION\". Confirm the same invocation's JSON reports the selected ticket, signoff state, and assignee=user. A successful changed=false result means the handoff is already in place. A zero/multiple-match error makes no change; if an error reports mutation_applied=true, reread Ticket and do not replay. Do not treat leaving it unassigned in signoff as a successful resolution. Do not process a second signoff ticket in this turn. Exit after this one-ticket turn."
)

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
