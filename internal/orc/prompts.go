package orc

import (
	"fmt"
	"strings"
)

// Ticket prompt templates are intentionally small. They are expanded only
// for a managed turn after ticket-orc has claimed one exact ticket.
const (
	CoderTicketPromptTemplate    = "Work only ticket {{ticket}}. Follow repository instructions and the Ticket Skill. Inspect the full ticket, implement and verify the requested change, submit it to review, and exit. Do not select or work another ticket."
	ReviewerTicketPromptTemplate = "Review only ticket {{ticket}}. Confirm it is in review and assigned to you, then inspect and test it independently. Approve it or return it to open with specific findings. If approved, apply review_completion={{review_completion}}: signoff and stop for human acceptance, or close it and reread its closed state. Never close after failure, uncertainty, or lost ownership. Exit after the transition."
)

// ValidateTicketPrompt rejects structurally unsafe configured templates before
// a managed claim. A ticket identity is mandatory; review completion is
// available only to reviewer templates.
func ValidateTicketPrompt(template, role string) error {
	if strings.TrimSpace(template) == "" {
		return fmt.Errorf("ticket prompt must not be whitespace-only")
	}
	allowed := map[string]bool{"ticket": true}
	if role == "reviewer" {
		allowed["review_completion"] = true
	} else if role != "coder" {
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
