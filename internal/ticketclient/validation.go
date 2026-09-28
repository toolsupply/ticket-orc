package ticketclient

import (
	"fmt"
	"strings"
	"time"
)

func validateMutationResult(result MutationResult, id string) error {
	if result.ID != id {
		return fmt.Errorf("%w: mutation returned ticket %q, expected %q", ErrProtocol, result.ID, id)
	}
	return nil
}

func validateMutationID(id string) error { return validateFullID(id) }

func validActor(actor string) bool {
	if len(actor) == 0 || len(actor) > 128 || !asciiLetterOrDigit(actor[0]) {
		return false
	}
	for i := 1; i < len(actor); i++ {
		if !asciiLetterOrDigit(actor[i]) && actor[i] != '.' && actor[i] != '_' && actor[i] != '-' {
			return false
		}
	}
	return true
}

// ValidateActor checks the explicit actor token accepted by Ticket clients.
// UI mutation gateways use it before launching a child process so malformed
// identity input is reported as a request error rather than a repository
// failure.
func ValidateActor(actor string) error {
	if strings.TrimSpace(actor) != actor || !validActor(actor) {
		return fmt.Errorf("ticket actor is invalid")
	}
	return nil
}

func asciiLetterOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func validateFullID(id string) error {
	if len(id) != 14 || id[8] != '-' {
		return fmt.Errorf("ticket ID must be a full YYYYMMDD-NNNNN ID")
	}
	if _, err := time.Parse("20060102", id[:8]); err != nil {
		return fmt.Errorf("ticket ID has an invalid date")
	}
	for _, digit := range id[9:] {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("ticket ID suffix must contain five digits")
		}
	}
	return nil
}

// ValidateFullID checks the explicit ticket identifier format used by all
// lifecycle operations. Maintenance commands use it before touching state.
func ValidateFullID(id string) error { return validateFullID(id) }
