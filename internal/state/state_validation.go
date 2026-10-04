package state

import (
	"fmt"
	"strings"
	"unicode"
)

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.Version != currentVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrMalformed, snapshot.Version)
	}
	type sessionHistory struct {
		current bool
		ids     map[string]struct{}
		count   int
	}
	seen := make(map[string]*sessionHistory, len(snapshot.Sessions))
	for _, session := range snapshot.Sessions {
		if err := validateSession(session); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		history := seen[sessionKey(session)]
		if history == nil {
			history = &sessionHistory{ids: make(map[string]struct{})}
			seen[sessionKey(session)] = history
		}
		if _, exists := history.ids[session.ID]; exists {
			return fmt.Errorf("%w: duplicate session ID in history", ErrMalformed)
		}
		history.ids[session.ID] = struct{}{}
		history.count++
		if session.IsCurrent() {
			if history.current {
				return fmt.Errorf("%w: multiple current sessions for ownership key", ErrMalformed)
			}
			history.current = true
		}
	}
	for _, history := range seen {
		if history.count > maxSessionHistoryPerKey {
			return fmt.Errorf("%w: session history exceeds limit", ErrMalformed)
		}
	}
	for key, loop := range snapshot.Loops {
		if key != ticketLoopKey(loop.Repository, loop.Ticket) {
			return fmt.Errorf("%w: ticket loop key mismatch", ErrMalformed)
		}
		if err := validateRepositoryIdentity("ticket loop", loop.Repository); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if err := validateToken("ticket loop ticket", loop.Ticket); err != nil {
			return fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if loop.StallCount < 0 || loop.BounceCount < 0 || loop.EffectiveBounceLimit < 0 {
			return fmt.Errorf("%w: negative ticket loop count or limit for %s", ErrMalformed, key)
		}
		if loop.Phase != TicketLoopActive && loop.Phase != TicketLoopContainmentPending && loop.Phase != TicketLoopHeld {
			return fmt.Errorf("%w: invalid ticket loop phase %q", ErrMalformed, loop.Phase)
		}
		if loop.Phase == TicketLoopActive && loop.EffectiveBounceLimit > 0 && loop.BounceCount >= loop.EffectiveBounceLimit {
			return fmt.Errorf("%w: active ticket loop has exceeded its bounce limit", ErrMalformed)
		}
		if loop.HeldFrom != "" && loop.HeldFrom != "open" && loop.HeldFrom != "review" {
			return fmt.Errorf("%w: invalid ticket loop held_from %q", ErrMalformed, loop.HeldFrom)
		}
		if loop.ClaimState != "" && loop.ClaimState != "open" && loop.ClaimState != "review" {
			return fmt.Errorf("%w: invalid ticket loop claim_state %q", ErrMalformed, loop.ClaimState)
		}
		if (loop.ClaimState == "") != (loop.ClaimActor == "") {
			return fmt.Errorf("%w: incomplete ticket loop claim observation", ErrMalformed)
		}
		if loop.Phase == TicketLoopHeld && (loop.ClaimState != "" || loop.DispatchPending) {
			return fmt.Errorf("%w: held ticket loop has active attempt state", ErrMalformed)
		}
		if loop.DispatchPending && loop.ClaimState != "" {
			return fmt.Errorf("%w: ticket loop has both a pending dispatch and active claim", ErrMalformed)
		}
		if loop.ClaimActor != "" {
			if err := validateSteerName("actor", loop.ClaimActor); err != nil {
				return fmt.Errorf("%w: invalid ticket loop claim actor", ErrMalformed)
			}
		}
	}
	return nil
}

func validateSession(session Session) error {
	if err := validateRepositoryIdentity("session", session.Repository); err != nil {
		return err
	}
	if err := validateToken("session ticket", session.Ticket); err != nil {
		return err
	}
	if err := validateToken("session role", session.Role); err != nil {
		return err
	}
	if err := validateToken("session harness", session.Harness); err != nil {
		return err
	}
	if session.Owner != "" {
		if err := validateToken("session owner", session.Owner); err != nil {
			return err
		}
	}
	if err := validateToken("session ID", session.ID); err != nil {
		return err
	}
	if session.SupersededAt != nil && session.SupersededReason == "" {
		return fmt.Errorf("session supersession reason must not be empty")
	}
	if session.SupersededAt == nil && session.SupersededReason != "" {
		return fmt.Errorf("session supersession time must be present")
	}
	if session.SupersededReason != "" && strings.IndexFunc(session.SupersededReason, unicode.IsControl) >= 0 {
		return fmt.Errorf("session supersession reason must not contain control characters")
	}
	if !session.ContextTelemetry.Valid() {
		return fmt.Errorf("session context telemetry is malformed")
	}
	if session.ReplacedBy != "" {
		if err := validateToken("replacement session ID", session.ReplacedBy); err != nil {
			return err
		}
	}
	if session.IsCurrent() && session.ReplacedBy != "" {
		return fmt.Errorf("current session cannot have a replacement ID")
	}
	return nil
}

func validateToken(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must not contain NUL", name)
	}
	return nil
}

func validateRepositoryIdentity(name, repository string) error {
	if err := validateToken(name+" repository", repository); err != nil {
		return err
	}
	if strings.IndexFunc(repository, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s repository must not contain control characters", name)
	}
	if isStableTicketRepositoryID(repository) {
		return nil
	}
	if normalizeRepositoryIdentity(repository) != repository {
		return fmt.Errorf("%s repository identity must be canonical", name)
	}
	return nil
}

func sameSessionKey(left, right Session) bool {
	return left.Repository == right.Repository && left.Ticket == right.Ticket && left.Role == right.Role && left.Harness == right.Harness && left.Owner == right.Owner
}

func sessionKey(session Session) string {
	return session.Repository + "\x00" + session.Ticket + "\x00" + session.Role + "\x00" + session.Harness + "\x00" + session.Owner
}
