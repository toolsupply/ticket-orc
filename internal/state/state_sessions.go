package state

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
)

func (s *Store) GetSession(ctx context.Context, ticket, role, harness string, owners ...string) (Session, bool, error) {
	owner := ""
	if len(owners) > 1 {
		return Session{}, false, fmt.Errorf("session owner accepts at most one value")
	}
	if len(owners) == 1 {
		owner = owners[0]
	}
	key := Session{Repository: s.repository, Ticket: ticket, Role: role, Harness: harness, Owner: owner, ID: "lookup"}
	if err := validateSession(key); err != nil {
		return Session{}, false, err
	}

	var result Session
	found := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for _, session := range data.Sessions {
			if s.matchesNamespace(session.Repository) && sameSessionKey(session, key) && session.IsCurrent() {
				result, found = session, true
				break
			}
		}
		return nil
	})
	return result, found, err
}

// SetSession creates or replaces one retained session mapping.
func (s *Store) SetSession(ctx context.Context, session Session) error {
	if err := s.setNamespace(&session.Repository); err != nil {
		return err
	}
	if err := validateSession(session); err != nil {
		return err
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for _, current := range data.Sessions {
			if s.matchesNamespace(current.Repository) && sameSessionKey(current, session) && current.ID == session.ID && !current.IsCurrent() {
				return ErrSessionSuperseded
			}
		}
		for i, current := range data.Sessions {
			if !s.matchesNamespace(current.Repository) || !sameSessionKey(current, session) {
				continue
			}
			if current.IsCurrent() {
				data.Sessions[i] = session
				return s.saveUnlocked(data)
			}
		}
		for _, current := range data.Sessions {
			if s.matchesNamespace(current.Repository) && sameSessionKey(current, session) && current.ID == session.ID {
				return ErrSessionSuperseded
			}
		}
		data.Sessions = append(data.Sessions, session)
		return s.saveUnlocked(data)
	})
}

// SupersedeSession durably retires an exact current session before a caller
// starts its replacement. The returned bool is false when that ID is no
// longer current, allowing callers to re-read rather than overwrite a race.
func (s *Store) SupersedeSession(ctx context.Context, expected Session, reason string) (Session, bool, error) {
	if err := s.setNamespace(&expected.Repository); err != nil {
		return Session{}, false, err
	}
	if err := validateSession(expected); err != nil {
		return Session{}, false, err
	}
	if err := validateToken("session supersession reason", reason); err != nil {
		return Session{}, false, err
	}
	var retired Session
	changed := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i := range data.Sessions {
			current := data.Sessions[i]
			if !s.matchesNamespace(current.Repository) || !sameSessionKey(current, expected) || current.ID != expected.ID || !current.IsCurrent() {
				continue
			}
			supersededAt := time.Now().UTC()
			current.SupersededAt = &supersededAt
			current.SupersededReason = reason
			data.Sessions[i] = current
			retired, changed = current, true
			return s.saveUnlocked(data)
		}
		return nil
	})
	return retired, changed, err
}

// RegisterSession adds a newly created session without permitting a retired
// ID to become current again. supersedesID links the new record to the
// already-durable tombstone when replacing a prior current session.
func (s *Store) RegisterSession(ctx context.Context, session Session, supersedesID string) error {
	if err := s.setNamespace(&session.Repository); err != nil {
		return err
	}
	if err := validateSession(session); err != nil {
		return err
	}
	if supersedesID != "" {
		if err := validateToken("superseded session ID", supersedesID); err != nil {
			return err
		}
		if supersedesID == session.ID {
			return fmt.Errorf("replacement session must have a new ID")
		}
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		var replacedIndex = -1
		var latestSupersededAt time.Time
		for i, current := range data.Sessions {
			if !s.matchesNamespace(current.Repository) || !sameSessionKey(current, session) {
				continue
			}
			if current.ID == session.ID {
				if current.IsCurrent() {
					return nil
				}
				return ErrSessionSuperseded
			}
			if current.IsCurrent() {
				return ErrSessionConflict
			}
			if current.ID == supersedesID {
				replacedIndex = i
			}
			if supersedesID == "" && !current.IsCurrent() && current.ReplacedBy == "" && current.SupersededAt != nil && (replacedIndex < 0 || current.SupersededAt.After(latestSupersededAt)) {
				replacedIndex = i
				latestSupersededAt = *current.SupersededAt
			}
		}
		if supersedesID != "" && replacedIndex < 0 {
			return fmt.Errorf("superseded session %q not found", supersedesID)
		}
		count := 0
		for _, current := range data.Sessions {
			if s.matchesNamespace(current.Repository) && sameSessionKey(current, session) {
				count++
			}
		}
		if count >= maxSessionHistoryPerKey {
			return fmt.Errorf("session history limit reached for ownership key")
		}
		data.Sessions = append(data.Sessions, session)
		if replacedIndex >= 0 {
			data.Sessions[replacedIndex].ReplacedBy = session.ID
		}
		return s.saveUnlocked(data)
	})
}

// LinkLatestSessionReplacement links a newly created but intentionally
// unretained session to the newest unlinked tombstone for its ownership key.
// No-op is successful when that key has no prior superseded session.
func (s *Store) LinkLatestSessionReplacement(ctx context.Context, key Session, replacementID string) error {
	if err := s.setNamespace(&key.Repository); err != nil {
		return err
	}
	if err := validateSession(key); err != nil {
		return err
	}
	if err := validateToken("replacement session ID", replacementID); err != nil {
		return err
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		index := -1
		for i := range data.Sessions {
			session := data.Sessions[i]
			if !s.matchesNamespace(session.Repository) || !sameSessionKey(session, key) || session.IsCurrent() || session.ReplacedBy != "" || session.ID == replacementID {
				continue
			}
			if session.SupersededAt != nil && (index < 0 || data.Sessions[index].SupersededAt == nil || session.SupersededAt.After(*data.Sessions[index].SupersededAt)) {
				index = i
			}
		}
		if index < 0 {
			return nil
		}
		data.Sessions[index].ReplacedBy = replacementID
		return s.saveUnlocked(data)
	})
}

// LinkSessionReplacement records a replacement ID for a superseded session
// when the replacement is intentionally not retained as current.
func (s *Store) LinkSessionReplacement(ctx context.Context, expected Session, replacementID string) error {
	if err := s.setNamespace(&expected.Repository); err != nil {
		return err
	}
	if err := validateSession(expected); err != nil {
		return err
	}
	if err := validateToken("replacement session ID", replacementID); err != nil {
		return err
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i := range data.Sessions {
			current := &data.Sessions[i]
			if s.matchesNamespace(current.Repository) && sameSessionKey(*current, expected) && current.ID == expected.ID && !current.IsCurrent() {
				if current.ReplacedBy == replacementID {
					return nil
				}
				current.ReplacedBy = replacementID
				return s.saveUnlocked(data)
			}
		}
		return fmt.Errorf("superseded session %q not found", expected.ID)
	})
}

// SetSessionContextTelemetry updates the latest authoritative context
// observation for an exact current managed session. Unknown data clears the
// current observation so a later reuse decision cannot rely on stale values.
func (s *Store) SetSessionContextTelemetry(ctx context.Context, expected Session, telemetry contextheadroom.Telemetry) (bool, error) {
	if err := s.setNamespace(&expected.Repository); err != nil {
		return false, err
	}
	if err := validateSession(expected); err != nil {
		return false, err
	}
	if !telemetry.Valid() {
		return false, fmt.Errorf("invalid managed session context telemetry")
	}
	changed := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i := range data.Sessions {
			current := &data.Sessions[i]
			if !s.matchesNamespace(current.Repository) || !sameSessionKey(*current, expected) || current.ID != expected.ID || !current.IsCurrent() {
				continue
			}
			if current.ContextTelemetry == telemetry {
				return nil
			}
			current.ContextTelemetry = telemetry
			changed = true
			return s.saveUnlocked(data)
		}
		return nil
	})
	return changed, err
}

func (s *Store) RemoveTicket(ctx context.Context, ticket string) ([]Session, bool, error) {
	if !s.namespaced() {
		return nil, false, fmt.Errorf("repository namespace required for ticket cleanup")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return nil, false, err
	}
	var removed []Session
	changed := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		kept := data.Sessions[:0]
		for _, session := range data.Sessions {
			if s.matchesNamespace(session.Repository) && session.Ticket == ticket {
				removed = append(removed, session)
				continue
			}
			kept = append(kept, session)
		}
		data.Sessions = kept
		changed = len(removed) > 0

		for key := range data.Bounces {
			repository, ticketID, _ := splitBounceKey(key)
			if ticketID != ticket || normalizeRepositoryIdentity(repository) != s.repository {
				continue
			}
			delete(data.Bounces, key)
			changed = true
		}
		loopKey := s.ticketLoopKey(ticket)
		if _, exists := data.Loops[loopKey]; exists {
			delete(data.Loops, loopKey)
			changed = true
		}

		if !changed {
			return nil
		}
		return s.saveUnlocked(data)
	})
	return removed, changed, err
}

// BounceCount returns the review-return count for a ticket in this repository.
func (s *Store) BounceCount(ctx context.Context, ticket string) (int, error) {
	if !s.namespaced() {
		return 0, fmt.Errorf("repository namespace required for bounce lookup")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return 0, err
	}
	count := 0
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if loop, ok := data.Loops[s.ticketLoopKey(ticket)]; ok {
			count = loop.BounceCount
		}
		return nil
	})
	return count, err
}

// IncrementBounces atomically increments a ticket's review-return count.
func (s *Store) IncrementBounces(ctx context.Context, ticket string) (int, error) {
	if !s.namespaced() {
		return 0, fmt.Errorf("repository namespace required for bounce mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return 0, err
	}
	count := 0
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop := data.Loops[key]
		if loop.BounceCount == math.MaxInt {
			return fmt.Errorf("bounce count for %s would overflow", ticket)
		}
		if loop.Repository == "" {
			loop = TicketLoop{Repository: s.repository, Ticket: ticket, Phase: TicketLoopActive}
		}
		loop.BounceCount++
		if loop.EffectiveBounceLimit > 0 && loop.BounceCount >= loop.EffectiveBounceLimit {
			loop.Phase = TicketLoopContainmentPending
		}
		data.Loops[key] = loop
		count = loop.BounceCount
		return s.saveUnlocked(data)
	})
	return count, err
}

// TicketLoop returns the durable loop record for one ticket in this repository.
func (s *Store) TicketLoop(ctx context.Context, ticket string) (TicketLoop, bool, error) {
	if !s.namespaced() {
		return TicketLoop{}, false, fmt.Errorf("repository namespace required for ticket loop lookup")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, false, err
	}
	var result TicketLoop
	found := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		result, found = data.Loops[s.ticketLoopKey(ticket)]
		return nil
	})
	return result, found, err
}

// Participate records the strictest max_bounces value seen in this ticket
// generation. It never loosens a limit already established by another worker.
// The returned bool is true only when this call changes an active loop to
// containment_pending.
func (s *Store) Participate(ctx context.Context, ticket string, maxBounces int) (TicketLoop, bool, error) {
	if !s.namespaced() {
		return TicketLoop{}, false, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, false, err
	}
	if maxBounces < 1 {
		return TicketLoop{}, false, fmt.Errorf("ticket loop bounce limit must be positive")
	}
	var result TicketLoop
	tripped := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop := data.Loops[key]
		if loop.Repository == "" {
			loop = TicketLoop{Repository: s.repository, Ticket: ticket, Phase: TicketLoopActive}
		}
		if loop.Phase == TicketLoopHeld {
			result = loop
			return nil
		}
		if loop.EffectiveBounceLimit == 0 || maxBounces < loop.EffectiveBounceLimit {
			loop.EffectiveBounceLimit = maxBounces
		}
		if loop.Phase == TicketLoopActive && loop.BounceCount >= loop.EffectiveBounceLimit {
			loop.Phase = TicketLoopContainmentPending
			tripped = true
		}
		data.Loops[key] = loop
		result = loop
		return s.saveUnlocked(data)
	})
	return result, tripped, err
}

// RecordClaim stores positive Ticket claim evidence for the ticket and queue.
func (s *Store) RecordClaim(ctx context.Context, ticket, actor, queue string) (TicketLoop, error) {
	if !s.namespaced() {
		return TicketLoop{}, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, err
	}
	if err := validateSteerName("actor", actor); err != nil {
		return TicketLoop{}, err
	}
	if queue != "open" && queue != "review" {
		return TicketLoop{}, fmt.Errorf("ticket loop claim queue must be open or review")
	}
	var result TicketLoop
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop := data.Loops[key]
		if loop.Repository == "" {
			loop = TicketLoop{Repository: s.repository, Ticket: ticket, Phase: TicketLoopActive}
		}
		if loop.Phase == TicketLoopHeld || loop.Phase == TicketLoopContainmentPending {
			result = loop
			return nil
		}
		if loop.AttemptID == ^uint64(0) {
			return fmt.Errorf("ticket loop attempt ID exhausted for %s", ticket)
		}
		loop.AttemptID++
		loop.ClaimState, loop.ClaimActor = queue, actor
		loop.DispatchPending = false
		data.Loops[key] = loop
		result = loop
		return s.saveUnlocked(data)
	})
	return result, err
}

// RecordDispatch records an accepted work-bearing dynamic steer for one
// concrete ticket. It does not count as claim evidence or a stall by itself.
func (s *Store) RecordDispatch(ctx context.Context, ticket string) (TicketLoop, error) {
	if !s.namespaced() {
		return TicketLoop{}, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, err
	}
	var result TicketLoop
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok || loop.Phase != TicketLoopActive {
			return fmt.Errorf("active ticket loop for %s is required before steer dispatch", ticket)
		}
		loop.DispatchPending = true
		data.Loops[key] = loop
		result = loop
		return s.saveUnlocked(data)
	})
	return result, err
}

func (s *Store) ClearDispatch(ctx context.Context, ticket string) error {
	if !s.namespaced() {
		return fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok || !loop.DispatchPending {
			return nil
		}
		loop.DispatchPending = false
		data.Loops[key] = loop
		return s.saveUnlocked(data)
	})
}

// RecordSteerClaim accepts active-claim evidence only for a ticket Orc
// previously selected for a work-bearing steer notification.
func (s *Store) RecordSteerClaim(ctx context.Context, ticket, actor, queue string) (TicketLoop, bool, error) {
	if !s.namespaced() {
		return TicketLoop{}, false, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, false, err
	}
	if err := validateSteerName("actor", actor); err != nil {
		return TicketLoop{}, false, err
	}
	if queue != "open" && queue != "review" {
		return TicketLoop{}, false, fmt.Errorf("ticket loop claim queue must be open or review")
	}
	var result TicketLoop
	recorded := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok || !loop.DispatchPending || loop.Phase != TicketLoopActive {
			result = loop
			return nil
		}
		if loop.AttemptID == ^uint64(0) {
			return fmt.Errorf("ticket loop attempt ID exhausted for %s", ticket)
		}
		loop.AttemptID++
		loop.ClaimState, loop.ClaimActor, loop.DispatchPending = queue, actor, false
		data.Loops[key] = loop
		result, recorded = loop, true
		return s.saveUnlocked(data)
	})
	return result, recorded, err
}

// RecordStall increments the shared same-state stall count and trips the
// ticket circuit at the fixed two-stall threshold.
func (s *Store) RecordStall(ctx context.Context, ticket string, expectedAttempt uint64) (TicketLoop, bool, error) {
	if !s.namespaced() {
		return TicketLoop{}, false, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	var result TicketLoop
	consumed := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok {
			return fmt.Errorf("ticket loop for %s does not exist", ticket)
		}
		if loop.Phase != TicketLoopActive || loop.ClaimState == "" || loop.AttemptID != expectedAttempt {
			result = loop
			return nil
		}
		if loop.StallCount < math.MaxInt {
			loop.StallCount++
		}
		if loop.StallCount >= 2 {
			loop.Phase = TicketLoopContainmentPending
		}
		loop.ClaimState, loop.ClaimActor = "", ""
		loop.DispatchPending = false
		data.Loops[key] = loop
		result = loop
		consumed = true
		return s.saveUnlocked(data)
	})
	return result, consumed, err
}

// RecordBounce increments the generation's review-return count and clears the
// positively observed review claim in the same durable mutation.
func (s *Store) RecordBounce(ctx context.Context, ticket string, expectedAttempt uint64) (TicketLoop, bool, error) {
	if !s.namespaced() {
		return TicketLoop{}, false, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if err := validateToken("ticket", ticket); err != nil {
		return TicketLoop{}, false, err
	}
	var result TicketLoop
	consumed := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok {
			return fmt.Errorf("ticket loop for %s does not exist", ticket)
		}
		if loop.Phase != TicketLoopActive || loop.ClaimState == "" || loop.AttemptID != expectedAttempt {
			result = loop
			return nil
		}
		if loop.BounceCount < math.MaxInt {
			loop.BounceCount++
		}
		if loop.EffectiveBounceLimit > 0 && loop.BounceCount >= loop.EffectiveBounceLimit {
			loop.Phase = TicketLoopContainmentPending
		}
		loop.ClaimState, loop.ClaimActor = "", ""
		loop.DispatchPending = false
		data.Loops[key] = loop
		result = loop
		consumed = true
		return s.saveUnlocked(data)
	})
	return result, consumed, err
}

// ClearClaim removes an attempt observation after authoritative reconciliation
// determines that it did not end as a same-state ready stall.
func (s *Store) ClearClaim(ctx context.Context, ticket string, expectedAttempt uint64) (bool, error) {
	if !s.namespaced() {
		return false, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	cleared := false
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok || loop.ClaimState == "" || loop.AttemptID != expectedAttempt {
			return nil
		}
		loop.ClaimState, loop.ClaimActor = "", ""
		data.Loops[key] = loop
		cleared = true
		return s.saveUnlocked(data)
	})
	return cleared, err
}

// MarkContainmentPending durably trips the circuit before any Ticket mutation.
func (s *Store) MarkContainmentPending(ctx context.Context, ticket string) (TicketLoop, error) {
	if !s.namespaced() {
		return TicketLoop{}, fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	var result TicketLoop
	err := s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok {
			return fmt.Errorf("ticket loop for %s does not exist", ticket)
		}
		loop.Phase = TicketLoopContainmentPending
		data.Loops[key] = loop
		result = loop
		return s.saveUnlocked(data)
	})
	return result, err
}

// MarkHeld persists Ticket's actual from_state after a successful ordinary
// hold, or records an externally observed hold for an active loop.
func (s *Store) MarkHeld(ctx context.Context, ticket, fromState string) error {
	if !s.namespaced() {
		return fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	if fromState != "" && fromState != "open" && fromState != "review" {
		return fmt.Errorf("ticket hold from_state must be open or review")
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		loop, ok := data.Loops[key]
		if !ok || (loop.Phase != TicketLoopActive && loop.Phase != TicketLoopContainmentPending) {
			return fmt.Errorf("ticket loop for %s is not active or pending containment", ticket)
		}
		loop.Phase, loop.HeldFrom = TicketLoopHeld, fromState
		loop.ClaimState, loop.ClaimActor = "", ""
		loop.DispatchPending = false
		data.Loops[key] = loop
		return s.saveUnlocked(data)
	})
}

// ResetTicketLoop begins a new generation after an authoritative observation
// proves a previously held ticket has left Ticket's hold state.
func (s *Store) ResetTicketLoop(ctx context.Context, ticket string) error {
	if !s.namespaced() {
		return fmt.Errorf("repository namespace required for ticket loop mutation")
	}
	return s.withLock(ctx, func() error {
		data, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		key := s.ticketLoopKey(ticket)
		if _, ok := data.Loops[key]; !ok {
			return nil
		}
		delete(data.Loops, key)
		return s.saveUnlocked(data)
	})
}
