package state

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/contextheadroom"
)

const (
	// Version 1 is the first public state schema. Unreleased development
	// formats are deliberately rejected rather than migrated implicitly.
	currentVersion          = 2
	stateFileName           = "state.json"
	lockFileName            = "lock"
	maxStateBytes           = 1 << 20
	defaultTimeout          = 5 * time.Second
	maxSessionHistoryPerKey = 32
)

var (
	// ErrMalformed reports state that cannot be safely interpreted.
	ErrMalformed = errors.New("malformed orchestration state")
	// ErrTooLarge reports a mutation whose complete serialized state would
	// exceed the bounded state-file size.
	ErrTooLarge = errors.New("orchestration state exceeds size limit")
	// ErrLockTimeout reports that another process held the state lock for the
	// bounded wait or an immediate ownership attempt.
	ErrLockTimeout = errors.New("orchestration state lock timeout")
	// ErrSessionConflict reports that a different session is already current
	// for a managed ownership key.
	ErrSessionConflict = errors.New("managed session ownership conflict")
	// ErrSessionSuperseded reports that a retired session ID cannot be
	// registered as current again.
	ErrSessionSuperseded = errors.New("managed session was superseded")
)

// Session identifies one retained harness session. Its logical key is the
// tuple (ticket, role, harness, owner).
type Session struct {
	Repository       string                    `json:"repository"`
	Ticket           string                    `json:"ticket"`
	Role             string                    `json:"role"`
	Harness          string                    `json:"harness"`
	Owner            string                    `json:"owner,omitempty"`
	ID               string                    `json:"id"`
	SupersededAt     *time.Time                `json:"superseded_at,omitempty"`
	SupersededReason string                    `json:"superseded_reason,omitempty"`
	ReplacedBy       string                    `json:"replaced_by,omitempty"`
	ContextTelemetry contextheadroom.Telemetry `json:"context_telemetry"`
}

func (s Session) IsCurrent() bool { return s.SupersededAt == nil || s.SupersededAt.IsZero() }

// Snapshot is a complete in-memory copy of the persisted state.
type Snapshot struct {
	Version  int                   `json:"version"`
	Sessions []Session             `json:"sessions"`
	Loops    map[string]TicketLoop `json:"ticket_loops"`
	// Bounces is a derived compatibility view for existing diagnostics. It is
	// never persisted in schema v2; bounce history lives in Loops.
	Bounces map[string]int `json:"-"`
}

// TicketLoop is the durable, repository-scoped circuit state shared by every
// managed and dynamically steered participant handling one ticket.
type TicketLoop struct {
	Repository           string `json:"repository"`
	Ticket               string `json:"ticket"`
	StallCount           int    `json:"stall_count"`
	BounceCount          int    `json:"bounce_count"`
	EffectiveBounceLimit int    `json:"effective_bounce_limit,omitempty"`
	Phase                string `json:"phase"`
	HeldFrom             string `json:"held_from,omitempty"`
	ClaimState           string `json:"claim_state,omitempty"`
	ClaimActor           string `json:"claim_actor,omitempty"`
	AttemptID            uint64 `json:"attempt_id,omitempty"`
	DispatchPending      bool   `json:"dispatch_pending,omitempty"`
}

const (
	TicketLoopActive             = "active"
	TicketLoopContainmentPending = "containment_pending"
	TicketLoopHeld               = "held"
)

// Store coordinates access to one state directory. Store has no in-memory
// cache; every operation reloads state while holding the cross-process lock.
type Store struct {
	dir         string
	repository  string
	lockTimeout time.Duration
}

// NewAdministrative returns the intentionally unscoped administrative view of
// a state directory. It supports inspection only; mutations require a scoped
// store created with NewForRepository.
func NewAdministrative(dir string) *Store { return &Store{dir: dir, lockTimeout: defaultTimeout} }

// NewForRepository returns a state store scoped to one resolved Ticket
// repository identity. Runtime callers use Ticket's stable repository ID;
// canonical paths remain supported for path-scoped state created by older
// callers. The identity is never taken from an ambient Ticket environment.
func NewForRepository(dir, repository string) *Store {
	return &Store{dir: dir, repository: normalizeRepositoryIdentity(repository), lockTimeout: defaultTimeout}
}

func normalizeRepositoryIdentity(repository string) string {
	repository = strings.TrimSpace(repository)
	if repository == "" {
		return ""
	}
	if isStableTicketRepositoryID(repository) {
		return repository
	}
	abs, err := filepath.Abs(repository)
	if err == nil {
		repository = abs
	}
	return filepath.Clean(repository)
}

func isStableTicketRepositoryID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value[14] != '4' {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (i == 19 && r != '8' && r != '9' && r != 'a' && r != 'b') || (i != 19 && !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func (s *Store) namespaced() bool { return s.repository != "" }

func (s *Store) setNamespace(repository *string) error {
	if !s.namespaced() {
		return fmt.Errorf("repository namespace required for state mutation")
	}
	if *repository != "" && normalizeRepositoryIdentity(*repository) != s.repository {
		return fmt.Errorf("state repository namespace mismatch")
	}
	*repository = s.repository
	return nil
}

func (s *Store) matchesNamespace(repository string) bool {
	return !s.namespaced() || normalizeRepositoryIdentity(repository) == s.repository
}

func (s *Store) bounceKey(ticket string) string {
	return s.repository + "\x00" + ticket
}

func (s *Store) ticketLoopKey(ticket string) string { return ticketLoopKey(s.repository, ticket) }

func splitBounceKey(key string) (string, string, bool) {
	if index := strings.IndexByte(key, 0); index >= 0 {
		return key[:index], key[index+1:], true
	}
	return "", key, false
}

func ticketLoopKey(repository, ticket string) string { return repository + "\x00" + ticket }

// Read returns a complete current-format snapshot. A missing state file is an
// empty current state; malformed or unsupported state is an error.
func (s *Store) Read(ctx context.Context) (Snapshot, error) {
	var result Snapshot
	err := s.withLock(ctx, func() error {
		var err error
		result, err = s.loadUnlocked()
		return err
	})
	if err != nil {
		return Snapshot{}, err
	}
	result, err = s.scopeSnapshot(result)
	if err != nil {
		return Snapshot{}, err
	}
	return result, nil
}

// scopeSnapshot hides records belonging to another resolved repository. An
// unscoped Store is intentionally an administrative view of all namespaces.
func (s *Store) scopeSnapshot(snapshot Snapshot) (Snapshot, error) {
	if !s.namespaced() {
		return snapshot, nil
	}
	snapshot.Sessions = filterByRepository(snapshot.Sessions, s.repository, func(item Session) string { return item.Repository })
	loops := make(map[string]TicketLoop, len(snapshot.Loops))
	for key, loop := range snapshot.Loops {
		if normalizeRepositoryIdentity(loop.Repository) == s.repository {
			loops[key] = loop
		}
	}
	snapshot.Loops = loops

	bounces := make(map[string]int, len(snapshot.Bounces))
	for key, count := range snapshot.Bounces {
		repository, _, namespaced := splitBounceKey(key)
		if namespaced && normalizeRepositoryIdentity(repository) == s.repository {
			bounces[key] = count
		}
		// Snapshot validation rejects keys without a repository identity.
	}
	snapshot.Bounces = bounces
	for key, loop := range loops {
		if loop.BounceCount > 0 {
			snapshot.Bounces[key] = loop.BounceCount
		}
	}
	return snapshot, nil
}

func filterByRepository[T any](records []T, repository string, recordRepository func(T) string) []T {
	filtered := make([]T, 0, len(records))
	for _, record := range records {
		if normalizeRepositoryIdentity(recordRepository(record)) == repository {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

// GetSession returns the retained session for one logical key.
