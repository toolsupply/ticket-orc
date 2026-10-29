package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
)

const steerRuntimeFileName = "steer-runtime.json"
const steerRuntimeFileVersion = 5

// SteerDelivery records Orc's notification state for one registration
// incarnation and an optional outstanding signoff or owned hold target.
type SteerDelivery struct {
	RepositoryID   string `json:"repository_id"`
	Actor          string `json:"actor"`
	RegistrationID string `json:"registration_id"`
	IncarnationID  string `json:"incarnation_id"`
	SessionID      string `json:"session_id"`
	State          string `json:"state"`
	Code           string `json:"code,omitempty"`
	TargetTicket   string `json:"target_ticket,omitempty"`
	// TargetQueue records the task state at reservation, independent of later policy.
	TargetQueue string `json:"target_queue,omitempty"`
	// BootstrapPending persists the one-time introduction for this session.
	BootstrapPending bool      `json:"bootstrap_pending,omitempty"`
	RecoveryPending  bool      `json:"recovery_pending,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type SteerRuntimeSnapshot struct {
	Version    int             `json:"version"`
	Deliveries []SteerDelivery `json:"deliveries"`
	migrated   bool
}

// SteerRuntimeStore owns the daemon's separate dynamic-steer delivery file.
type SteerRuntimeStore struct{ dir string }

func NewSteerRuntimeStore(dir string) *SteerRuntimeStore { return &SteerRuntimeStore{dir: dir} }

func (s *SteerRuntimeStore) Snapshot(ctx context.Context) (SteerRuntimeSnapshot, error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerRuntimeSnapshot{}, errors.New("steer runtime directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return SteerRuntimeSnapshot{}, err
	}
	defer lock.Release()
	return s.loadUnlocked()
}

// Reconcile makes the persisted set match current registration incarnations.
func (s *SteerRuntimeStore) Reconcile(ctx context.Context, registrations []SteerRegistration) error {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return errors.New("steer runtime directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return err
	}
	defer lock.Release()
	current, err := s.loadUnlocked()
	if err != nil {
		return err
	}
	byKey := make(map[string]SteerDelivery, len(current.Deliveries))
	byOwner := make(map[string]SteerDelivery, len(current.Deliveries))
	for _, d := range current.Deliveries {
		byKey[steerDeliveryKey(d.RepositoryID, d.Actor, d.RegistrationID)] = d
		byOwner[steerOwnerKey(d.RepositoryID, d.Actor)] = d
	}
	next := SteerRuntimeSnapshot{Version: steerRuntimeFileVersion, Deliveries: make([]SteerDelivery, 0, len(registrations))}
	for _, reg := range registrations {
		if err := validateSteerRegistration(reg); err != nil {
			return fmt.Errorf("invalid steer registration: %w", err)
		}
		key := steerDeliveryKey(reg.RepositoryID, reg.Actor, reg.RegistrationID)
		d, ok := byKey[key]
		if ok && d.IncarnationID != reg.IncarnationID {
			recoveryPending := d.RecoveryPending || d.SessionID != reg.SessionID
			bootstrapPending := d.BootstrapPending || d.SessionID != reg.SessionID
			d.IncarnationID = reg.IncarnationID
			d.SessionID = reg.SessionID
			// Hold ownership may predate an accepted or uncertain delivery.
			// Replacement never grants permission to execute it again.
			if d.TargetQueue != "hold" {
				d.State = "none"
				d.Code = ""
				d.TargetTicket = ""
				d.TargetQueue = ""
			}
			d.BootstrapPending = bootstrapPending
			d.RecoveryPending = recoveryPending
			d.UpdatedAt = time.Now().UTC()
		} else if !ok {
			previous, hadPrevious := byOwner[steerOwnerKey(reg.RepositoryID, reg.Actor)]
			recoveryPending := hadPrevious && (previous.RecoveryPending || previous.SessionID != reg.SessionID)
			bootstrapPending := !hadPrevious || previous.BootstrapPending || previous.SessionID != reg.SessionID
			d = SteerDelivery{
				RepositoryID: reg.RepositoryID, Actor: reg.Actor, RegistrationID: reg.RegistrationID,
				IncarnationID: reg.IncarnationID, SessionID: reg.SessionID, State: "none",
				BootstrapPending: bootstrapPending,
				RecoveryPending:  recoveryPending,
				UpdatedAt:        time.Now().UTC(),
			}
			if hadPrevious && previous.TargetQueue == "hold" && previous.TargetTicket != "" {
				d.TargetTicket, d.TargetQueue = previous.TargetTicket, previous.TargetQueue
				d.State, d.Code = previous.State, previous.Code
			}
		}
		next.Deliveries = append(next.Deliveries, d)
	}
	sort.Slice(next.Deliveries, func(i, j int) bool {
		if next.Deliveries[i].RepositoryID != next.Deliveries[j].RepositoryID {
			return next.Deliveries[i].RepositoryID < next.Deliveries[j].RepositoryID
		}
		return next.Deliveries[i].Actor < next.Deliveries[j].Actor
	})
	if len(next.Deliveries) == len(current.Deliveries) {
		sort.Slice(current.Deliveries, func(i, j int) bool {
			if current.Deliveries[i].RepositoryID != current.Deliveries[j].RepositoryID {
				return current.Deliveries[i].RepositoryID < current.Deliveries[j].RepositoryID
			}
			return current.Deliveries[i].Actor < current.Deliveries[j].Actor
		})
		same := true
		for i := range next.Deliveries {
			if next.Deliveries[i] != current.Deliveries[i] {
				same = false
				break
			}
		}
		if same && !current.migrated {
			return nil
		}
	}
	return s.saveUnlocked(next)
}

// Update changes state only if the exact registration incarnation is current.
func (s *SteerRuntimeStore) Update(ctx context.Context, registration SteerRegistration, state, code string) (bool, error) {
	return s.update(ctx, registration, state, code, false, false, false)
}

// CompleteDelivery records an accepted message and clears only the
// bootstrap/recovery intents included in that message.
func (s *SteerRuntimeStore) CompleteDelivery(ctx context.Context, registration SteerRegistration, nextState string, bootstrap, recovery bool) (bool, error) {
	return s.update(ctx, registration, nextState, "", bootstrap, recovery, false)
}

// ConfirmNoActiveClaim clears stale recovery intent after the authoritative
// Ticket lookup finds no claim for this actor.
func (s *SteerRuntimeStore) ConfirmNoActiveClaim(ctx context.Context, registration SteerRegistration) (bool, error) {
	return s.update(ctx, registration, "", "", false, false, true)
}

// ReserveSignoffTarget persists an exact signoff or hold intent
// before transport is called. A generic outstanding wake also blocks reservation.
// It returns true only when this call newly reserves the target for the exact
// current registration incarnation; an existing target suppresses another
// send, even when it is the same ticket.
func (s *SteerRuntimeStore) ReserveSignoffTarget(ctx context.Context, registration SteerRegistration, ticketID, targetQueue string) (bool, error) {
	if err := validateSignoffTarget(registration, ticketID); err != nil {
		return false, err
	}
	if targetQueue != "signoff" && targetQueue != "hold" {
		return false, fmt.Errorf("exact target_queue must be signoff or hold, got %q", targetQueue)
	}
	return s.updateSignoffTarget(ctx, registration, ticketID, targetQueue, "reserve", false, false)
}

// CompleteSignoffDelivery records accepted transport delivery while retaining
// the target so restart and reconciliation cannot resend the same ticket.
func (s *SteerRuntimeStore) CompleteSignoffDelivery(ctx context.Context, registration SteerRegistration, ticketID string) (bool, error) {
	return s.CompleteSignoffDeliveryWithIntents(ctx, registration, ticketID, false, false)
}

// CompleteSignoffDeliveryWithIntents records accepted delivery and retires
// only the bootstrap/recovery notices included in that exact message.
func (s *SteerRuntimeStore) CompleteSignoffDeliveryWithIntents(ctx context.Context, registration SteerRegistration, ticketID string, bootstrap, recovery bool) (bool, error) {
	if err := validateSignoffTarget(registration, ticketID); err != nil {
		return false, err
	}
	return s.updateSignoffTarget(ctx, registration, ticketID, "", "complete", bootstrap, recovery)
}

// RejectSignoffDelivery retains an owned hold target or retires a signoff and
// records the existing one-attempt degraded state, preventing a later pass
// from reserving and resending it after a partial two-write transition.
func (s *SteerRuntimeStore) RejectSignoffDelivery(ctx context.Context, registration SteerRegistration, ticketID string) (bool, error) {
	if err := validateSignoffTarget(registration, ticketID); err != nil {
		return false, err
	}
	return s.updateSignoffTarget(ctx, registration, ticketID, "", "reject", false, false)
}

// ClearSignoffTarget retires a resolved target for the exact registration
// incarnation. It leaves bootstrap and recovery intents unchanged.
func (s *SteerRuntimeStore) ClearSignoffTarget(ctx context.Context, registration SteerRegistration, ticketID string) (bool, error) {
	if err := validateSignoffTarget(registration, ticketID); err != nil {
		return false, err
	}
	return s.updateSignoffTarget(ctx, registration, ticketID, "", "clear", false, false)
}

func validateSignoffTarget(registration SteerRegistration, ticketID string) error {
	if err := validateSteerRegistration(registration); err != nil {
		return fmt.Errorf("invalid signoff target registration: %w", err)
	}
	if !validSignoffTargetTicketID(ticketID) {
		return errors.New("signoff target must be a full YYYYMMDD-NNNNN ticket ID")
	}
	return nil
}

func validSignoffTargetTicketID(ticketID string) bool {
	if len(ticketID) != 14 || ticketID[8] != '-' {
		return false
	}
	if _, err := time.Parse("20060102", ticketID[:8]); err != nil {
		return false
	}
	for _, digit := range ticketID[9:] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func (s *SteerRuntimeStore) updateSignoffTarget(ctx context.Context, registration SteerRegistration, ticketID, targetQueue, operation string, bootstrap, recovery bool) (bool, error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return false, errors.New("steer runtime directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return false, err
	}
	defer lock.Release()
	regs, err := NewRegistrationStore(s.dir).loadUnlocked()
	if err != nil {
		return false, err
	}
	current := false
	for _, reg := range regs.Registrations {
		if reg.RepositoryID == registration.RepositoryID && reg.Actor == registration.Actor &&
			reg.RegistrationID == registration.RegistrationID && reg.IncarnationID == registration.IncarnationID &&
			reg.SessionID == registration.SessionID {
			current = true
			break
		}
	}
	if !current {
		return false, nil
	}
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return false, err
	}
	for index := range snapshot.Deliveries {
		delivery := &snapshot.Deliveries[index]
		if delivery.RepositoryID != registration.RepositoryID || delivery.Actor != registration.Actor ||
			delivery.RegistrationID != registration.RegistrationID {
			continue
		}
		if delivery.IncarnationID != registration.IncarnationID || delivery.SessionID != registration.SessionID {
			return false, nil
		}
		switch operation {
		case "reserve":
			if delivery.TargetTicket != "" || delivery.State != "none" {
				return false, nil
			}
			delivery.TargetTicket = ticketID
			delivery.TargetQueue = targetQueue
			delivery.State = "sending"
			delivery.Code = ""
		case "complete":
			if delivery.TargetTicket != ticketID {
				return false, nil
			}
			if delivery.State == "queued" {
				return true, nil
			}
			if delivery.State != "sending" {
				return false, nil
			}
			delivery.State = "queued"
			delivery.Code = ""
			delivery.BootstrapPending = delivery.BootstrapPending && !bootstrap
			delivery.RecoveryPending = delivery.RecoveryPending && !recovery
		case "clear":
			if delivery.TargetTicket != ticketID {
				return false, nil
			}
			delivery.TargetTicket = ""
			delivery.TargetQueue = ""
			delivery.State = "none"
			delivery.Code = ""
		case "reject":
			if delivery.TargetTicket != ticketID || delivery.State != "sending" {
				return false, nil
			}
			// A rejected hold send still leaves Ticket assigned. Preserve its
			// exact target evidence for operator reconciliation, never resend.
			if delivery.TargetQueue != "hold" {
				delivery.TargetTicket = ""
				delivery.TargetQueue = ""
			}
			delivery.State = "degraded"
			delivery.Code = "queue_rejected"
		default:
			return false, errors.New("unsupported signoff target operation")
		}
		delivery.UpdatedAt = time.Now().UTC()
		if err := s.saveUnlocked(snapshot); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (s *SteerRuntimeStore) update(ctx context.Context, registration SteerRegistration, state, code string, bootstrap, recovery, noActiveClaim bool) (bool, error) {
	if state != "" && state != "none" && state != "sending" && state != "queued" && state != "consumed" && state != "degraded" {
		return false, fmt.Errorf("unsupported steer delivery state %q", state)
	}
	if state == "" && !noActiveClaim {
		return false, errors.New("steer delivery update requires a state")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return false, err
	}
	defer lock.Release()
	regs, err := NewRegistrationStore(s.dir).loadUnlocked()
	if err != nil {
		return false, err
	}
	current := false
	for _, reg := range regs.Registrations {
		if reg.RepositoryID == registration.RepositoryID && reg.Actor == registration.Actor && reg.RegistrationID == registration.RegistrationID && reg.IncarnationID == registration.IncarnationID {
			current = true
			break
		}
	}
	if !current {
		return false, nil
	}
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return false, err
	}
	found := false
	for i := range snapshot.Deliveries {
		d := &snapshot.Deliveries[i]
		if d.RepositoryID == registration.RepositoryID && d.Actor == registration.Actor && d.RegistrationID == registration.RegistrationID {
			if noActiveClaim && state == "" && !d.RecoveryPending {
				return true, nil
			}
			if state != "" {
				d.State = state
				d.Code = code
				d.IncarnationID = registration.IncarnationID
				d.SessionID = registration.SessionID
			}
			d.BootstrapPending = d.BootstrapPending && !bootstrap
			d.RecoveryPending = d.RecoveryPending && !recovery
			if noActiveClaim {
				d.RecoveryPending = false
			}
			d.UpdatedAt = time.Now().UTC()
			found = true
			break
		}
	}
	if !found {
		if noActiveClaim {
			return true, nil
		}
		snapshot.Deliveries = append(snapshot.Deliveries, SteerDelivery{RepositoryID: registration.RepositoryID, Actor: registration.Actor, RegistrationID: registration.RegistrationID, IncarnationID: registration.IncarnationID, SessionID: registration.SessionID, State: state, Code: code, UpdatedAt: time.Now().UTC()})
	}
	return true, s.saveUnlocked(snapshot)
}

func (s *SteerRuntimeStore) loadUnlocked() (SteerRuntimeSnapshot, error) {
	data, err := readBoundedStateFile(filepath.Join(s.dir, steerRuntimeFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return SteerRuntimeSnapshot{Version: steerRuntimeFileVersion, Deliveries: []SteerDelivery{}}, nil
		}
		return SteerRuntimeSnapshot{}, fmt.Errorf("read steer runtime: %w", err)
	}
	if err := jsonx.Validate(data); err != nil {
		return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	switch version.Version {
	case steerRuntimeFileVersion, 4:
		var snapshot SteerRuntimeSnapshot
		if err := decodeStrictStateJSON(data, &snapshot); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		// Inspect field presence as well as values: target_queue is absent in
		// published v4 and on v5 ordinary notifications, even if empty/null.
		var fields struct {
			Deliveries []map[string]json.RawMessage `json:"deliveries"`
		}
		if err := json.Unmarshal(data, &fields); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		for index, delivery := range snapshot.Deliveries {
			_, hasQueue := fields.Deliveries[index]["target_queue"]
			if version.Version == 4 && hasQueue {
				return SteerRuntimeSnapshot{}, fmt.Errorf("%w: steer runtime v4 delivery %d must not contain target_queue", ErrMalformed, index)
			}
			if version.Version == steerRuntimeFileVersion && delivery.TargetTicket == "" && hasQueue {
				return SteerRuntimeSnapshot{}, fmt.Errorf("%w: delivery %d target_queue must be absent without target_ticket", ErrMalformed, index)
			}
		}
		if version.Version == 4 {
			// Published v4 dispatched only signoff targets. An operator must
			// reconcile any ambiguous records from the unpublished c56f0f8
			// candidate before upgrade; current Ticket state cannot infer origin.
			snapshot.Version = steerRuntimeFileVersion
			snapshot.migrated = true
			for index := range snapshot.Deliveries {
				if snapshot.Deliveries[index].TargetTicket != "" {
					snapshot.Deliveries[index].TargetQueue = "signoff"
				}
			}
		}
		if err := validateSteerRuntimeSnapshot(snapshot); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		return snapshot, nil
	case 3:
		var previous steerRuntimeSnapshotV3
		if err := decodeStrictStateJSON(data, &previous); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if err := validateSteerRuntimeSnapshotV3(previous); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		snapshot := migrateSteerRuntimeSnapshotV3(previous)
		if err := validateSteerRuntimeSnapshot(snapshot); err != nil {
			return SteerRuntimeSnapshot{}, fmt.Errorf("%w: migrated steer runtime: %v", ErrMalformed, err)
		}
		return snapshot, nil
	default:
		return SteerRuntimeSnapshot{}, fmt.Errorf("%w: unsupported steer runtime version %d (want %d)", ErrMalformed, version.Version, steerRuntimeFileVersion)
	}
}

type steerRuntimeSnapshotV3 struct {
	Version    int               `json:"version"`
	Deliveries []steerDeliveryV3 `json:"deliveries"`
}

type steerDeliveryV3 struct {
	RepositoryID     string    `json:"repository_id"`
	Actor            string    `json:"actor"`
	RegistrationID   string    `json:"registration_id"`
	JoinSignal       string    `json:"join_signal"`
	ThreadID         string    `json:"thread_id"`
	State            string    `json:"state"`
	Code             string    `json:"code,omitempty"`
	BootstrapPending bool      `json:"bootstrap_pending,omitempty"`
	RecoveryPending  bool      `json:"recovery_pending,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func validateSteerRuntimeSnapshotV3(snapshot steerRuntimeSnapshotV3) error {
	if snapshot.Version != 3 {
		return fmt.Errorf("unsupported previous steer runtime version %d", snapshot.Version)
	}
	if snapshot.Deliveries == nil {
		return errors.New("steer runtime deliveries must be an array")
	}
	seen := make(map[string]bool, len(snapshot.Deliveries))
	for _, delivery := range snapshot.Deliveries {
		if !isStableTicketRepositoryID(delivery.RepositoryID) || validateSteerName("actor", delivery.Actor) != nil ||
			!validSteerRegistrationID(delivery.RegistrationID) || !validSteerRegistrationID(delivery.JoinSignal) ||
			validateToken("steer runtime thread_id", delivery.ThreadID) != nil || delivery.UpdatedAt.IsZero() ||
			!(delivery.State == "none" || delivery.State == "sending" || delivery.State == "queued" || delivery.State == "consumed" || delivery.State == "degraded") ||
			!validSteerRuntimeCode(delivery.Code) {
			return errors.New("invalid previous steer runtime delivery")
		}
		key := steerDeliveryKey(delivery.RepositoryID, delivery.Actor, delivery.RegistrationID)
		if seen[key] {
			return errors.New("duplicate previous steer runtime delivery")
		}
		seen[key] = true
	}
	return nil
}

func migrateSteerRuntimeSnapshotV3(previous steerRuntimeSnapshotV3) SteerRuntimeSnapshot {
	snapshot := SteerRuntimeSnapshot{Version: steerRuntimeFileVersion, Deliveries: make([]SteerDelivery, 0, len(previous.Deliveries)), migrated: true}
	for _, old := range previous.Deliveries {
		snapshot.Deliveries = append(snapshot.Deliveries, SteerDelivery{
			RepositoryID: old.RepositoryID, Actor: old.Actor, RegistrationID: old.RegistrationID,
			IncarnationID: old.JoinSignal, SessionID: old.ThreadID, State: old.State, Code: old.Code,
			BootstrapPending: old.BootstrapPending, RecoveryPending: old.RecoveryPending, UpdatedAt: old.UpdatedAt,
		})
	}
	return snapshot
}

func (s *SteerRuntimeStore) saveUnlocked(snap SteerRuntimeSnapshot) error {
	if err := validateSteerRuntimeSnapshot(snap); err != nil {
		return fmt.Errorf("invalid steer runtime: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode steer runtime: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxSteerFileSize {
		return fmt.Errorf("%w: steer runtime file exceeds size limit", ErrTooLarge)
	}
	if err := WriteAtomicMode(filepath.Join(s.dir, steerRuntimeFileName), data, 0o600); err != nil {
		return fmt.Errorf("write steer runtime: %w", err)
	}
	return nil
}

func validateSteerRuntimeSnapshot(snap SteerRuntimeSnapshot) error {
	if snap.Version != steerRuntimeFileVersion {
		return fmt.Errorf("unsupported steer runtime version %d (want %d)", snap.Version, steerRuntimeFileVersion)
	}
	if snap.Deliveries == nil {
		return errors.New("steer runtime deliveries must be an array")
	}
	seen := map[string]bool{}
	for _, d := range snap.Deliveries {
		if !isStableTicketRepositoryID(d.RepositoryID) || validateSteerName("actor", d.Actor) != nil || !validSteerRegistrationID(d.RegistrationID) || !validSteerRegistrationID(d.IncarnationID) || validateSteerString("runtime session_id", d.SessionID, maxSteerSessionIDBytes) != nil || d.UpdatedAt.IsZero() || !(d.State == "none" || d.State == "sending" || d.State == "queued" || d.State == "consumed" || d.State == "degraded") || !validSteerRuntimeCode(d.Code) || (d.TargetTicket != "" && !validSignoffTargetTicketID(d.TargetTicket)) {
			return errors.New("invalid steer runtime delivery")
		}
		if d.TargetTicket == "" && d.TargetQueue != "" {
			return errors.New("target_queue must be absent without target_ticket")
		}
		if d.TargetTicket != "" && d.TargetQueue != "signoff" && d.TargetQueue != "hold" {
			return errors.New("exact target_queue must be signoff or hold")
		}
		key := steerDeliveryKey(d.RepositoryID, d.Actor, d.RegistrationID)
		if seen[key] {
			return errors.New("duplicate steer runtime delivery")
		}
		seen[key] = true
	}
	return nil
}

func validSteerRuntimeCode(code string) bool {
	switch code {
	case "", "ready", "busy", "idle", "consumed", "awaiting_claim", "queue_uncertain", "queue_rejected":
		return true
	default:
		return false
	}
}

func steerDeliveryKey(repositoryID, actor, registrationID string) string {
	return fmt.Sprintf("%s\x00%s\x00%s", repositoryID, actor, registrationID)
}

func steerOwnerKey(repositoryID, actor string) string { return repositoryID + "\x00" + actor }
