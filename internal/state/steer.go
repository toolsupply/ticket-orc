package state

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
)

const (
	steerFileName                = "steer.json"
	steerRegistrationFileVersion = 5
	maxSteerFileSize             = 1 << 20
	maxSteerTransportParams      = 8
	maxSteerTransportKeyBytes    = 64
	maxSteerTransportValueBytes  = 4096
	maxSteerTransportTotalBytes  = 8192
	maxSteerSessionIDBytes       = 4096
)

// SteerRegistration describes one externally-owned harness session's current
// Ticket routing. The logical key is repository ID plus Ticket actor.
type SteerRegistration struct {
	RepositoryID   string              `json:"repository_id"`
	RegistrationID string              `json:"registration_id"`
	IncarnationID  string              `json:"incarnation_id"`
	RepositoryPath string              `json:"repository_path"`
	RepositoryName string              `json:"repository_name,omitempty"`
	Actor          string              `json:"actor"`
	Role           string              `json:"role"`
	Harness        string              `json:"harness"`
	SessionID      string              `json:"session_id"`
	Transport      SteerTransportRoute `json:"transport"`
}

// SteerTransportRoute identifies the message transport and its bounded,
// transport-specific configuration.
type SteerTransportRoute struct {
	Kind   string            `json:"kind"`
	Params map[string]string `json:"params,omitempty"`
}

type SteerSnapshot struct {
	Version       int                 `json:"version"`
	Registrations []SteerRegistration `json:"registrations"`
}

// RegistrationStore persists local join/leave state separately from managed
// orchestration state while sharing its cross-process lock and atomic writer.
type RegistrationStore struct {
	dir string
}

func NewRegistrationStore(dir string) *RegistrationStore {
	return &RegistrationStore{dir: dir}
}

// Join upserts the (repository ID, actor) registration. changed is true for a
// new or routing-changed registration; previous is non-nil when replacing one.
func (s *RegistrationStore) Join(ctx context.Context, next SteerRegistration) (current SteerRegistration, previous *SteerRegistration, changed bool, err error) {
	return s.JoinWithPreparation(ctx, next, nil)
}

// JoinWithPreparation prepares the candidate incarnation while holding the
// registration lock and before replacing durable ownership. The callback is
// intended for local transport setup; it must not perform network or harness
// work. A preparation error leaves the previous registration unchanged.
func (s *RegistrationStore) JoinWithPreparation(ctx context.Context, next SteerRegistration, prepare func(SteerRegistration) error) (current SteerRegistration, previous *SteerRegistration, changed bool, err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerRegistration{}, nil, false, errors.New("steer state directory must not be empty")
	}
	if err := validateSteerRegistrationRoute(next); err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("invalid steer registration: %w", err)
	}
	next.IncarnationID, err = newSteerRegistrationID()
	if err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("generate steer incarnation ID: %w", err)
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return SteerRegistration{}, nil, false, err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return SteerRegistration{}, nil, false, err
	}
	for index, old := range snapshot.Registrations {
		if old.RepositoryID != next.RepositoryID || old.Actor != next.Actor {
			continue
		}
		previousCopy := old
		previous = &previousCopy
		if sameSteerRoute(old, next) {
			next.RegistrationID = old.RegistrationID
			if err := validateSteerRegistration(next); err != nil {
				return SteerRegistration{}, nil, false, fmt.Errorf("invalid steer registration: %w", err)
			}
			if prepare != nil {
				if err := prepare(next); err != nil {
					return SteerRegistration{}, nil, false, fmt.Errorf("prepare steer registration: %w", err)
				}
			}
			if old.RepositoryName == next.RepositoryName {
				snapshot.Registrations[index] = next
				if err := s.saveUnlocked(snapshot); err != nil {
					return SteerRegistration{}, nil, false, err
				}
				return next, previous, false, nil
			}
			snapshot.Registrations[index] = next
			if err := s.saveUnlocked(snapshot); err != nil {
				return SteerRegistration{}, nil, false, err
			}
			return next, previous, false, nil
		}
		next.RegistrationID, err = newSteerRegistrationID()
		if err != nil {
			return SteerRegistration{}, nil, false, fmt.Errorf("generate steer registration ID: %w", err)
		}
		if err := validateSteerRegistration(next); err != nil {
			return SteerRegistration{}, nil, false, fmt.Errorf("invalid steer registration: %w", err)
		}
		if prepare != nil {
			if err := prepare(next); err != nil {
				return SteerRegistration{}, nil, false, fmt.Errorf("prepare steer registration: %w", err)
			}
		}
		snapshot.Registrations[index] = next
		if err := s.saveUnlocked(snapshot); err != nil {
			return SteerRegistration{}, nil, false, err
		}
		return next, previous, true, nil
	}
	next.RegistrationID, err = newSteerRegistrationID()
	if err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("generate steer registration ID: %w", err)
	}
	if err := validateSteerRegistration(next); err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("invalid steer registration: %w", err)
	}
	if prepare != nil {
		if err := prepare(next); err != nil {
			return SteerRegistration{}, nil, false, fmt.Errorf("prepare steer registration: %w", err)
		}
	}
	snapshot.Registrations = append(snapshot.Registrations, next)
	sortSteerRegistrations(snapshot.Registrations)
	if err := s.saveUnlocked(snapshot); err != nil {
		return SteerRegistration{}, nil, false, err
	}
	return next, nil, true, nil
}

// Leave removes a registration only when the supplied route still owns its
// repository/actor key. A missing or replaced route is a harmless no-op.
func (s *RegistrationStore) Leave(ctx context.Context, expected SteerRegistration) (removed bool, err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return false, errors.New("steer state directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return false, err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return false, err
	}
	for index, registration := range snapshot.Registrations {
		if registration.RepositoryID != expected.RepositoryID || registration.Actor != expected.Actor ||
			registration.RegistrationID != expected.RegistrationID || registration.IncarnationID != expected.IncarnationID ||
			!sameSteerRoute(registration, expected) {
			continue
		}
		snapshot.Registrations = append(snapshot.Registrations[:index], snapshot.Registrations[index+1:]...)
		if err := s.saveUnlocked(snapshot); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (s *RegistrationStore) Find(ctx context.Context, repositoryID, actor string) (registration SteerRegistration, found bool, err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerRegistration{}, false, errors.New("steer state directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return SteerRegistration{}, false, err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return SteerRegistration{}, false, err
	}
	for _, registration := range snapshot.Registrations {
		if registration.RepositoryID == repositoryID && registration.Actor == actor {
			return registration, true, nil
		}
	}
	return SteerRegistration{}, false, nil
}

func (s *RegistrationStore) Snapshot(ctx context.Context) (snapshot SteerSnapshot, err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerSnapshot{}, errors.New("steer state directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return SteerSnapshot{}, err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	return s.loadUnlocked()
}

// WithSnapshot reads registrations and calls use while holding the
// cross-process registration lock. The callback must do local filesystem work
// only and must not call RegistrationStore methods. This lets sidecar cleanup
// compare durable ownership and filesystem entries without racing Join's
// prepare-before-replace transaction.
func (s *RegistrationStore) WithSnapshot(ctx context.Context, use func(SteerSnapshot) error) (err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return errors.New("steer state directory must not be empty")
	}
	if use == nil {
		return errors.New("steer snapshot callback must not be nil")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	snapshot, err := s.loadUnlocked()
	if err != nil {
		return err
	}
	return use(snapshot)
}

// SnapshotWithPresence reports whether a valid registration file was present
// for this read. Callers observing lifecycle changes must distinguish a missing
// file from a valid empty snapshot.
func (s *RegistrationStore) SnapshotWithPresence(ctx context.Context) (snapshot SteerSnapshot, present bool, err error) {
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerSnapshot{}, false, errors.New("steer state directory must not be empty")
	}
	lock, err := AcquireLock(ctx, s.dir, defaultTimeout)
	if err != nil {
		return SteerSnapshot{}, false, err
	}
	defer func() {
		if releaseErr := lock.Release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	return s.loadObservedUnlocked()
}

func (s *RegistrationStore) loadUnlocked() (SteerSnapshot, error) {
	snapshot, _, err := s.loadObservedUnlocked()
	return snapshot, err
}

func (s *RegistrationStore) loadObservedUnlocked() (SteerSnapshot, bool, error) {
	path := filepath.Join(s.dir, steerFileName)
	data, err := readBoundedStateFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return SteerSnapshot{Version: steerRegistrationFileVersion, Registrations: []SteerRegistration{}}, false, nil
		}
		return SteerSnapshot{}, false, fmt.Errorf("read steer registrations: %w", err)
	}
	if err := jsonx.Validate(data); err != nil {
		return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	switch version.Version {
	case steerRegistrationFileVersion:
		var snapshot SteerSnapshot
		if err := decodeStrictStateJSON(data, &snapshot); err != nil {
			return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if err := validateSteerSnapshot(snapshot); err != nil {
			return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		return snapshot, true, nil
	case steerRegistrationFileVersion - 1:
		var previous steerSnapshotV4
		if err := decodeStrictStateJSON(data, &previous); err != nil {
			return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if err := validateSteerSnapshotV4(previous); err != nil {
			return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		snapshot := migrateSteerSnapshotV4(previous)
		if err := validateSteerSnapshot(snapshot); err != nil {
			return SteerSnapshot{}, true, fmt.Errorf("%w: migrated steer registrations: %v", ErrMalformed, err)
		}
		return snapshot, true, nil
	default:
		return SteerSnapshot{}, true, fmt.Errorf("%w: unsupported steer registration version %d (want %d)", ErrMalformed, version.Version, steerRegistrationFileVersion)
	}
}

type steerSnapshotV4 struct {
	Version       int                   `json:"version"`
	Registrations []steerRegistrationV4 `json:"registrations"`
}

type steerRegistrationV4 struct {
	RepositoryID   string `json:"repository_id"`
	RegistrationID string `json:"registration_id"`
	JoinSignal     string `json:"join_signal"`
	RepositoryPath string `json:"repository_path"`
	RepositoryName string `json:"repository_name,omitempty"`
	Actor          string `json:"actor"`
	Role           string `json:"role"`
	CodexHome      string `json:"codex_home"`
	ThreadID       string `json:"thread_id"`
}

func decodeStrictStateJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func validateSteerSnapshotV4(snapshot steerSnapshotV4) error {
	if snapshot.Version != steerRegistrationFileVersion-1 {
		return fmt.Errorf("unsupported previous steer registration version %d", snapshot.Version)
	}
	if snapshot.Registrations == nil {
		return errors.New("steer registrations must be an array")
	}
	seen := make(map[string]struct{}, len(snapshot.Registrations))
	for _, registration := range snapshot.Registrations {
		if err := validateSteerRegistrationV4(registration); err != nil {
			return err
		}
		key := registration.RepositoryID + "\x00" + registration.Actor
		if _, ok := seen[key]; ok {
			return errors.New("duplicate steer registration owner")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateSteerRegistrationV4(registration steerRegistrationV4) error {
	if !isStableTicketRepositoryID(registration.RepositoryID) ||
		validateSteerName("actor", registration.Actor) != nil ||
		validateSteerName("role", registration.Role) != nil ||
		!validSteerRegistrationID(registration.RegistrationID) ||
		!validSteerRegistrationID(registration.JoinSignal) {
		return errors.New("invalid previous steer registration")
	}
	for _, field := range []struct{ name, value string }{
		{"repository_path", registration.RepositoryPath},
		{"codex_home", registration.CodexHome},
		{"thread_id", registration.ThreadID},
	} {
		if err := validateToken("steer "+field.name, field.value); err != nil || strings.IndexFunc(field.value, unicode.IsControl) >= 0 {
			return fmt.Errorf("invalid previous steer %s", field.name)
		}
	}
	if !filepath.IsAbs(registration.RepositoryPath) || filepath.Clean(registration.RepositoryPath) != registration.RepositoryPath {
		return errors.New("previous repository_path must be an absolute canonical path")
	}
	if !filepath.IsAbs(registration.CodexHome) || filepath.Clean(registration.CodexHome) != registration.CodexHome {
		return errors.New("previous codex_home must be an absolute canonical path")
	}
	if registration.RepositoryName != "" && strings.IndexFunc(registration.RepositoryName, unicode.IsControl) >= 0 {
		return errors.New("previous repository_name must not contain control characters")
	}
	return nil
}

func migrateSteerSnapshotV4(previous steerSnapshotV4) SteerSnapshot {
	snapshot := SteerSnapshot{Version: steerRegistrationFileVersion, Registrations: make([]SteerRegistration, 0, len(previous.Registrations))}
	for _, old := range previous.Registrations {
		snapshot.Registrations = append(snapshot.Registrations, SteerRegistration{
			RepositoryID: old.RepositoryID, RegistrationID: old.RegistrationID, IncarnationID: old.JoinSignal,
			RepositoryPath: old.RepositoryPath, RepositoryName: old.RepositoryName, Actor: old.Actor, Role: old.Role,
			Harness: "codex", SessionID: old.ThreadID,
			Transport: SteerTransportRoute{Kind: "codex-queue", Params: map[string]string{"home": old.CodexHome}},
		})
	}
	return snapshot
}

func (s *RegistrationStore) saveUnlocked(snapshot SteerSnapshot) error {
	if err := validateSteerSnapshot(snapshot); err != nil {
		return fmt.Errorf("invalid steer registration state: %w", err)
	}
	sortSteerRegistrations(snapshot.Registrations)
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode steer registrations: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxSteerFileSize {
		return fmt.Errorf("%w: steer registration file exceeds size limit", ErrTooLarge)
	}
	if err := WriteAtomicMode(filepath.Join(s.dir, steerFileName), data, 0o600); err != nil {
		return fmt.Errorf("write steer registrations: %w", err)
	}
	return nil
}

func validateSteerSnapshot(snapshot SteerSnapshot) error {
	if snapshot.Version != steerRegistrationFileVersion {
		return fmt.Errorf("unsupported steer registration version %d", snapshot.Version)
	}
	if snapshot.Registrations == nil {
		return errors.New("steer registrations must be an array")
	}
	seen := make(map[string]struct{}, len(snapshot.Registrations))
	for _, registration := range snapshot.Registrations {
		if err := validateSteerRegistration(registration); err != nil {
			return err
		}
		key := registration.RepositoryID + "\x00" + registration.Actor
		if _, ok := seen[key]; ok {
			return errors.New("duplicate steer registration owner")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateSteerRegistration(registration SteerRegistration) error {
	if err := validateSteerRegistrationRoute(registration); err != nil {
		return err
	}
	if !validSteerRegistrationID(registration.RegistrationID) {
		return errors.New("registration_id must be a 128-bit lowercase hexadecimal ID")
	}
	if !validSteerRegistrationID(registration.IncarnationID) {
		return errors.New("incarnation_id must be a 128-bit lowercase hexadecimal ID")
	}
	return nil
}

func validateSteerRegistrationRoute(registration SteerRegistration) error {
	if !isStableTicketRepositoryID(registration.RepositoryID) {
		return errors.New("repository_id must be a canonical Ticket repository UUID")
	}
	if err := validateSteerName("actor", registration.Actor); err != nil {
		return err
	}
	if err := validateSteerName("role", registration.Role); err != nil {
		return err
	}
	if err := validateSteerName("harness", registration.Harness); err != nil {
		return err
	}
	if err := validateSteerName("transport kind", registration.Transport.Kind); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"repository_path", registration.RepositoryPath},
		{"session_id", registration.SessionID},
	} {
		if err := validateSteerString(field.name, field.value, maxSteerSessionIDBytes); err != nil {
			return err
		}
	}
	if !filepath.IsAbs(registration.RepositoryPath) || filepath.Clean(registration.RepositoryPath) != registration.RepositoryPath {
		return errors.New("repository_path must be an absolute canonical path")
	}
	if registration.RepositoryName != "" && strings.IndexFunc(registration.RepositoryName, unicode.IsControl) >= 0 {
		return errors.New("repository_name must not contain control characters")
	}
	return validateSteerTransport(registration.Transport)
}

func validateSteerString(name, value string, maxBytes int) error {
	if err := validateToken("steer "+name, value); err != nil {
		return err
	}
	if len(value) > maxBytes {
		return fmt.Errorf("steer %s exceeds %d bytes", name, maxBytes)
	}
	if strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("steer %s must be trimmed and contain no control characters", name)
	}
	return nil
}

func validateSteerTransport(transport SteerTransportRoute) error {
	if len(transport.Params) > maxSteerTransportParams {
		return fmt.Errorf("steer transport has more than %d parameters", maxSteerTransportParams)
	}
	total := 0
	for key, value := range transport.Params {
		if err := validateSteerName("transport parameter", key); err != nil {
			return err
		}
		if len(key) > maxSteerTransportKeyBytes {
			return fmt.Errorf("steer transport parameter key exceeds %d bytes", maxSteerTransportKeyBytes)
		}
		if err := validateSteerParameterValue(value); err != nil {
			return err
		}
		total += len(key) + len(value)
	}
	encodedParams, err := json.Marshal(transport.Params)
	if err != nil {
		return fmt.Errorf("encode steer transport parameters: %w", err)
	}
	if total > maxSteerTransportTotalBytes || len(encodedParams) > maxSteerTransportTotalBytes {
		return fmt.Errorf("steer transport parameters exceed %d bytes", maxSteerTransportTotalBytes)
	}
	switch transport.Kind {
	case "codex-queue":
		home, ok := transport.Params["home"]
		if !ok || len(transport.Params) != 1 {
			return errors.New("codex-queue transport requires only the home parameter")
		}
		if !filepath.IsAbs(home) || filepath.Clean(home) != home {
			return errors.New("codex-queue home must be an absolute canonical path")
		}
	case "spool":
		if len(transport.Params) != 0 {
			return errors.New("spool transport does not accept parameters")
		}
	default:
		return fmt.Errorf("unsupported steer transport kind %q", transport.Kind)
	}
	return nil
}

func validateSteerParameterValue(value string) error {
	if err := validateToken("steer transport parameter value", value); err != nil {
		return err
	}
	if len(value) > maxSteerTransportValueBytes {
		return fmt.Errorf("steer transport parameter value exceeds %d bytes", maxSteerTransportValueBytes)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return errors.New("steer transport parameter value must not contain control characters")
	}
	return nil
}

func newSteerRegistrationID() (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func validSteerRegistrationID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func validateSteerName(label, value string) error {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || len(value) > 64 || value == "." || value == ".." {
		return fmt.Errorf("steer %s is invalid", label)
	}
	for index, char := range value {
		valid := (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || (index > 0 && (char == '.' || char == '_' || char == '-'))
		if !valid {
			return fmt.Errorf("steer %s is invalid", label)
		}
	}
	return nil
}

func sameSteerRoute(left, right SteerRegistration) bool {
	return left.RepositoryID == right.RepositoryID && left.Actor == right.Actor &&
		left.Role == right.Role && left.Harness == right.Harness && left.SessionID == right.SessionID &&
		left.RepositoryPath == right.RepositoryPath && sameSteerTransport(left.Transport, right.Transport)
}

func sameSteerTransport(left, right SteerTransportRoute) bool {
	if left.Kind != right.Kind || len(left.Params) != len(right.Params) {
		return false
	}
	for key, value := range left.Params {
		if right.Params[key] != value {
			return false
		}
	}
	return true
}

func sortSteerRegistrations(registrations []SteerRegistration) {
	sort.Slice(registrations, func(i, j int) bool {
		if registrations[i].RepositoryID != registrations[j].RepositoryID {
			return registrations[i].RepositoryID < registrations[j].RepositoryID
		}
		return registrations[i].Actor < registrations[j].Actor
	})
}
