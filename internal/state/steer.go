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
	steerRegistrationFileVersion = 4
	maxSteerFileSize             = 1 << 20
)

// SteerRegistration describes one externally-owned Codex session's current
// Ticket routing. The logical key is repository ID plus Ticket actor.
type SteerRegistration struct {
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
	if s == nil || strings.TrimSpace(s.dir) == "" {
		return SteerRegistration{}, nil, false, errors.New("steer state directory must not be empty")
	}
	if err := validateSteerRegistrationRoute(next); err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("invalid steer registration: %w", err)
	}
	next.JoinSignal, err = newSteerRegistrationID()
	if err != nil {
		return SteerRegistration{}, nil, false, fmt.Errorf("generate steer join signal: %w", err)
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
	snapshot.Registrations = append(snapshot.Registrations, next)
	sortSteerRegistrations(snapshot.Registrations)
	if err := s.saveUnlocked(snapshot); err != nil {
		return SteerRegistration{}, nil, false, err
	}
	return next, nil, true, nil
}

// Leave removes a registration only when the current local thread still owns
// the key. A missing or replaced session is a harmless no-op.
func (s *RegistrationStore) Leave(ctx context.Context, repositoryID, actor, repositoryPath, codexHome, threadID string) (removed bool, err error) {
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
		if registration.RepositoryID != repositoryID || registration.Actor != actor || registration.RepositoryPath != repositoryPath || registration.CodexHome != codexHome || registration.ThreadID != threadID {
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
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot SteerSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := validateSteerSnapshot(snapshot); err != nil {
		return SteerSnapshot{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return snapshot, true, nil
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
	if !validSteerRegistrationID(registration.JoinSignal) {
		return errors.New("join_signal must be a 128-bit lowercase hexadecimal ID")
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
	for _, field := range []struct{ name, value string }{
		{"repository_path", registration.RepositoryPath},
		{"codex_home", registration.CodexHome},
		{"thread_id", registration.ThreadID},
	} {
		name, value := field.name, field.value
		if err := validateToken("steer "+name, value); err != nil {
			return err
		}
		if strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return fmt.Errorf("steer %s must not contain control characters", name)
		}
	}
	if !filepath.IsAbs(registration.RepositoryPath) || filepath.Clean(registration.RepositoryPath) != registration.RepositoryPath {
		return errors.New("repository_path must be an absolute canonical path")
	}
	if !filepath.IsAbs(registration.CodexHome) || filepath.Clean(registration.CodexHome) != registration.CodexHome {
		return errors.New("codex_home must be an absolute canonical path")
	}
	if registration.RepositoryName != "" && strings.IndexFunc(registration.RepositoryName, unicode.IsControl) >= 0 {
		return errors.New("repository_name must not contain control characters")
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
		left.Role == right.Role && left.ThreadID == right.ThreadID &&
		left.CodexHome == right.CodexHome && left.RepositoryPath == right.RepositoryPath
}

func sortSteerRegistrations(registrations []SteerRegistration) {
	sort.Slice(registrations, func(i, j int) bool {
		if registrations[i].RepositoryID != registrations[j].RepositoryID {
			return registrations[i].RepositoryID < registrations[j].RepositoryID
		}
		return registrations[i].Actor < registrations[j].Actor
	})
}
