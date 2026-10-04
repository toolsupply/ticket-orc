package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (s *Store) withLock(ctx context.Context, fn func() error) error {
	if strings.TrimSpace(s.dir) == "" {
		return fmt.Errorf("state directory must not be empty")
	}
	if ctx == nil {
		return fmt.Errorf("state context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := prepareDir(s.dir); err != nil {
		return fmt.Errorf("prepare state directory %s: %w", s.dir, err)
	}
	lock, err := acquireLock(ctx, s.dir, s.lockTimeout)
	if err != nil {
		return err
	}
	operationErr := fn()
	releaseErr := lock.Release()
	if operationErr != nil {
		return operationErr
	}
	return releaseErr
}

func prepareDir(dir string) error {
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create state directory %s: %w", dir, err)
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return fmt.Errorf("inspect state directory %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("state directory must be a real directory: %s", dir)
	}
	if err := securePrivateDirectory(dir); err != nil {
		return fmt.Errorf("secure state directory %s: %w", dir, err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("verify state directory %s: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !privateDirectoryModeMatches(info, 0o700) {
		return fmt.Errorf("state directory is not a private directory: %s", dir)
	}
	return nil
}

func (s *Store) loadUnlocked() (Snapshot, error) {
	path := filepath.Join(s.dir, stateFileName)
	data, err := readBoundedStateFile(path)
	if os.IsNotExist(err) {
		return emptySnapshot(), nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, err := decodeState(data)
	if err != nil {
		return Snapshot{}, fmt.Errorf("decode state file %s: %w", path, err)
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err == nil && header.Version == 1 {
		// Persist the deliberate v1 -> v2 conversion while the caller already
		// holds the state lock. Existing sessions and repository keys survive.
		if err := s.saveUnlocked(snapshot); err != nil {
			return Snapshot{}, fmt.Errorf("migrate state file %s: %w", path, err)
		}
	}
	return snapshot, nil
}

func readBoundedStateFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("inspect state file %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("state file must be a regular file: %s", path)
	}
	if info.Size() > maxStateBytes {
		return nil, fmt.Errorf("%w: state file %s exceeds %d bytes", ErrMalformed, path, maxStateBytes)
	}
	file, err := openStateFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state file %s: %w", path, err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened state file %s: %w", path, err)
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("state file must be a regular file: %s", path)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("state file changed while opening: %s", path)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxStateBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read state file %s: %w", path, err)
	}
	if len(data) > maxStateBytes {
		return nil, fmt.Errorf("%w: state file %s exceeds %d bytes", ErrMalformed, path, maxStateBytes)
	}
	return data, nil
}

var openStateFile = os.Open

type stateWire struct {
	Version  int                   `json:"version"`
	Sessions []Session             `json:"sessions"`
	Bounces  map[string]int        `json:"bounces,omitempty"`
	Loops    map[string]TicketLoop `json:"ticket_loops,omitempty"`
}

func decodeState(data []byte) (Snapshot, error) {
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if header.Version != 1 && header.Version != currentVersion {
		return Snapshot{}, fmt.Errorf("%w: unsupported state version %d; expected %d", ErrMalformed, header.Version, currentVersion)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire stateWire
	if err := decoder.Decode(&wire); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Snapshot{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if header.Version == 1 && fields["ticket_loops"] != nil {
		return Snapshot{}, fmt.Errorf("%w: released state version 1 must not contain ticket_loops", ErrMalformed)
	}
	if header.Version == currentVersion && fields["bounces"] != nil {
		return Snapshot{}, fmt.Errorf("%w: schema v2 must not contain legacy bounce records", ErrMalformed)
	}
	if header.Version == currentVersion && fields["ticket_loops"] == nil {
		return Snapshot{}, fmt.Errorf("%w: schema v2 must contain ticket_loops", ErrMalformed)
	}
	if header.Version == currentVersion && bytes.Equal(bytes.TrimSpace(fields["ticket_loops"]), []byte("null")) {
		return Snapshot{}, fmt.Errorf("%w: schema v2 ticket_loops must be an object", ErrMalformed)
	}
	snapshot := Snapshot{Version: currentVersion, Sessions: wire.Sessions, Loops: wire.Loops, Bounces: map[string]int{}}
	if snapshot.Sessions == nil {
		snapshot.Sessions = []Session{}
	}
	if snapshot.Loops == nil {
		snapshot.Loops = map[string]TicketLoop{}
	}
	if header.Version == 1 {
		for key, count := range wire.Bounces {
			repository, ticket, namespaced := splitBounceKey(key)
			if !namespaced {
				return Snapshot{}, fmt.Errorf("%w: bounce key must include a repository namespace", ErrMalformed)
			}
			loop := TicketLoop{Repository: repository, Ticket: ticket, BounceCount: count, Phase: TicketLoopActive}
			if _, exists := snapshot.Loops[key]; exists {
				return Snapshot{}, fmt.Errorf("%w: duplicate ticket loop during migration", ErrMalformed)
			}
			snapshot.Loops[key] = loop
		}
	}
	refreshBounceView(&snapshot)
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: multiple JSON values", ErrMalformed)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrMalformed, err)
	}
	return nil
}

func (s *Store) saveUnlocked(snapshot Snapshot) error {
	snapshot.Version = currentVersion
	if snapshot.Sessions == nil {
		snapshot.Sessions = []Session{}
	}
	if snapshot.Bounces == nil {
		snapshot.Bounces = map[string]int{}
	}
	if snapshot.Loops == nil {
		snapshot.Loops = map[string]TicketLoop{}
	}
	sort.Slice(snapshot.Sessions, func(i, j int) bool {
		left, right := snapshot.Sessions[i], snapshot.Sessions[j]
		if left.Repository != right.Repository {
			return left.Repository < right.Repository
		}
		if left.Ticket != right.Ticket {
			return left.Ticket < right.Ticket
		}
		if left.Role != right.Role {
			return left.Role < right.Role
		}
		return left.Harness < right.Harness
	})
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxStateBytes {
		return fmt.Errorf("%w: encoded state is %d bytes; limit is %d", ErrTooLarge, len(data), maxStateBytes)
	}
	return writeStateAtomic(filepath.Join(s.dir, stateFileName), data)
}

func emptySnapshot() Snapshot {
	return Snapshot{Version: currentVersion, Sessions: []Session{}, Loops: map[string]TicketLoop{}, Bounces: map[string]int{}}
}

func refreshBounceView(snapshot *Snapshot) {
	snapshot.Bounces = make(map[string]int, len(snapshot.Loops))
	for key, loop := range snapshot.Loops {
		if loop.BounceCount > 0 {
			snapshot.Bounces[key] = loop.BounceCount
		}
	}
}
