package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const daemonControlFileName = "daemon-control.json"
const daemonControlVersion = 1

// DaemonControlMode is the durable daemon-wide dispatch mode.
type DaemonControlMode string

const (
	DaemonRunning DaemonControlMode = "running"
	DaemonPaused  DaemonControlMode = "paused"
	DaemonAborted DaemonControlMode = "aborted"
)

// DaemonControlState is stored separately from Ticket steer registrations.
type DaemonControlState struct {
	Version   int               `json:"version"`
	Mode      DaemonControlMode `json:"mode"`
	ChangedAt time.Time         `json:"changed_at,omitempty"`
}

type daemonControlWire struct {
	Version   int               `json:"version"`
	Mode      DaemonControlMode `json:"mode"`
	ChangedAt time.Time         `json:"changed_at,omitempty"`
}

// DaemonControlStore owns the instance-wide control file. Its state is not
// ticket-bound and therefore has no repository namespace.
type DaemonControlStore struct {
	store *Store
}

// NewDaemonControlStore returns the instance-wide daemon control store rooted
// at dir.
func NewDaemonControlStore(dir string) *DaemonControlStore {
	return &DaemonControlStore{store: NewAdministrative(dir)}
}

// DaemonControl loads the durable control mode. Missing state means running.
func (s *DaemonControlStore) DaemonControl(ctx context.Context) (DaemonControlState, error) {
	var result DaemonControlState
	err := s.store.withLock(ctx, func() error {
		var err error
		result, err = s.store.loadDaemonControlUnlocked()
		return err
	})
	if err != nil {
		return DaemonControlState{}, err
	}
	return result, nil
}

// PauseDaemon durably pauses dispatch. An aborted mode is never downgraded.
func (s *DaemonControlStore) PauseDaemon(ctx context.Context) (DaemonControlState, error) {
	return s.updateDaemonControl(ctx, func(current DaemonControlState) DaemonControlState {
		if current.Mode == DaemonAborted {
			return current
		}
		if current.Mode == DaemonPaused {
			return current
		}
		return changedDaemonControl(DaemonPaused)
	})
}

// ResumeDaemon durably opens dispatch by returning to running mode.
func (s *DaemonControlStore) ResumeDaemon(ctx context.Context) (DaemonControlState, error) {
	return s.updateDaemonControl(ctx, func(current DaemonControlState) DaemonControlState {
		if current.Mode == DaemonRunning {
			return current
		}
		return changedDaemonControl(DaemonRunning)
	})
}

// AbortDaemon durably enters the emergency-stop mode.
func (s *DaemonControlStore) AbortDaemon(ctx context.Context) (DaemonControlState, error) {
	return s.updateDaemonControl(ctx, func(current DaemonControlState) DaemonControlState {
		if current.Mode == DaemonAborted {
			return current
		}
		return changedDaemonControl(DaemonAborted)
	})
}

func changedDaemonControl(mode DaemonControlMode) DaemonControlState {
	return DaemonControlState{Version: daemonControlVersion, Mode: mode, ChangedAt: time.Now().UTC()}
}

func (s *DaemonControlStore) updateDaemonControl(ctx context.Context, update func(DaemonControlState) DaemonControlState) (DaemonControlState, error) {
	var result DaemonControlState
	err := s.store.withLock(ctx, func() error {
		current, err := s.store.loadDaemonControlUnlocked()
		if err != nil {
			return err
		}
		result = update(current)
		if result == current {
			return nil
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("encode daemon control state: %w", err)
		}
		data = append(data, '\n')
		if err := writeStateAtomic(filepath.Join(s.store.dir, daemonControlFileName), data); err != nil {
			return fmt.Errorf("persist daemon control state: %w", err)
		}
		return nil
	})
	if err != nil {
		return DaemonControlState{}, err
	}
	return result, nil
}

func (s *Store) loadDaemonControlUnlocked() (DaemonControlState, error) {
	path := filepath.Join(s.dir, daemonControlFileName)
	data, err := readBoundedStateFile(path)
	if err != nil {
		if isMissingStateFile(err) {
			return DaemonControlState{Version: daemonControlVersion, Mode: DaemonRunning}, nil
		}
		return DaemonControlState{}, fmt.Errorf("read daemon control state %s: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire daemonControlWire
	if err := decoder.Decode(&wire); err != nil {
		return DaemonControlState{}, fmt.Errorf("decode daemon control state %s: %w: %v", path, ErrMalformed, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return DaemonControlState{}, fmt.Errorf("decode daemon control state %s: %w", path, err)
	}
	if wire.Version != daemonControlVersion {
		return DaemonControlState{}, fmt.Errorf("decode daemon control state %s: %w: unsupported version %d; expected %d", path, ErrMalformed, wire.Version, daemonControlVersion)
	}
	if wire.Mode != DaemonRunning && wire.Mode != DaemonPaused && wire.Mode != DaemonAborted {
		return DaemonControlState{}, fmt.Errorf("decode daemon control state %s: %w: unsupported mode %q", path, ErrMalformed, wire.Mode)
	}
	return DaemonControlState{Version: wire.Version, Mode: wire.Mode, ChangedAt: wire.ChangedAt}, nil
}

func isMissingStateFile(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
