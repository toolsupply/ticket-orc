package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDaemonControlMissingAndPersistedModes(t *testing.T) {
	dir := t.TempDir()
	store := NewDaemonControlStore(dir)
	ctx := context.Background()

	got, err := store.DaemonControl(ctx)
	if err != nil || got.Mode != DaemonRunning || got.Version != daemonControlVersion {
		t.Fatalf("missing daemon control = %#v, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, daemonControlFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing-state read created file: %v", err)
	}

	for _, mode := range []DaemonControlMode{DaemonPaused, DaemonAborted} {
		var changed DaemonControlState
		if mode == DaemonPaused {
			changed, err = store.PauseDaemon(ctx)
		} else {
			changed, err = store.AbortDaemon(ctx)
		}
		if err != nil || changed.Mode != mode || changed.ChangedAt.IsZero() {
			t.Fatalf("persist %s = %#v, %v", mode, changed, err)
		}
		restarted := NewDaemonControlStore(dir)
		loaded, err := restarted.DaemonControl(ctx)
		if err != nil || loaded != changed {
			t.Fatalf("reload %s = %#v, %v; want %#v", mode, loaded, err, changed)
		}
		if mode == DaemonPaused {
			if _, err := store.ResumeDaemon(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestDaemonControlMutationsAreIdempotentAndOrdered(t *testing.T) {
	store := NewDaemonControlStore(t.TempDir())
	ctx := context.Background()

	paused, err := store.PauseDaemon(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pausedAgain, err := store.PauseDaemon(ctx)
	if err != nil || pausedAgain != paused {
		t.Fatalf("repeated pause = %#v, %v; want %#v", pausedAgain, err, paused)
	}
	loaded, err := store.DaemonControl(ctx)
	if err != nil || loaded != paused {
		t.Fatalf("pause returned before durable state was readable: %#v, %v", loaded, err)
	}

	aborted, err := store.AbortDaemon(ctx)
	if err != nil || aborted.Mode != DaemonAborted {
		t.Fatalf("abort = %#v, %v", aborted, err)
	}
	pausedAgain, err = store.PauseDaemon(ctx)
	if err != nil || pausedAgain != aborted {
		t.Fatalf("pause downgraded aborted state: %#v, %v", pausedAgain, err)
	}
	resumed, err := store.ResumeDaemon(ctx)
	if err != nil || resumed.Mode != DaemonRunning {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	resumedAgain, err := store.ResumeDaemon(ctx)
	if err != nil || resumedAgain != resumed {
		t.Fatalf("repeated resume = %#v, %v; want %#v", resumedAgain, err, resumed)
	}
}

func TestMalformedDaemonControlFailsClosed(t *testing.T) {
	for name, contents := range map[string]string{
		"invalid json":        `{`,
		"unsupported version": `{"version":2,"mode":"running"}`,
		"unsupported mode":    `{"version":1,"mode":"stopped"}`,
		"unknown field":       `{"version":1,"mode":"running","extra":true}`,
		"trailing value":      `{"version":1,"mode":"running"}{}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, daemonControlFileName), []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := NewDaemonControlStore(dir).DaemonControl(context.Background())
			if !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "daemon control state") {
				t.Fatalf("malformed state error = %v", err)
			}
		})
	}
}
