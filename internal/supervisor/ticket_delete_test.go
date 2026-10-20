package supervisor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTicketDeletionHoldsLifecycleLockWithoutStateLock(t *testing.T) {
	const repository = "5dc15231-0b71-4bb8-bb22-9dbf655e29ee"
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	lifecycle := NewLifecycleManager([]RunWorker{}, LifecycleHooks[RunWorker, int]{
		Name: func(w RunWorker) string { return w.Name },
		ChildWorker: func(int) RunWorker {
			close(entered)
			<-release
			return RunWorker{Config: RoleConfig{RepositoryIdentity: repository}}
		},
	})
	lifecycle.TrackChild("active", 1)
	done := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("deletion did not exit")
		}
	})
	stateDir := t.TempDir()
	go func() {
		defer close(finished)
		_, err := DeleteRepositoryTicket(context.Background(), lifecycle, nil, stateDir, repository, "20261017-00001")
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("deletion did not reach child inspection")
	}
	// The inspection barrier proves the operation has acquired its locks.
	// TryLock assertions require no timing assumption about another goroutine.
	if lifecycle.gate.global.TryLock() {
		lifecycle.gate.global.Unlock()
		t.Error("deletion did not serialize with start/stop/reload")
	}
	if !lifecycle.mu.TryLock() {
		t.Error("manager state lock held during child inspection")
	} else {
		lifecycle.mu.Unlock()
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, ErrRepositoryWorkersRunning) {
			t.Fatalf("delete error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deletion did not finish")
	}
}
