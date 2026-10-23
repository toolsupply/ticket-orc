package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConversationStoreRejectsMalformedAndUnsupportedState(t *testing.T) {
	for name, data := range map[string]string{
		"version":  `{"version":2,"conversations":{},"receipts":{},"expired":{}}`,
		"unknown":  `{"version":1,"conversations":{},"receipts":{},"expired":{},"legacy":true}`,
		"missing":  `{"version":1}`,
		"trailing": `{"version":1,"conversations":{},"receipts":{},"expired":{}} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, conversationFileName), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewConversationStore(dir).Read(t.Context()); !errors.Is(err, ErrMalformed) {
				t.Fatal(err)
			}
		})
	}
}

func TestConversationStoreAtomicFailureAndHardBound(t *testing.T) {
	dir := t.TempDir()
	store := NewConversationStore(dir)
	c := Conversation{ID: "conversation", State: "open", Profile: "planning", Operator: "human", Execution: ConversationExecution{Harness: "codex", Actor: "planner", MaxDuration: time.Minute}, CreatedAt: time.Now()}
	if err := store.Update(t.Context(), func(s *ConversationSnapshot) error { s.Conversations[c.ID] = c; return nil }); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("rejected")
	if err := store.Update(t.Context(), func(s *ConversationSnapshot) error { delete(s.Conversations, c.ID); return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	err := store.Update(t.Context(), func(s *ConversationSnapshot) error {
		c.Operator = strings.Repeat("x", maxStateBytes)
		s.Conversations[c.ID] = c
		return nil
	})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	snapshot, err := store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Conversations[c.ID].Operator != "human" {
		t.Fatal("failed transaction changed state")
	}
	// The separate file shares the same cross-process lock discipline as other
	// orchestration state, rather than permitting unsynchronized replacements.
	lock, err := AcquireLock(t.Context(), dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
