package orc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/state"
)

func conversationPolicy(t *testing.T) (*Conversations, string, time.Time) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewConversations(state.NewConversationStore(dir), DefaultConversationLimits())
	if err != nil {
		t.Fatal(err)
	}
	return s, dir, time.Now().UTC()
}
func createConversation(t *testing.T, s *Conversations, now time.Time) string {
	t.Helper()
	receipt, err := s.Create(t.Context(), ConversationCreate{RequestID: NewConversationRequestID(now), Profile: "planning", Operator: "human", Execution: state.ConversationExecution{Harness: "codex", Model: "model", Actor: "planner", WorkingDir: t.TempDir(), Workspace: t.TempDir(), MaxDuration: time.Minute}}, now)
	if err != nil {
		t.Fatal(err)
	}
	return receipt.ConversationID
}
func conversationRequest(now time.Time) ConversationSubmit {
	return ConversationSubmit{RequestID: NewConversationRequestID(now), Text: "plan the work", Operator: "human"}
}
func requireConversationError(t *testing.T, err error, code string) {
	t.Helper()
	if !IsConversationError(err, code) {
		t.Fatalf("got %v; want %s", err, code)
	}
}

func TestConversationAcceptanceIsAtomicAcrossStores(t *testing.T) {
	s, dir, now := conversationPolicy(t)
	id := createConversation(t, s, now)
	other, err := NewConversations(state.NewConversationStore(dir), s.Limits)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, policy := range []*Conversations{s, other} {
		wg.Add(1)
		go func(s *Conversations) {
			defer wg.Done()
			<-start
			_, _, err := s.Accept(context.Background(), id, conversationRequest(now), now)
			errs <- err
		}(policy)
	}
	close(start)
	wg.Wait()
	close(errs)
	accepted, busy := 0, 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if IsConversationError(err, "turn_active") {
			busy++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || busy != 1 {
		t.Fatalf("accepted=%d busy=%d", accepted, busy)
	}
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Turns) != 1 || len(c.Records) != 2 {
		t.Fatalf("unexpected durable acceptance: %+v", c)
	}
}

func TestConversationDuplicateAfterRestartAndClose(t *testing.T) {
	s, dir, now := conversationPolicy(t)
	id := createConversation(t, s, now)
	req := conversationRequest(now)
	receipt, fresh, err := s.Accept(t.Context(), id, req, now)
	if err != nil || !fresh {
		t.Fatalf("%+v %t %v", receipt, fresh, err)
	}
	if err := s.FinishStopped(t.Context(), id, receipt.TurnID, "completed", "", "session", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(t.Context(), id, "closer", now); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewConversations(state.NewConversationStore(dir), s.Limits)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, fresh, err := restarted.Accept(t.Context(), id, req, now)
	if err != nil || fresh || duplicate != receipt {
		t.Fatalf("duplicate=%+v fresh=%t err=%v", duplicate, fresh, err)
	}
	req.Text = "changed"
	_, _, err = restarted.Accept(t.Context(), id, req, now)
	requireConversationError(t, err, "request_id_conflict")
	c, err := restarted.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.State != "closed" || c.ClosedBy != "closer" || len(c.Turns) != 1 {
		t.Fatalf("%+v", c)
	}
}

func TestConversationCreationDedupAndExpiredResourceNeverReexecutes(t *testing.T) {
	s, _, now := conversationPolicy(t)
	s.Limits.ClosedAge = time.Minute
	req := ConversationCreate{RequestID: NewConversationRequestID(now), Profile: "plan", Operator: "human", Execution: state.ConversationExecution{Harness: "codex", Actor: "planner", MaxDuration: time.Minute}}
	first, err := s.Create(t.Context(), req, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(t.Context(), first.ConversationID, "human", now); err != nil {
		t.Fatal(err)
	}
	if err := s.Prune(t.Context(), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	duplicate, err := s.Create(t.Context(), req, now.Add(2*time.Minute))
	if err != nil || duplicate != first {
		t.Fatalf("%+v %v", duplicate, err)
	}
	snapshot, err := s.Store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Conversations) != 0 || len(snapshot.Receipts) != 1 {
		t.Fatalf("expired history or retry receipt not bounded: %+v", snapshot)
	}
	if err := s.Prune(t.Context(), now.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, err = s.Create(t.Context(), req, now.Add(25*time.Hour))
	requireConversationError(t, err, "request_id_expired")
	snapshot, err = s.Store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Receipts) != 0 {
		t.Fatal("expired tombstones retained")
	}
}

func TestConversationCapacityAndTerminalReserve(t *testing.T) {
	s, _, now := conversationPolicy(t)
	s.Limits.Count = 2
	s.Limits.ConcurrentTurns = 1
	first := createConversation(t, s, now)
	second := createConversation(t, s, now)
	_, err := s.Create(t.Context(), ConversationCreate{RequestID: NewConversationRequestID(now), Profile: "plan", Operator: "human", Execution: state.ConversationExecution{Harness: "codex", Actor: "planner", MaxDuration: time.Minute}}, now)
	requireConversationError(t, err, "capacity_exceeded")
	r, _, err := s.Accept(t.Context(), first, conversationRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Accept(t.Context(), second, conversationRequest(now), now)
	requireConversationError(t, err, "capacity_exceeded")
	if err := s.Running(t.Context(), first, r.TurnID); err != nil {
		t.Fatal(err)
	}
	// Stop exactly at the storage bound, preserving every previously accepted
	// record and enough space for cancellation plus the terminal outcome.
	for i := 0; i < 100; i++ {
		err = s.Append(t.Context(), first, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", 1000)}})
		if err != nil {
			break
		}
	}
	requireConversationError(t, err, "transcript_limit")
	before, err := s.Get(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RequestCancel(t.Context(), first, r.TurnID, "human"); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishStopped(t.Context(), first, r.TurnID, "failed", "transcript_limit", "session", false); err != nil {
		t.Fatal(err)
	}
	after, err := s.Get(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Records) != len(before.Records)+2 || after.Turns[0].State != "cancelled" || conversationSize(after) > s.Limits.TranscriptBytes {
		t.Fatal("terminal reservation violated")
	}
	for i, record := range after.Records {
		if record.Revision != uint64(i+1) {
			t.Fatal("unordered revision")
		}
	}
}

func TestConversationCancellationIntentAndLostSession(t *testing.T) {
	s, _, now := conversationPolicy(t)
	id := createConversation(t, s, now)
	r, _, err := s.Accept(t.Context(), id, conversationRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RequestCancel(t.Context(), id, r.TurnID, "human"); err != nil {
		t.Fatal(err)
	}
	requireConversationError(t, s.Running(t.Context(), id, r.TurnID), "turn_not_launchable")
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "accepted" {
		t.Fatal("request declared terminal before stop")
	}
	if err := s.FinishStopped(t.Context(), id, r.TurnID, "cancelled", "", "", false); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Accept(t.Context(), id, conversationRequest(now), now)
	requireConversationError(t, err, "session_unavailable")
}

func TestConversationTranscriptPaginationAndIsolation(t *testing.T) {
	s, dir, now := conversationPolicy(t)
	id := createConversation(t, s, now)
	r, _, err := s.Accept(t.Context(), id, conversationRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Running(t.Context(), id, r.TurnID); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: "answer"}}); err != nil {
		t.Fatal(err)
	}
	page, err := s.Transcript(t.Context(), id, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || !page.HasMore || page.NextAfter != 2 || page.ThroughRevision != 4 || page.Turn.State != "running" {
		t.Fatalf("%+v", page)
	}
	page, err = s.Transcript(t.Context(), id, page.NextAfter, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.HasMore || page.Records[1].Message.Text != "answer" {
		t.Fatalf("%+v", page)
	}
	snapshot, err := state.NewForRepository(dir, t.TempDir()).Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Sessions) != 0 || len(snapshot.Loops) != 0 {
		t.Fatal("conversation leaked into repository state")
	}
	// A failed output transaction does not publish a new durable revision.
	err = s.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", s.Limits.RecordBytes)}})
	requireConversationError(t, err, "transcript_limit")
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Records) != 4 {
		t.Fatal("failed append persisted")
	}
}

func TestConversationOutputBeforeCancellationStopRemainsReadable(t *testing.T) {
	s, _, now := conversationPolicy(t)
	id := createConversation(t, s, now)
	r, _, err := s.Accept(t.Context(), id, conversationRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Running(t.Context(), id, r.TurnID); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestCancel(t.Context(), id, r.TurnID, "human"); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: "last complete message"}}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Turns[0].State != "running" || c.Records[len(c.Records)-1].Message.Text != "last complete message" {
		t.Fatal("cancellation lost output before child stop")
	}
}

func TestConversationAcceptanceBoundsCanonicalMessage(t *testing.T) {
	s, _, now := conversationPolicy(t)
	s.Limits.RecordBytes = 1024
	id := createConversation(t, s, now)
	// Reproduce a record that fits before canonical identity/revision fields are
	// assigned, but exceeds the bound once persisted.
	template := state.ConversationRecord{TurnID: strings.Repeat("t", 32), Kind: "message", Message: &state.ConversationMessage{ID: strings.Repeat("m", 32), Role: "operator"}}
	encoded, _ := json.Marshal(template)
	req := conversationRequest(now)
	req.Text = strings.Repeat("x", s.Limits.RecordBytes-len(encoded))
	_, fresh, err := s.Accept(t.Context(), id, req, now)
	requireConversationError(t, err, "capacity_exceeded")
	if fresh {
		t.Fatal("oversized canonical record accepted")
	}
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Turns) != 0 || len(c.Records) != 0 {
		t.Fatal("rejected acceptance changed transcript")
	}
	// The exact complete-record boundary remains usable.
	template.ConversationID = id
	template.Revision = 2
	encoded, _ = json.Marshal(template)
	req.RequestID = NewConversationRequestID(now)
	req.Text = strings.Repeat("x", s.Limits.RecordBytes-len(encoded))
	r, fresh, err := s.Accept(t.Context(), id, req, now)
	if err != nil || !fresh {
		t.Fatalf("fresh=%t err=%v", fresh, err)
	}
	c, err = s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ = json.Marshal(c.Records[1])
	if len(encoded) != s.Limits.RecordBytes {
		t.Fatalf("record bytes=%d", len(encoded))
	}
	if err := s.Running(t.Context(), id, r.TurnID); err != nil {
		t.Fatal(err)
	}
}

func TestConversationEscapedMetadataReservesLifecycleAndClose(t *testing.T) {
	for _, cancelAccepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_accepted=%t", cancelAccepted), func(t *testing.T) {
			s, _, now := conversationPolicy(t)
			s.Limits.RecordBytes = 1024
			s.Limits.TranscriptBytes = 12000
			id := createConversation(t, s, now)
			// Attribution is bounded by its JSON encoding as well as input bytes, so
			// audit values cannot expand beyond the lifecycle record budget.
			req := conversationRequest(now)
			req.Operator = strings.Repeat("\x01", 256)
			_, _, err := s.Accept(t.Context(), id, req, now)
			requireConversationError(t, err, "invalid_turn")
			req.RequestID = NewConversationRequestID(now)
			req.Operator = strings.Repeat("\x01", 42) + "xxxx"
			r, _, err := s.Accept(t.Context(), id, req, now)
			if err != nil {
				t.Fatal(err)
			}
			if !cancelAccepted {
				if err := s.Running(t.Context(), id, r.TurnID); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 100; i++ {
					err = s.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", 500)}})
					if err != nil {
						break
					}
				}
				requireConversationError(t, err, "transcript_limit")
			}
			cancelOperator := strings.Repeat("\x02", 42) + "yyyy"
			if err := s.RequestCancel(t.Context(), id, r.TurnID, cancelOperator); err != nil {
				t.Fatal(err)
			}
			if err := s.FinishStopped(t.Context(), id, r.TurnID, "failed", strings.Repeat("\x03", 21)+"zz", strings.Repeat("\x04", 256), false); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(t.Context(), id, cancelOperator, now); err != nil {
				t.Fatal(err)
			}
			c, err := s.Get(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if conversationSize(c) > s.Limits.TranscriptBytes {
				t.Fatalf("conversation bytes=%d limit=%d", conversationSize(c), s.Limits.TranscriptBytes)
			}
			for _, record := range c.Records {
				data, _ := json.Marshal(record)
				if len(data) > s.Limits.RecordBytes {
					t.Fatalf("%s record bytes=%d limit=%d", record.Kind, len(data), s.Limits.RecordBytes)
				}
			}
		})
	}
}

func TestConversationAllLifecycleRecordBounds(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "cancelled", "interrupted"} {
		t.Run(outcome, func(t *testing.T) {
			s, _, now := conversationPolicy(t)
			s.Limits.RecordBytes = 1024
			id := createConversation(t, s, now)
			req := conversationRequest(now)
			req.Operator = strings.Repeat("x", 256)
			r, _, err := s.Accept(t.Context(), id, req, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Running(t.Context(), id, r.TurnID); err != nil {
				t.Fatal(err)
			}
			if outcome == "cancelled" || outcome == "interrupted" {
				if err := s.RequestCancel(t.Context(), id, r.TurnID, strings.Repeat("y", 256)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.FinishStopped(t.Context(), id, r.TurnID, outcome, strings.Repeat("z", 128), strings.Repeat("\x01", 256), false); err != nil {
				t.Fatal(err)
			}
			c, err := s.Get(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if c.Turns[0].State != outcome {
				t.Fatalf("state=%q", c.Turns[0].State)
			}
			for _, record := range c.Records {
				data, _ := json.Marshal(record)
				if len(data) > s.Limits.RecordBytes {
					t.Fatalf("%s record bytes=%d", record.Kind, len(data))
				}
			}
		})
	}
}

func TestConversationWholeFileReservesEscapedTerminalMetadata(t *testing.T) {
	s, _, now := conversationPolicy(t)
	s.Limits.RecordBytes = 1024
	s.Limits.TranscriptBytes = state.ConversationStateBytes
	filler := createConversation(t, s, now)
	id := createConversation(t, s, now)
	// Populate bounded server-owned metadata to approach the whole-file limit
	// without a long output loop. Leave space for initial lifecycle reservations.
	if err := s.Store.Update(t.Context(), func(snapshot *state.ConversationSnapshot) error {
		c := snapshot.Conversations[filler]
		c.Execution.Model = ""
		snapshot.Conversations[filler] = c
		data, _ := json.Marshal(snapshot)
		c.Execution.Model = strings.Repeat("p", state.ConversationStateBytes-12000-len(data))
		snapshot.Conversations[filler] = c
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	req := conversationRequest(now)
	req.Operator = strings.Repeat("\x01", 42) + "xxxx"
	r, _, err := s.Accept(t.Context(), id, req, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Running(t.Context(), id, r.TurnID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		err = s.Append(t.Context(), id, r.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", 500)}})
		if err != nil {
			break
		}
	}
	requireConversationError(t, err, "transcript_limit")
	c, err := s.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if conversationSize(c) > 12000 {
		t.Fatal("test hit conversation bound instead of whole-file bound")
	}
	operator := strings.Repeat("\x02", 42) + "yyyy"
	if err := s.RequestCancel(t.Context(), id, r.TurnID, operator); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishStopped(t.Context(), id, r.TurnID, "failed", strings.Repeat("\x03", 21)+"zz", strings.Repeat("\x04", 256), false); err != nil {
		t.Fatal(err)
	}
	for _, conversation := range []string{filler, id} {
		if err := s.Close(t.Context(), conversation, operator, now); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := s.Store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(snapshot)
	if len(data) > state.ConversationStateBytes {
		t.Fatalf("state bytes=%d", len(data))
	}
}

func TestConversationPublishesOnlyOrderedCommittedRecords(t *testing.T) {
	policy, _, now := conversationPolicy(t)
	id := createConversation(t, policy, now)
	var revisions []uint64
	policy.SetPublisher(func(record state.ConversationRecord) {
		// Reading the committed file here also proves publication holds no store lock.
		c, err := policy.Get(t.Context(), id)
		if err != nil || uint64(len(c.Records)) < record.Revision {
			t.Errorf("published before commit: %+v %v", record, err)
		}
		revisions = append(revisions, record.Revision)
	})
	req := conversationRequest(now)
	receipt, _, err := policy.Accept(t.Context(), id, req, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Running(t.Context(), id, receipt.TurnID); err != nil {
		t.Fatal(err)
	}
	if err := policy.Append(t.Context(), id, receipt.TurnID, state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", policy.Limits.RecordBytes)}}); !IsConversationError(err, "transcript_limit") {
		t.Fatal(err)
	}
	if _, _, err := policy.Accept(t.Context(), id, req, now); err != nil {
		t.Fatal(err)
	}
	if err := policy.FinishStopped(t.Context(), id, receipt.TurnID, "completed", "", "session", true); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(revisions) != "[1 2 3 4]" {
		t.Fatalf("failed/duplicate mutation published or out of order: %v", revisions)
	}
}
