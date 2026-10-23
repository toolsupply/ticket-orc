package orc

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/jsonx"
	"github.com/toolsupply/ticket-orc/internal/state"
)

func TestConversationProfileCaptureAndReloadDedup(t *testing.T) {
	policy, err := NewConversations(state.NewConversationStore(t.TempDir()), DefaultConversationLimits())
	if err != nil {
		t.Fatal(err)
	}
	profile := ConversationProfile{Name: "planner", Execution: state.ConversationExecution{Harness: "codex", Actor: "agent", Model: "model-a", WorkingDir: "/private", Workspace: "/private/work", CodexSandbox: "danger-full-access", MaxDuration: time.Minute, Repositories: []state.ConversationRepository{{ID: "a", Path: "/private/a"}, {ID: "b", Path: "/private/b"}}}}
	profiles := NewConversationProfiles([]ConversationProfile{profile})
	req := ConversationProfileCreate{RequestID: NewConversationRequestID(time.Now()), Profile: "planner", RepositoryIDs: []string{"b"}, Operator: "human"}
	receipt, err := profiles.Create(t.Context(), policy, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	profile.Execution.Actor = "different"
	profile.Execution.Model = "model-b"
	profile.Execution.Repositories[1].Path = "/changed"
	profiles.Replace([]ConversationProfile{profile})
	duplicate, err := profiles.Create(t.Context(), policy, req, time.Now())
	if err != nil || duplicate != receipt {
		t.Fatalf("dedup: %#v %v", duplicate, err)
	}
	captured, err := policy.Get(t.Context(), receipt.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Operator != "human" || captured.Execution.Actor != "agent" || captured.Execution.Model != "model-a" || len(captured.Execution.Repositories) != 1 || captured.Execution.Repositories[0].Path != "/private/b" {
		t.Fatalf("capture changed: %#v", captured)
	}
	profiles.Replace(nil)
	if duplicate, err := profiles.Create(t.Context(), policy, req, time.Now()); err != nil || duplicate != receipt {
		t.Fatalf("removed profile broke dedup: %v", err)
	}
	req.RequestID = NewConversationRequestID(time.Now())
	if _, err := profiles.Create(t.Context(), policy, req, time.Now()); !IsConversationError(err, "profile_unavailable") {
		t.Fatalf("disabled creation: %v", err)
	}
	profiles.Replace([]ConversationProfile{profile})
	for _, ids := range [][]string{{"other"}, {"a", "a"}} {
		req.RequestID = NewConversationRequestID(time.Now())
		req.RepositoryIDs = ids
		if _, err := profiles.Create(t.Context(), policy, req, time.Now()); !IsConversationError(err, "repository_out_of_scope") {
			t.Fatalf("invalid subset %v: %v", ids, err)
		}
	}
	metadata, _ := json.Marshal(profiles.Metadata())
	for _, secret := range []string{"agent", "different", "/private", "model-b", "danger-full-access"} {
		if strings.Contains(string(metadata), secret) {
			t.Fatalf("private profile data: %s", metadata)
		}
	}
	for _, field := range []string{"actor", "env", "argv", "working_dir", "sandbox", "execution"} {
		var create ConversationProfileCreate
		if err := jsonx.Decode([]byte(`{"profile":"planner","`+field+`":"injected"}`), &create); err == nil {
			t.Fatalf("creation accepted %s", field)
		}
	}
}

func TestConversationProfileTranscriptLimitIsCaptured(t *testing.T) {
	limits := DefaultConversationLimits()
	policy, err := NewConversations(state.NewConversationStore(t.TempDir()), limits)
	if err != nil {
		t.Fatal(err)
	}
	profiles := NewConversationProfiles([]ConversationProfile{{Name: "bounded", Execution: state.ConversationExecution{Harness: "codex", Actor: "agent", MaxDuration: time.Minute, TranscriptBytes: limits.RecordBytes}}})
	receipt, err := profiles.Create(t.Context(), policy, ConversationProfileCreate{RequestID: NewConversationRequestID(time.Now()), Profile: "bounded", Operator: "human"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	turn, _, err := policy.Accept(t.Context(), receipt.ConversationID, ConversationSubmit{RequestID: NewConversationRequestID(time.Now()), Text: "Plan", Operator: "human"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Running(t.Context(), receipt.ConversationID, turn.TurnID); err != nil {
		t.Fatal(err)
	}
	message := state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: strings.Repeat("x", 10000)}}
	if err := policy.Append(t.Context(), receipt.ConversationID, turn.TurnID, message); err != nil {
		t.Fatal(err)
	}
	message.Message.ID = "" // Append assigns each message a fresh stable ID.
	if err := policy.Append(t.Context(), receipt.ConversationID, turn.TurnID, message); !IsConversationError(err, "transcript_limit") {
		t.Fatalf("profile storage bound ignored: %v", err)
	}
	if err := policy.FinishStopped(t.Context(), receipt.ConversationID, turn.TurnID, "failed", "transcript_limit", "", false); err != nil {
		t.Fatalf("terminal reserve lost: %v", err)
	}
}
