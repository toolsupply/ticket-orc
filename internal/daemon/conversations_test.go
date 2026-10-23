package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

func conversationTestServer(t *testing.T, dir string, hooks supervisor.ConversationProcessHooks, configuredLimits ...orc.ConversationLimits) (*Server, *ConversationAPI) {
	t.Helper()
	limits := orc.DefaultConversationLimits()
	if len(configuredLimits) > 0 {
		limits = configuredLimits[0]
	}
	policy, err := orc.NewConversations(state.NewConversationStore(dir), limits)
	if err != nil {
		t.Fatal(err)
	}
	profiles := orc.NewConversationProfiles([]orc.ConversationProfile{{Name: "planner", Execution: state.ConversationExecution{Harness: "codex", Actor: "agent", WorkingDir: "/private/path", MaxDuration: time.Minute}}})
	manager := supervisor.NewConversationManager(policy, hooks)
	if err := manager.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	api := &ConversationAPI{Policy: policy, Profiles: profiles, Manager: manager}
	s, err := NewServer(Config{StateDir: dir, EndpointKey: testEndpointKey, EventHeartbeat: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetConversations(api); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		manager.Shutdown(ctx)
		s.Shutdown(ctx)
	})
	return s, api
}

func conversationHTTP(t *testing.T, s *Server, method, path string, body any, want int) []byte {
	t.Helper()
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		input = bytes.NewReader(data)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s.Endpoint().CapabilityURL()+path, input)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != want {
		t.Fatalf("%s %s status=%d want=%d: %s", method, path, res.StatusCode, want, data)
	}
	return data
}
func decodeConversationJSON[T any](t *testing.T, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func conversationCreateRequest() orc.ConversationProfileCreate {
	return orc.ConversationProfileCreate{RequestID: orc.NewConversationRequestID(time.Now()), Profile: "planner", RepositoryIDs: []string{}, Operator: "human"}
}

func TestConversationHTTPStreamingDetachedRecovery(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	emitted := make(chan struct{})
	var runs atomic.Int32
	s, api := conversationTestServer(t, t.TempDir(), supervisor.ConversationProcessHooks{Run: func(ctx context.Context, c state.Conversation, turn state.ConversationTurn, text string, emit func(state.ConversationRecord) error) (harness.RunResult, error) {
		runs.Add(1)
		if err := emit(state.ConversationRecord{Kind: "message", Message: &state.ConversationMessage{ID: "stable-message", Role: "assistant", Text: "partial before exit"}}); err != nil {
			return harness.RunResult{}, err
		}
		close(emitted)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return harness.RunResult{SessionID: "private-provider"}, nil
	}})
	request := conversationCreateRequest()
	c := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "POST", "/v1/conversations", request, 201))
	// Subscribe before snapshot; merge every record by revision.
	streamCtx, cancelStream := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelStream()
	req, _ := http.NewRequestWithContext(streamCtx, "GET", s.Endpoint().CapabilityURL()+"/v1/events", nil)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	read := func() Event {
		t.Helper()
		for {
			line, e := reader.ReadString('\n')
			if e != nil {
				t.Fatal(e)
			}
			if strings.HasPrefix(line, "data: ") {
				return decodeConversationJSON[Event](t, []byte(strings.TrimPrefix(line, "data: ")))
			}
		}
	}
	if e := read(); e.Type != "stream.sync" {
		t.Fatal(e)
	}
	turnReq := orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "private human text", Operator: "human"}
	receipt := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, s, "POST", "/v1/conversations/"+c.ID+"/turns", turnReq, 202))
	select {
	case <-emitted:
	case <-streamCtx.Done():
		t.Fatal("no live output")
	}
	live := map[uint64]state.ConversationRecord{}
	for {
		e := read()
		if e.Conversation != nil {
			live[e.Conversation.Revision] = *e.Conversation
			if e.Conversation.Message != nil && e.Conversation.Message.Role == "assistant" {
				break
			}
		}
	}
	path := "/v1/conversations/" + c.ID
	metadata := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "GET", path, nil, 200))
	if metadata.Turn == nil || metadata.Turn.State != "running" {
		t.Fatalf("output arrived after exit: %+v", metadata)
	}
	conversationHTTP(t, s, "POST", path+"/close", map[string]string{"operator": "human"}, 409)
	busy := turnReq
	busy.RequestID = orc.NewConversationRequestID(time.Now())
	conversationHTTP(t, s, "POST", path+"/turns", busy, 409)
	// Closing the request/stream cannot stop the accepted execution.
	cancelStream()
	response.Body.Close()
	duplicate := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, s, "POST", path+"/turns", turnReq, 202))
	if duplicate != receipt {
		t.Fatalf("duplicate receipt: %+v %+v", receipt, duplicate)
	}
	merged := map[uint64]state.ConversationRecord{}
	after := uint64(0)
	for {
		var page struct {
			Records []state.ConversationRecord `json:"records"`
			Next    uint64                     `json:"next_after"`
			More    bool                       `json:"has_more"`
		}
		page = decodeConversationJSON[struct {
			Records []state.ConversationRecord `json:"records"`
			Next    uint64                     `json:"next_after"`
			More    bool                       `json:"has_more"`
		}](t, conversationHTTP(t, s, "GET", fmt.Sprintf("%s/transcript?after=%d&limit=1", path, after), nil, 200))
		for _, r := range page.Records {
			merged[r.Revision] = r
		}
		after = page.Next
		if !page.More {
			break
		}
	}
	for revision, r := range live {
		merged[revision] = r
	}
	if len(merged) != int(metadata.Revision) || runs.Load() != 1 {
		t.Fatalf("snapshot overlap or duplicate launch: %d %+v", len(merged), metadata)
	}
	// Stop through cancellation and wait for the durable terminal barrier.
	cancelBody := map[string]string{"operator": "human"}
	data := conversationHTTP(t, s, "POST", path+"/turns/"+receipt.TurnID+"/cancel", cancelBody, 200)
	if !strings.Contains(string(data), `"stop_confirmed":true`) {
		t.Fatalf("cancel: %s", data)
	}
	if err := api.Manager.Wait(t.Context(), receipt.TurnID); err != nil {
		t.Fatal(err)
	}
	conversationHTTP(t, s, "POST", path+"/close", cancelBody, 200)
	conversationHTTP(t, s, "POST", path+"/close", cancelBody, 200)
	// Reconstruct policy/runtime from disk and replay exact HTTP requests. No turn
	// runs again, even though the conversation is closed and the manager aborted.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, recovered := conversationTestServer(t, s.config.StateDir, supervisor.ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		t.Error("replayed execution")
		return harness.RunResult{}, nil
	}})
	recovered.Manager.BlockExecution()
	conversationHTTP(t, restarted, "POST", "/v1/conversations", request, 201)
	got := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, restarted, "POST", path+"/turns", turnReq, 202))
	if got != receipt {
		t.Fatal(got)
	}
	turnReq.Text = "conflict"
	conversationHTTP(t, restarted, "POST", path+"/turns", turnReq, 409)
}

func TestConversationHTTPBoundsPrivacyAndValidation(t *testing.T) {
	s, api := conversationTestServer(t, t.TempDir(), supervisor.ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		return harness.RunResult{SessionID: "private-session"}, nil
	}})
	request := conversationCreateRequest()
	c := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "POST", "/v1/conversations", request, 201))
	for _, path := range []string{"/v1/status", "/v1/conversation-profiles", "/v1/conversations", "/v1/conversations/" + c.ID} {
		data := conversationHTTP(t, s, "GET", path, nil, 200)
		for _, secret := range []string{"/private/path", "private-session", `"actor"`, `"execution"`} {
			if strings.Contains(string(data), secret) {
				t.Fatalf("%s leaked %s: %s", path, secret, data)
			}
		}
	}
	capabilities := s.currentStatus().Capabilities.Conversations
	if capabilities == nil || !capabilities.Enabled || !capabilities.Stream || capabilities.RequestIDWindowSeconds != 86400 || capabilities.RecordBytes != api.Policy.Limits.RecordBytes {
		t.Fatalf("capabilities %+v", capabilities)
	}
	for _, test := range []struct {
		body any
		want int
	}{
		{map[string]any{"profile": "planner", "request_id": orc.NewConversationRequestID(time.Now()), "operator": "human", "actor": "injected"}, 400},
		{orc.ConversationProfileCreate{Profile: "missing", RequestID: orc.NewConversationRequestID(time.Now()), Operator: "human"}, 422},
		{orc.ConversationProfileCreate{Profile: "planner", RequestID: orc.NewConversationRequestID(time.Now()), Operator: "human", RepositoryIDs: []string{"unknown"}}, 400},
		{orc.ConversationProfileCreate{Profile: "planner", RequestID: orc.NewConversationRequestID(time.Now().Add(-25 * time.Hour)), Operator: "human"}, 400},
	} {
		conversationHTTP(t, s, "POST", "/v1/conversations", test.body, test.want)
	}
	path := "/v1/conversations/" + c.ID
	conversationHTTP(t, s, "POST", path+"/turns", orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: strings.Repeat("x", api.Policy.Limits.TextBytes+1), Operator: "human"}, 400)
	conversationHTTP(t, s, "GET", path+"/transcript?limit=129", nil, 400)
	conversationHTTP(t, s, "GET", "/v1/conversations?cursor=invalid", nil, 400)
	conversationHTTP(t, s, "GET", path+"/transcript?after=1", nil, 410)
	conversationHTTP(t, s, "GET", path+"/close", nil, 405)
	// One inventory item per opaque page; no transcript/private settings included.
	conversationHTTP(t, s, "POST", "/v1/conversations", conversationCreateRequest(), 201)
	page := decodeConversationJSON[struct {
		Items  []ConversationMetadata `json:"conversations"`
		Cursor string                 `json:"next_cursor"`
	}](t, conversationHTTP(t, s, "GET", "/v1/conversations?limit=1", nil, 200))
	if len(page.Items) != 1 || page.Cursor == "" {
		t.Fatal(page)
	}
	next := decodeConversationJSON[struct {
		Items []ConversationMetadata `json:"conversations"`
	}](t, conversationHTTP(t, s, "GET", "/v1/conversations?limit=1&cursor="+page.Cursor, nil, 200))
	if len(next.Items) != 1 || next.Items[0].ID == page.Items[0].ID {
		t.Fatal(next)
	}
	req, _ := http.NewRequestWithContext(t.Context(), "GET", s.Endpoint().URL+"/v1/conversations", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatal(res.StatusCode)
	}
	// Expired resource receipts remain safe within the ID window, then old IDs
	// remain invalid once receipts and history have been pruned.
	if err := api.Policy.Close(t.Context(), c.ID, "human", time.Now().Add(-8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := api.Policy.Prune(t.Context(), time.Now()); err != nil {
		t.Fatal(err)
	}
	conversationHTTP(t, s, "POST", "/v1/conversations", request, 410)
	conversationHTTP(t, s, "GET", path, nil, 410)
	if err := api.Policy.Prune(t.Context(), time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, err = api.Profiles.Create(t.Context(), api.Policy, request, time.Now().Add(48*time.Hour))
	if !orc.IsConversationError(err, "request_id_expired") {
		t.Fatal(err)
	}
}

func TestConversationBrokerIsolationAndFinalWatermark(t *testing.T) {
	b := newEventBroker()
	id, _, events, _ := b.subscribe()
	defer b.unsubscribe(id)
	for i := 0; i < 100; i++ {
		r := state.ConversationRecord{ConversationID: "c", Revision: uint64(i + 1), Kind: "message", Message: &state.ConversationMessage{Role: "assistant", Text: "content"}}
		b.publish(Event{Type: "conversation.message", Conversation: &r})
	}
	b.publish(Event{Type: "worker.state", Worker: "coder"})
	frames := b.checkpoint(id)
	if len(frames) != 7 || len(events) != 0 {
		t.Fatalf("unbounded conversation traffic: %d", len(frames))
	}
	for i := 0; i < 4; i++ {
		if frames[i].Conversation == nil {
			t.Fatal(frames[i])
		}
	}
	if frames[4].Type != "conversation.resync" || frames[5].Worker != "coder" || frames[5].Conversation != nil || frames[6].Type != "stream.position" || frames[6].Seq != 101 {
		t.Fatalf("missing recovery/fleet frames %+v", frames)
	}
	// Fill fleet capacity, drop the final event, then expose its position without
	// publishing anything else. Recovery never relies on a subsequent content event.
	for i := 0; i < 33; i++ {
		b.publish(Event{Type: "worker.state"})
	}
	frames = b.checkpoint(id)
	if len(frames) != 33 || frames[31].Seq != 133 || frames[32].Seq != 134 {
		t.Fatalf("dropped final event not detectable: %+v", frames)
	}
	if watermark := b.checkpoint(id); len(watermark) != 1 || watermark[0].Seq != 134 {
		t.Fatal("watermark increments sequence")
	}
}

func TestConversationHTTPRestartInterruptsAcceptedWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	policy, err := orc.NewConversations(state.NewConversationStore(dir), orc.DefaultConversationLimits())
	if err != nil {
		t.Fatal(err)
	}
	profiles := orc.NewConversationProfiles([]orc.ConversationProfile{{Name: "planner", Execution: state.ConversationExecution{Harness: "codex", Actor: "agent", MaxDuration: time.Minute}}})
	created, err := profiles.Create(t.Context(), policy, conversationCreateRequest(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request := orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "accepted before restart", Operator: "human"}
	receipt, _, err := policy.Accept(t.Context(), created.ConversationID, request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s, _ := conversationTestServer(t, dir, supervisor.ConversationProcessHooks{Run: func(context.Context, state.Conversation, state.ConversationTurn, string, func(state.ConversationRecord) error) (harness.RunResult, error) {
		t.Error("replayed abandoned turn")
		return harness.RunResult{}, nil
	}})
	path := "/v1/conversations/" + created.ConversationID
	data := conversationHTTP(t, s, "GET", path+"/transcript", nil, 200)
	if !bytes.Contains(data, []byte(`"state":"interrupted"`)) || !bytes.Contains(data, []byte("accepted before restart")) {
		t.Fatalf("restart history lost: %s", data)
	}
	recovered := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, s, "POST", path+"/turns", request, 202))
	if recovered.TurnID != receipt.TurnID || recovered.Revision != receipt.Revision {
		t.Fatal(recovered)
	}
	request.RequestID = orc.NewConversationRequestID(time.Now())
	data = conversationHTTP(t, s, "POST", path+"/turns", request, 409)
	if !bytes.Contains(data, []byte("session_unavailable")) {
		t.Fatalf("lost session replaced: %s", data)
	}
}

type conversationGatedStream struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	frames  chan []byte
	first   bool
}

func (w *conversationGatedStream) Header() http.Header { return w.header }
func (w *conversationGatedStream) WriteHeader(int)     {}
func (w *conversationGatedStream) Flush()              {}
func (w *conversationGatedStream) Write(data []byte) (int, error) {
	if !w.first {
		w.first = true
		close(w.entered)
		<-w.release
	}
	w.frames <- append([]byte(nil), data...)
	return len(data), nil
}
func TestConversationSSEWatermarkExposesDroppedFinalWithoutMoreContent(t *testing.T) {
	s, err := NewServer(Config{StateDir: t.TempDir(), EndpointKey: testEndpointKey, EventHeartbeat: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.lifecycle = ctx
	writer := &conversationGatedStream{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}), frames: make(chan []byte, 64)}
	done := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(writer.release) }) }
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("stream failed to stop")
		}
	})
	go func() {
		defer close(done)
		request, _ := http.NewRequestWithContext(ctx, "GET", "/v1/events", nil)
		s.handleEvents(writer, request)
	}()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	select {
	case <-writer.entered:
	case <-guard.C:
		t.Fatal("no sync barrier")
	}
	// The stream is subscribed but cannot drain. A final conversation event is
	// lost behind fleet traffic. No later publication is made to reveal the gap.
	for i := 0; i < 32; i++ {
		s.PublishEvent(Event{Type: "worker.state"})
	}
	s.PublishEvent(Event{Type: "conversation.turn", Conversation: &state.ConversationRecord{ConversationID: "c", Revision: 1, Kind: "turn", Turn: &state.ConversationTurn{ID: "t", State: "completed"}}})
	unblock()
	last := uint64(0)
	for {
		select {
		case frame := <-writer.frames:
			for _, line := range strings.Split(string(frame), "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				event := decodeConversationJSON[Event](t, []byte(strings.TrimPrefix(line, "data: ")))
				if event.Type == "stream.position" {
					if last != 32 || event.Seq != 33 {
						t.Fatalf("missing final-event gap: last=%d watermark=%+v", last, event)
					}
					return
				}
				if event.Seq < last {
					t.Fatal("watermark overtook queued records")
				}
				last = event.Seq
			}
		case <-guard.C:
			t.Fatal("no final position watermark")
		}
	}
}

func TestConversationHTTPConcurrentDuplicateDuringPreflight(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		for _, conflict := range []bool{false, true} {
			t.Run(fmt.Sprintf("invalid=%t/conflict=%t", invalid, conflict), func(t *testing.T) {
				entered, release, started, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				var validations, runs atomic.Int32
				s, api := conversationTestServer(t, t.TempDir(), supervisor.ConversationProcessHooks{
					Validate: func(ctx context.Context, _ state.Conversation) error {
						if validations.Add(1) == 1 {
							close(entered)
							select {
							case <-release:
							case <-ctx.Done():
								return ctx.Err()
							}
							if invalid {
								return &orc.ConversationError{Code: "profile_unavailable"}
							}
						}
						return nil
					},
					Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
						runs.Add(1)
						close(started)
						<-ctx.Done()
						return harness.RunResult{}, ctx.Err()
					},
				})
				c := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "POST", "/v1/conversations", conversationCreateRequest(), 201))
				path := "/v1/conversations/" + c.ID + "/turns"
				request := orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "plan", Operator: "human"}
				body, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(func() {
					unblock()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("HTTP submission did not stop")
					}
				})
				var firstBody []byte
				var firstStatus int
				var firstErr error
				go func() {
					defer close(done)
					req, err := http.NewRequestWithContext(ctx, "POST", s.Endpoint().CapabilityURL()+path, bytes.NewReader(body))
					if err != nil {
						firstErr = err
						return
					}
					req.Header.Set("Content-Type", "application/json")
					res, err := http.DefaultClient.Do(req)
					if err != nil {
						firstErr = err
						return
					}
					defer res.Body.Close()
					firstStatus = res.StatusCode
					firstBody, firstErr = io.ReadAll(res.Body)
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("preflight did not start")
				}
				secondRequest := request
				if conflict {
					secondRequest.Text = "different accepted payload"
				}
				accepted := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, s, "POST", path, secondRequest, 202))
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("accepted turn did not launch")
				}
				api.Manager.BlockExecution()
				unblock()
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("first HTTP submission did not finish")
				}
				if firstErr != nil {
					t.Fatal(firstErr)
				}
				if conflict {
					if firstStatus != 409 || !bytes.Contains(firstBody, []byte(`"code":"request_id_conflict"`)) {
						t.Fatalf("conflict hidden: %d %s", firstStatus, firstBody)
					}
				} else {
					if firstStatus != 202 {
						t.Fatalf("accepted duplicate reported unaccepted: %d %s", firstStatus, firstBody)
					}
					original := decodeConversationJSON[conversationReceipt](t, firstBody)
					if original != accepted {
						t.Fatalf("receipt changed: %+v want %+v", original, accepted)
					}
				}
				if runs.Load() != 1 {
					t.Fatalf("executed %d times", runs.Load())
				}
			})
		}
	}
}

func TestConversationHTTPCloseKeepsCommittedOutcome(t *testing.T) {
	for _, failure := range []string{"expired_readback", "failed_readback", "typed_cleanup", "private_cleanup"} {
		t.Run(failure, func(t *testing.T) {
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			limits := orc.DefaultConversationLimits()
			limits.ClosedAge = time.Nanosecond
			s, api := conversationTestServer(t, t.TempDir(), supervisor.ConversationProcessHooks{Cleanup: func(ctx context.Context, c state.Conversation) error {
				if c.State != "closed" {
					return fmt.Errorf("cleanup saw uncommitted state")
				}
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				switch failure {
				case "typed_cleanup":
					return &orc.ConversationError{Code: "session_unavailable"}
				case "private_cleanup":
					return fmt.Errorf("private provider path /secret/credential")
				}
				return nil
			}}, limits)
			c := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "POST", "/v1/conversations", conversationCreateRequest(), 201))
			rejected := conversationHTTP(t, s, "POST", "/v1/conversations/"+c.ID+"/close", map[string]string{"operator": ""}, 400)
			if !bytes.Contains(rejected, []byte(`"mutation_applied":false`)) {
				t.Fatalf("pre-close rejection misclassified: %s", rejected)
			}
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("close request did not finish")
				}
			})
			responseWriter := httptest.NewRecorder()
			request := httptest.NewRequest("POST", s.Endpoint().CapabilityURL()+"/v1/conversations/"+c.ID+"/close", strings.NewReader(`{"operator":"human"}`))
			request.Header.Set("Content-Type", "application/json")
			status := 503
			if failure == "expired_readback" {
				status = 410
			}
			if failure == "typed_cleanup" {
				status = 409
			}
			go func() {
				defer close(done)
				s.handler().ServeHTTP(responseWriter, request)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("close did not reach cleanup")
			}
			closed, err := api.Policy.Get(t.Context(), c.ID)
			if err != nil || closed.State != "closed" {
				t.Fatalf("closure not committed: %+v %v", closed, err)
			}
			if failure == "expired_readback" {
				if err := api.Policy.Prune(t.Context(), closed.ClosedAt.Add(api.Policy.Limits.ClosedAge)); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "failed_readback" {
				blockConversationStateReads(t, s.config.StateDir)
			}
			unblock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("close did not finish")
			}
			response := responseWriter.Body.Bytes()
			if responseWriter.Code != status {
				t.Fatalf("close response: %d want %d: %s", responseWriter.Code, status, response)
			}
			var result struct {
				Error struct {
					Code    string `json:"code"`
					Applied *bool  `json:"mutation_applied"`
					Unknown bool   `json:"outcome_unknown"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response, &result); err != nil {
				t.Fatal(err)
			}
			if result.Error.Applied == nil || !*result.Error.Applied || result.Error.Unknown {
				t.Fatalf("committed close reported unaccepted or unknown: %s", response)
			}
			if bytes.Contains(response, []byte("/secret/credential")) {
				t.Fatalf("private cleanup error leaked: %s", response)
			}
		})
	}
}

// Fail actual store reads without replacing the store or adding runtime hooks.
// Each fixture owns its directory; restore before lifecycle cleanup.
func blockConversationStateReads(t *testing.T, dir string) func() {
	t.Helper()
	path := filepath.Join(dir, "conversations.json")
	saved := filepath.Join(dir, "saved-conversations.json")
	if err := os.Rename(path, saved); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	restore := func() {
		once.Do(func() {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				t.Error(err)
			}
			if err := os.Rename(saved, path); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(restore)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	return restore
}

func TestConversationHTTPCancelKeepsCommittedIntentOnStopFailure(t *testing.T) {
	started, stopping, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	s, api := conversationTestServer(t, t.TempDir(), supervisor.ConversationProcessHooks{Run: func(ctx context.Context, _ state.Conversation, _ state.ConversationTurn, _ string, _ func(state.ConversationRecord) error) (harness.RunResult, error) {
		close(started)
		<-ctx.Done()
		close(stopping)
		<-release
		return harness.RunResult{}, ctx.Err()
	}})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c := decodeConversationJSON[ConversationMetadata](t, conversationHTTP(t, s, "POST", "/v1/conversations", conversationCreateRequest(), 201))
	receipt := decodeConversationJSON[conversationReceipt](t, conversationHTTP(t, s, "POST", "/v1/conversations/"+c.ID+"/turns", orc.ConversationSubmit{RequestID: orc.NewConversationRequestID(time.Now()), Text: "plan", Operator: "human"}, 202))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("turn did not start")
	}
	path := "/v1/conversations/" + c.ID + "/turns/" + receipt.TurnID + "/cancel"
	rejected := conversationHTTP(t, s, "POST", path, map[string]string{"operator": ""}, 400)
	if !bytes.Contains(rejected, []byte(`"mutation_applied":false`)) {
		t.Fatalf("pre-intent rejection misclassified: %s", rejected)
	}
	writer := httptest.NewRecorder()
	request := httptest.NewRequest("POST", s.Endpoint().CapabilityURL()+path, strings.NewReader(`{"operator":"human"}`))
	request.Header.Set("Content-Type", "application/json")
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("cancel request did not finish")
		}
	})
	go func() { defer close(done); s.handler().ServeHTTP(writer, request) }()
	select {
	case <-stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel intent did not reach child")
	}
	pending, err := api.Policy.Get(t.Context(), c.ID)
	if err != nil || !pending.Turns[0].CancelRequested {
		t.Fatalf("intent missing before failed reconciliation: %+v %v", pending, err)
	}
	restore := blockConversationStateReads(t, s.config.StateDir)
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel request did not finish")
	}
	if writer.Code != 503 || !bytes.Contains(writer.Body.Bytes(), []byte(`"mutation_applied":true`)) || bytes.Contains(writer.Body.Bytes(), []byte(s.config.StateDir)) {
		t.Fatalf("committed cancel intent lost or private path leaked: %d %s", writer.Code, writer.Body.Bytes())
	}
	restore()
	pending, err = api.Policy.Get(t.Context(), c.ID)
	if err != nil || !pending.Turns[0].CancelRequested {
		t.Fatalf("intent was lost: %+v %v", pending, err)
	}
	if err := api.Manager.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
}
