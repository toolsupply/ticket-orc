package supervisor

import (
	"context"
	"io"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
)

// ConversationHarnessHooks reuses the provider's session and live-output
// contract. The factory supplies the managed environment and process containment.
// Recovery deliberately remains fail-closed when a daemon crash left a running
// intent without positive stop evidence; no remembered PID is ever signalled.
func ConversationHarnessHooks(stateDir string, factory func(state.Conversation) (harness.Harness, error)) ConversationProcessHooks {
	return ConversationProcessHooks{
		Run: func(ctx context.Context, c state.Conversation, t state.ConversationTurn, text string, emit func(state.ConversationRecord) error) (harness.RunResult, error) {
			agent, err := factory(c)
			if err != nil {
				return harness.RunResult{}, err
			}
			events := make(chan harness.OutputEvent, harness.MaxOutputBuffer)
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			outputDone := make(chan error, 1)
			go func() {
				var first error
				for event := range events {
					if first != nil {
						continue
					}
					record := state.ConversationRecord{Kind: event.Kind}
					switch event.Kind {
					case "message":
						record.Message = &state.ConversationMessage{ID: t.ID + ":" + event.ItemID, Role: "assistant", Text: event.Text}
					case "activity":
						record.Activity = &state.ConversationActivity{Name: event.ToolName, State: event.Phase}
					}
					if err := emit(record); err != nil {
						first = err
						cancel()
					}
				}
				outputDone <- first
			}()
			e := c.Execution
			// TicketID is only the raw-log key here; no Ticket assignment or queue is used.
			req := harness.RunRequest{WorkerName: c.ID, TicketID: c.ID, Role: "conversation", WorkingDir: e.WorkingDir, Workspace: e.Workspace, Actor: e.Actor, Model: e.Model, Reasoning: e.Reasoning, CodexSandbox: e.CodexSandbox, CodexServiceTier: e.CodexServiceTier, StateDir: stateDir, OutputMode: "quiet", Operator: io.Discard, Events: events, RequireSession: true, Prompt: orc.ConversationPrompt(c, text)}
			var result harness.RunResult
			if c.SessionID == "" {
				result, err = agent.Run(runCtx, req)
			} else {
				result, err = agent.Resume(runCtx, c.SessionID, req)
			}
			close(events)
			outputErr := <-outputDone
			if outputErr != nil {
				return result, outputErr
			}
			return result, err
		},
		Cleanup: func(ctx context.Context, c state.Conversation) error {
			if c.SessionID == "" {
				return nil
			}
			agent, err := factory(c)
			if err != nil {
				return err
			}
			return agent.Cleanup(ctx, c.SessionID, harness.CleanupKeep)
		},
	}
}
