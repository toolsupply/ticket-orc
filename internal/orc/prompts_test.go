package orc

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
)

func TestAcceptancePromptDefinesAtomicHumanHandoff(t *testing.T) {
	const id = "20261007-12345"
	got := AcceptancePrompt(id)
	for _, want := range []string{"ticket query -j id:" + id + " state:signoff unclaimed :: reassign --to user", "assignee=user", "changed=false", "mutation_applied=true", "Do not process a second signoff ticket"} {
		if !strings.Contains(got, want) {
			t.Errorf("acceptance prompt %q missing %q", got, want)
		}
	}
	for _, unwanted := range []string{"@user", "leave it in signoff and explain"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("acceptance prompt retains obsolete guidance %q", unwanted)
		}
	}
}

func TestExpandTicketPromptSubstitutesSupportedFields(t *testing.T) {
	template := "ticket={{ticket}} completion={{review_completion}};  keep  "
	got, err := ExpandTicketPrompt(template, "20260919-12345", "reviewer", "close")
	if err != nil {
		t.Fatalf("ExpandTicketPrompt: %v", err)
	}
	want := "ticket=20260919-12345 completion=close;  keep  "
	if got != want {
		t.Fatalf("expanded prompt = %q, want %q", got, want)
	}
}

func TestValidateTicketPromptRejectsUnsafeTemplates(t *testing.T) {
	for _, test := range []struct {
		name string
		role string
		text string
		want string
	}{
		{"blank", "coder", " \t\n ", "whitespace-only"},
		{"missing ticket", "coder", "work without an ID", "include {{ticket}}"},
		{"unknown", "coder", "work {{ticket}} {{repo}}", "unknown placeholder"},
		{"actor placeholder removed", "coder", "work {{ticket}} {{actor}}", "unknown placeholder"},
		{"role placeholder removed", "reviewer", "work {{ticket}} {{role}}", "unknown placeholder"},
		{"reviewer-only placeholder", "coder", "work {{ticket}} {{review_completion}}", "unknown placeholder"},
		{"unterminated", "reviewer", "work {{ticket", "unterminated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateTicketPrompt(test.text, test.role)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestExpandTicketPromptRequiresTicketIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		ticket string
		want   string
	}{
		{"ticket", "", "ticket identity"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExpandTicketPrompt("work {{ticket}}", test.ticket, "coder", "")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCoderAndReviewerTurnsUseResolvedTicketPrompts(t *testing.T) {
	coderAgent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "coder-thread"}}
	coderConfig := coderConfig()
	coderConfig.TicketPrompt = "C={{ticket}}"
	if _, err := runCoderTurn(context.Background(), coderConfig, "20260919-11111", coderAgent, newFakeCoderState()); err != nil {
		t.Fatalf("runCoderTurn: %v", err)
	}
	if got, want := coderAgent.runs[0].Prompt, "C=20260919-11111"; got != want {
		t.Fatalf("coder prompt = %q, want %q", got, want)
	}
	if !strings.Contains(coderAgent.runs[0].Prompt, "20260919-11111") || strings.Contains(coderAgent.runs[0].Prompt, "Orc-steered dynamic work turn") {
		t.Fatalf("managed coder prompt lost exact-ticket scope or acquired dynamic-turn guidance: %q", coderAgent.runs[0].Prompt)
	}

	reviewerAgent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "reviewer-thread"}}
	reviewerConfig := reviewerConfig()
	reviewerConfig.Operator = io.Discard
	reviewerConfig.TicketPrompt = "R={{ticket}}/{{review_completion}}"
	reviewerConfig.ReviewCompletion = "close"
	if _, err := runReviewerTurn(context.Background(), reviewerConfig, "20260919-22222", reviewerAgent, newFakeCoderState()); err != nil {
		t.Fatalf("runReviewerTurn: %v", err)
	}
	if got, want := reviewerAgent.runs[0].Prompt, "R=20260919-22222/close"; got != want {
		t.Fatalf("reviewer prompt = %q, want %q", got, want)
	}
	if !strings.Contains(reviewerAgent.runs[0].Prompt, "20260919-22222") || strings.Contains(reviewerAgent.runs[0].Prompt, "Orc-steered dynamic work turn") {
		t.Fatalf("managed reviewer prompt lost exact-ticket scope or acquired dynamic-turn guidance: %q", reviewerAgent.runs[0].Prompt)
	}
}

func TestManagedTurnsKeepExactTicketLifecyclePrompts(t *testing.T) {
	coderAgent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "managed-coder"}}
	coder := coderConfig()
	coder.TicketPrompt = ""
	if _, err := runCoderTurn(context.Background(), coder, "20260919-33333", coderAgent, newFakeCoderState()); err != nil {
		t.Fatalf("run managed coder turn: %v", err)
	}
	for _, want := range []string{"Work only ticket 20260919-33333", "submit it to review, and exit", "Do not select or work another ticket"} {
		if !strings.Contains(coderAgent.runs[0].Prompt, want) {
			t.Errorf("managed coder prompt %q omitted %q", coderAgent.runs[0].Prompt, want)
		}
	}

	reviewerAgent := &fakeCoderHarness{runRes: harness.RunResult{SessionID: "managed-reviewer"}}
	reviewer := reviewerConfig()
	reviewer.Operator = io.Discard
	reviewer.TicketPrompt = ""
	reviewer.ReviewCompletion = "signoff"
	if _, err := runReviewerTurn(context.Background(), reviewer, "20260919-44444", reviewerAgent, newFakeCoderState()); err != nil {
		t.Fatalf("run managed reviewer turn: %v", err)
	}
	for _, want := range []string{"Review only ticket 20260919-44444", "assigned to you", "review_completion=signoff", "Exit after the transition"} {
		if !strings.Contains(reviewerAgent.runs[0].Prompt, want) {
			t.Errorf("managed reviewer prompt %q omitted %q", reviewerAgent.runs[0].Prompt, want)
		}
	}
	if strings.Contains(coderAgent.runs[0].Prompt, "Orc-steered dynamic work turn") || strings.Contains(reviewerAgent.runs[0].Prompt, "Orc-steered dynamic work turn") {
		t.Fatal("dynamic turn boundary leaked into a managed exact-ticket prompt")
	}
}

func TestReviewerPromptRequiresExactOwnedTicket(t *testing.T) {
	prompt := ReviewerPrompt("20260922-12345")
	for _, want := range []string{"20260922-12345", "assigned to you", "in review", "review_completion=signoff", "signoff", "human acceptance", "return it to open", "specific findings"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("reviewer prompt %q does not contain %q", prompt, want)
		}
	}
	for _, unwanted := range []string{"reviewer", "actor", "--config", "--scope", "configured Ticket target", "{{role}}"} {
		if strings.Contains(prompt, unwanted) {
			t.Errorf("reviewer prompt %q contains routing or identity text %q", prompt, unwanted)
		}
	}
	if len(prompt) > 720 {
		t.Fatalf("reviewer prompt length = %d, want at most 720", len(prompt))
	}
}

func TestManagedPromptsAllowOwnedBlockedOutcome(t *testing.T) {
	for _, prompt := range []string{CoderPrompt("20261017-01957"), ReviewerPrompt("20261017-01957")} {
		for _, want := range []string{"Ticket Skill", "concrete blocker", "action needed to resume", "Handoff", "hold this ticket while you still own it", "verify hold", "Do not release it first", "Do not select or work another ticket"} {
			if !strings.Contains(prompt, want) {
				t.Errorf("prompt %q missing %q", prompt, want)
			}
		}
	}
}
