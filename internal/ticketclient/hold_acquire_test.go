package ticketclient

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Separate persistent processes share one native Ticket repository. Both
// observers finish before either acquisition runs. A launch/wake is authorized
// only by the new atomic receipt, regardless of the caller's execution mode.
func TestAcquireHoldNativeExclusive(t *testing.T) {
	for _, modes := range []string{"managed-managed", "managed-steered"} {
		t.Run(modes, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			root := t.TempDir()
			target := Target{Repository: root}
			cmd := exec.CommandContext(ctx, "ticket", "init", "-j")
			cmd.Dir, cmd.Env = root, ApplyTargetEnvironment(os.Environ(), "first", target)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("init: %s %v", out, err)
			}
			clients := make([]*Client, 2)
			for i, actor := range []string{"first", "second"} {
				c, err := NewWithWorkingDirAndTarget(actor, root, target)
				if err != nil {
					t.Fatal(err)
				}
				clients[i] = c
				t.Cleanup(func() { c.Close() })
			}
			hold := "hold"
			made, err := clients[0].CreateTicket(ctx, CreateInput{Title: "exclusive assessment", State: &hold, Sections: map[string]string{"objective": "Assess once"}})
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range clients {
				observed, err := c.Show(ctx, made.ID)
				if err != nil || observed.Assignee != "" || observed.State != "hold" {
					t.Fatalf("observation: %+v %v", observed, err)
				}
			}
			type result struct {
				item *Ticket
				err  error
			}
			results := make(chan result, 2)
			gate := make(chan struct{})
			for _, c := range clients {
				go func() { <-gate; item, err := c.AcquireHold(ctx, made.ID, QueueFilters{}); results <- result{item, err} }()
			}
			close(gate)
			launches := 0
			owner := ""
			for range clients {
				r := <-results
				if r.err != nil {
					t.Fatal(r.err)
				}
				if r.item != nil {
					launches++
					owner = r.item.Assignee
				}
			}
			if launches != 1 {
				t.Fatalf("execution authorizations=%d", launches)
			}
			for _, c := range clients {
				item, err := c.AcquireHold(ctx, made.ID, QueueFilters{})
				if err != nil || item != nil {
					t.Fatalf("preassigned/stale assignment replay: %+v %v", item, err)
				}
				observed, err := c.Show(ctx, made.ID)
				if err != nil || observed.Assignee != owner {
					t.Fatalf("loser changed owner: %+v %v", observed, err)
				}
			}
			for _, precondition := range []string{"open", "user", "other", "filters"} {
				input := CreateInput{Title: "ineligible " + precondition, State: &hold, Sections: map[string]string{"objective": "Preserve state and ownership"}}
				if precondition == "open" {
					input.State = nil
				}
				if precondition == "user" || precondition == "other" {
					owner := precondition
					input.Assignee = &owner
				}
				created, err := clients[0].CreateTicket(ctx, input)
				if err != nil {
					t.Fatal(err)
				}
				filters := QueueFilters{}
				if precondition == "filters" {
					filters.Tags = []string{"missing"}
				}
				item, err := clients[1].AcquireHold(ctx, created.ID, filters)
				if err != nil || item != nil {
					t.Fatalf("%s acquired: %+v %v", precondition, item, err)
				}
				current, err := clients[0].Show(ctx, created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if precondition == "open" && current.State != "open" || (precondition == "user" || precondition == "other") && current.Assignee != precondition || precondition == "filters" && current.Assignee != "" {
					t.Fatalf("precondition changed: %+v", current)
				}
			}
		})
	}
}

func TestAcquireHoldRejectsAmbiguousReceipts(t *testing.T) {
	for _, response := range []string{
		`{"id":"20260919-12345","state":"hold","assignee":"triage","from_assignee":"","changed":false}`,
		`{"id":"20260919-12345","state":"open","assignee":"triage","from_assignee":"","changed":true}`,
		`{"id":"20260919-12345","state":"hold","assignee":"other","from_assignee":"","changed":true}`,
		`{"id":"20260919-12345","state":"hold","assignee":"triage","from_assignee":"triage","changed":true}`,
		`{"id":"20260919-12345","state":"hold","assignee":"triage","changed":true}`,
	} {
		t.Run(response, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "single-response")
			t.Setenv(helperResponseEnv, response)
			c := startHelperClient(t, "triage")
			defer c.Close()
			item, err := c.AcquireHold(context.Background(), "20260919-12345", QueueFilters{})
			if item != nil || !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "operator reconciliation") {
				t.Fatalf("acquisition=%+v %v", item, err)
			}
		})
	}
}

func TestAcquireHoldMutationAppliedIsNotExecutionAuthority(t *testing.T) {
	t.Setenv(helperScenarioEnv, "single-response")
	t.Setenv(helperResponseEnv, `{"error":{"code":"scm_failed","message":"persist failed","details":{"mutation_applied":true}}}`)
	c := startHelperClient(t, "triage")
	defer c.Close()
	item, err := c.AcquireHold(context.Background(), "20260919-12345", QueueFilters{})
	if item != nil || !MutationApplied(err) || !strings.Contains(err.Error(), "operator reconciliation") {
		t.Fatalf("uncertain acquisition: %+v %v", item, err)
	}
}

func TestEscalateHoldNativePreservesContextAndExcludesReassessment(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	target := Target{Repository: root}
	cmd := exec.CommandContext(ctx, "ticket", "init", "-j")
	cmd.Dir, cmd.Env = root, ApplyTargetEnvironment(os.Environ(), "assessor", target)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init: %s %v", out, err)
	}
	for _, owner := range []string{"", "assessor"} {
		c, err := NewWithWorkingDirAndTarget("assessor", root, target)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		hold := "hold"
		var assignee *string
		if owner != "" {
			assignee = &owner
		}
		made, err := c.CreateTicket(ctx, CreateInput{Title: "unresolved assessment", State: &hold, Assignee: assignee, Sections: map[string]string{"objective": "Assess blocker", "handoff": "Existing blocker: user must choose the storage owner."}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.EscalateHold(ctx, made.ID, owner, "Automated assessment unresolved. User must decide the storage owner, then reopen.")
		if err != nil || got.State != "hold" || got.Assignee != "user" {
			t.Fatalf("escalation: %+v %v", got, err)
		}
		detail, err := c.ShowFullBody(ctx, made.ID)
		if err != nil || !strings.Contains(detail.Body, "Existing blocker:") || !strings.Contains(detail.Body, "User must decide") {
			t.Fatalf("lost context: %+v %v", detail, err)
		}
		candidate, err := c.AcquireHold(ctx, made.ID, QueueFilters{})
		if err != nil || candidate != nil {
			t.Fatalf("reassessment: %+v %v", candidate, err)
		}
	}
}
