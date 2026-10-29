package ticketclient

import (
	"context"
	"errors"
	"testing"
)

func TestEscalateHoldGuardAndEvidence(t *testing.T) {
	for _, mode := range []string{"owned", "unassigned", "other owner", "owner race", "truncated", "ambiguous", "uncertain", "observation failed"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "hold-escalation")
			t.Setenv(helperResponseEnv, mode)
			c := startHelperClient(t, "triage")
			defer c.Close()
			owner := "triage"
			if mode == "unassigned" {
				owner = ""
			}
			item, err := c.EscalateHold(context.Background(), testID, owner, "Operator action")
			switch mode {
			case "owned", "unassigned":
				if err != nil || item.State != "hold" || item.Assignee != "user" {
					t.Fatalf("handoff: %+v %v", item, err)
				}
			case "other owner", "owner race":
				if err != nil || item.Assignee != "other" {
					t.Fatalf("changed owner: %+v %v", item, err)
				}
			case "truncated", "ambiguous":
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("ambiguous evidence: %+v %v", item, err)
				}
			case "uncertain":
				if !MutationApplied(err) {
					t.Fatalf("lost uncertainty: %v", err)
				}
			default:
				if err == nil {
					t.Fatal("observation failure ignored")
				}
			}
		})
	}
}
