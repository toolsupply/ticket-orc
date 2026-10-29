package ticketclient

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestHoldFrontierUsesFilteredPageAndAuthoritativeShow(t *testing.T) {
	t.Setenv(helperScenarioEnv, "hold-frontier")
	client := startHelperClient(t, "triage")
	defer client.Close()
	result, err := client.HoldFrontier(context.Background(), QueueFilters{
		Tags: []string{"urgent", "backend"}, WithoutTags: []string{"draft", "blocked"},
	}, 2, 4)
	if err != nil || len(result.Items) != 2 || !result.More {
		t.Fatalf("HoldFrontier = %#v, %v", result, err)
	}
	for _, item := range result.Items {
		if item.State != "hold" || item.Assignee != "" || item.Title != "rechecked" {
			t.Fatalf("hold frontier did not recheck exact ID: %#v", item)
		}
	}
}

func TestHoldFrontierOmitsTargetsThatChangedBeforeShow(t *testing.T) {
	for _, response := range []string{
		`{"state":"open"}`, `{"state":"hold","assignee":"other"}`, `{"state":"hold","assignee":"user"}`,
	} {
		t.Run(response, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "hold-frontier")
			t.Setenv(helperResponseEnv, response)
			client := startHelperClient(t, "triage")
			defer client.Close()
			result, err := client.HoldFrontier(context.Background(), QueueFilters{
				Tags: []string{"backend", "urgent"}, WithoutTags: []string{"blocked", "draft"},
			}, 2, 4)
			if err != nil || len(result.Items) != 0 || !result.More {
				t.Fatalf("changed hold frontier = %#v, %v; want empty rechecked page preserving more", result, err)
			}
		})
	}
}

func TestHoldFrontierRejectsMalformedAndInconsistentObservations(t *testing.T) {
	for _, response := range []string{
		`{"items":[{"id":"20260919-12345","state":"review"}],"more":false}`,
		`{"items":[{"id":"20260919-12345","state":"hold","assignee":"user"}],"more":false}`,
		`{"items":[{"id":"short","state":"hold"}],"more":false}`,
		`{"items":[{"state":"hold"}],"more":false}`,
		`{"items":[{"id":"20260919-12345"}],"more":false}`,
		`{"items":[{"id":"20260919-12345","state":"hold","assignee":null}],"more":false}`,
		`{"items":[{"id":"20260919-12345","state":"hold"},{"id":"20260919-12345","state":"hold"}],"more":false}`,
		`{"items":[null],"more":false}`,
		`{"items":null,"more":false}`, `{"items":[],"more":"yes"}`, `{"more":false}`, `{"items":[]}`,
		`{"items":[{},{},{}],"more":false}`,
	} {
		t.Run(response, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "single-response")
			t.Setenv(helperResponseEnv, response)
			client := startHelperClient(t, "triage")
			defer client.Close()
			if _, err := client.HoldFrontier(context.Background(), QueueFilters{}, 2, 0); !errors.Is(err, ErrProtocol) {
				t.Fatalf("HoldFrontier error = %v, want protocol error", err)
			}
		})
	}
}

func TestHoldFrontierRejectsMalformedShowRechecks(t *testing.T) {
	for _, response := range []string{
		`{"id":"20260919-99999"}`, `{"state":null}`, `{"assignee":null}`, `{"assignee":7}`, `null`,
	} {
		t.Run(response, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "hold-frontier")
			t.Setenv(helperResponseEnv, response)
			client := startHelperClient(t, "triage")
			defer client.Close()
			if _, err := client.HoldFrontier(context.Background(), QueueFilters{
				Tags: []string{"backend", "urgent"}, WithoutTags: []string{"blocked", "draft"},
			}, 2, 4); !errors.Is(err, ErrProtocol) {
				t.Fatalf("HoldFrontier Show error = %v, want protocol error", err)
			}
		})
	}
}

func TestHoldFrontierBounds(t *testing.T) {
	client := startHelperClient(t, "triage")
	defer client.Close()
	for _, test := range []struct{ limit, offset int }{{0, 0}, {21, 0}, {1, -1}} {
		if _, err := client.HoldFrontier(context.Background(), QueueFilters{}, test.limit, test.offset); err == nil || !strings.Contains(err.Error(), "ticket hold frontier") {
			t.Fatalf("HoldFrontier(%d, %d) error = %v", test.limit, test.offset, err)
		}
	}
}
