package ticketclient

import (
	"context"
	"errors"
	"testing"
)

func TestNextAndClaimReviewUsesAtomicFilteredSelection(t *testing.T) {
	t.Setenv(helperScenarioEnv, "next-filters")
	client := startHelperClient(t, "worker")
	defer client.Close()
	item, err := client.NextAndClaimReview(context.Background(), QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"trivial"}})
	if err != nil || item == nil || item.ID != testID || item.Assignee != "worker" || item.State != "review" {
		t.Fatalf("next review = %#v, %v", item, err)
	}
}

func TestNextAndClaimReviewReturnsEmptyWithoutWaitingOrRetrying(t *testing.T) {
	t.Setenv(helperScenarioEnv, "review-filter-empty-then")
	client := startHelperClient(t, "worker")
	defer client.Close()
	filters := QueueFilters{WithoutTags: []string{"trivial"}}
	item, err := client.NextAndClaimReview(context.Background(), filters)
	if err != nil || item != nil {
		t.Fatalf("empty next review = %#v, %v", item, err)
	}
	item, err = client.NextAndClaimReview(context.Background(), filters)
	if err != nil || item == nil || item.ID != testID {
		t.Fatalf("second next review = %#v, %v", item, err)
	}
}

func TestNextAndClaimReviewPreservesOwnershipAndProtocolChecks(t *testing.T) {
	for _, response := range []string{
		`{"item":{"id":"bad","state":"review","assignee":"worker"}}`,
		`{"item":{"id":"20260919-12345","state":"open","assignee":"worker"}}`,
		`{"item":{"id":"20260919-12345","state":"review","assignee":"other"}}`,
	} {
		t.Run(response, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "single-response")
			t.Setenv(helperResponseEnv, response)
			client := startHelperClient(t, "worker")
			defer client.Close()
			if _, err := client.NextAndClaimReview(context.Background(), QueueFilters{}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error = %v, want protocol failure", err)
			}
		})
	}
}

func TestNextAndClaimReviewVerifiesActorQueueConflict(t *testing.T) {
	t.Setenv(helperScenarioEnv, "queue-conflict")
	t.Setenv("TICKETCLIENT_HELPER_CONFLICT_STATE", "open")
	client := startHelperClient(t, "worker")
	defer client.Close()
	_, err := client.NextAndClaimReview(context.Background(), QueueFilters{})
	conflict := ActorQueueConflict(err, "review")
	if conflict == nil || conflict.TicketID != testID || conflict.ConflictingQueue != "open" {
		t.Fatalf("conflict=%#v err=%v", conflict, err)
	}
}
