package ticketclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testID                  = "20260919-12345"
	helperScenarioEnv       = "TICKETCLIENT_HELPER_SCENARIO"
	helperReadyFileEnv      = "TICKETCLIENT_HELPER_READY_FILE"
	helperResponseEnv       = "TICKETCLIENT_HELPER_RESPONSE"
	helperExpectedConfigEnv = "TICKETCLIENT_HELPER_EXPECT_CONFIG"
	helperExpectedScopeEnv  = "TICKETCLIENT_HELPER_EXPECT_SCOPE"
	helperProbeEnv          = "TICKETCLIENT_HELPER_PROBE"
	helperProbeExitEnv      = "TICKETCLIENT_HELPER_PROBE_EXIT"
	helperProbeMalformedEnv = "TICKETCLIENT_HELPER_PROBE_MALFORMED"
	helperProbeErrorEnv     = "TICKETCLIENT_HELPER_PROBE_ERROR"
	versionHelperEnv        = "TICKETCLIENT_VERSION_HELPER"
	versionOutputEnv        = "TICKETCLIENT_VERSION_OUTPUT"
	versionStderrEnv        = "TICKETCLIENT_VERSION_STDERR"
	versionExitEnv          = "TICKETCLIENT_VERSION_EXIT"
	versionCountFileEnv     = "TICKETCLIENT_VERSION_COUNT_FILE"
	versionRepeatEnv        = "TICKETCLIENT_VERSION_REPEAT"
)

func TestMain(m *testing.M) {
	if os.Getenv(versionHelperEnv) != "" {
		if path := os.Getenv(versionCountFileEnv); path != "" {
			data, _ := os.ReadFile(path)
			_ = os.WriteFile(path, []byte(strings.TrimSpace(string(data))+"x"), 0o600)
		}
		versionOutput := os.Getenv(versionOutputEnv)
		if repeat, err := strconv.Atoi(os.Getenv(versionRepeatEnv)); err == nil && repeat > 0 {
			versionOutput = strings.Repeat("x", repeat)
		}
		_, _ = io.WriteString(os.Stdout, versionOutput)
		_, _ = io.WriteString(os.Stderr, os.Getenv(versionStderrEnv))
		if code := os.Getenv(versionExitEnv); code != "" {
			if exit, err := strconv.Atoi(code); err == nil {
				os.Exit(exit)
			}
		}
		os.Exit(0)
	}
	if scenario := os.Getenv(helperScenarioEnv); scenario != "" {
		os.Exit(runTicketHelper(scenario))
	}
	os.Exit(m.Run())
}

func TestPersistentClientRunsSequentialCommandsBeforeEOF(t *testing.T) {
	t.Setenv(helperScenarioEnv, "sequence")
	t.Setenv("TICKET_ACTOR", "old-actor")
	t.Setenv("TICKET_REPOSITORY", "inherited-repository")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx := context.Background()

	implementation, err := client.WaitAndClaimImplementation(ctx, QueueFilters{})
	if err != nil || implementation.ID != "20260919-10001" {
		t.Fatalf("WaitAndClaimImplementation = %#v, %v", implementation, err)
	}
	review, err := client.WaitAndClaimReview(ctx, QueueFilters{})
	if err != nil || review.ID != "20260919-10002" {
		t.Fatalf("WaitAndClaimReview = %#v, %v", review, err)
	}
	shown, err := client.Show(ctx, testID)
	if err != nil || shown.Title != "worker|inherited-repository" {
		t.Fatalf("Show = %#v, %v", shown, err)
	}
	released, err := client.Release(ctx, testID)
	if err != nil || !released.Changed {
		t.Fatalf("Release = %#v, %v", released, err)
	}

	select {
	case <-client.done:
		t.Fatal("persistent child exited after a response")
	default:
	}
}

func TestReviewQueueFiltersConfiguredTags(t *testing.T) {
	t.Setenv(helperScenarioEnv, "review-filter")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ticket, err := client.WaitAndClaimReview(context.Background(), QueueFilters{WithoutTags: []string{"trivial", "no-review", "trivial"}})
	if err != nil || ticket.ID != testID {
		t.Fatalf("WaitAndClaimReviewWithoutTags = %#v, %v", ticket, err)
	}
}

func TestQueueFiltersReachReadyAndAtomicClaimCommands(t *testing.T) {
	t.Run("ready uses repeated required and excluded flags", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "ready-filters")
		client := startHelperClient(t, "worker")
		defer client.Close()
		_, err := client.ReadyFrontier(context.Background(), "review", QueueFilters{
			Tags: []string{"urgent", "backend"}, WithoutTags: []string{"skip"},
		}, 2)
		if err != nil {
			t.Fatalf("ReadyFrontier: %v", err)
		}
	})
	t.Run("positive-only wait uses Ticket wait", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "wait-tags")
		client := startHelperClient(t, "worker")
		defer client.Close()
		if _, err := client.WaitAndClaimImplementation(context.Background(), QueueFilters{Tags: []string{"urgent", "backend"}}); err != nil {
			t.Fatalf("WaitAndClaimImplementation: %v", err)
		}
	})
	t.Run("mixed filters use atomic next claim", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "next-filters")
		client := startHelperClient(t, "worker")
		defer client.Close()
		if _, err := client.WaitAndClaimReview(context.Background(), QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"trivial"}}); err != nil {
			t.Fatalf("WaitAndClaimReview: %v", err)
		}
	})
}

func TestPreviewTicketQueueUsesBoundedReadOnlyTicketProjections(t *testing.T) {
	t.Setenv(helperScenarioEnv, "queue-preview")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx := context.Background()
	active, err := client.ActiveClaims(ctx, "review", 4)
	if err != nil || len(active.Items) != 1 || active.Items[0].ID != "20260919-10001" || !active.More {
		t.Fatalf("ActiveClaims=%#v err=%v", active, err)
	}
	ready, err := client.ReadyFrontier(ctx, "review", QueueFilters{WithoutTags: []string{"trivial", "no-review"}}, 3)
	if err != nil || len(ready.Items) != 2 || ready.Items[0].ID != "20260919-10002" || ready.Items[1].ID != "20260919-10003" || !ready.More {
		t.Fatalf("ReadyFrontier=%#v err=%v", ready, err)
	}
}

func TestReviewQueueFiltersPollsPastEmptyFilteredQueue(t *testing.T) {
	t.Setenv(helperScenarioEnv, "review-filter-empty-then")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ticket, err := client.WaitAndClaimReview(ctx, QueueFilters{WithoutTags: []string{"trivial"}})
	if err != nil || ticket.ID != testID {
		t.Fatalf("WaitAndClaimReviewWithoutTags = %#v, %v", ticket, err)
	}
}

func TestFilteredQueueWaitStopsPollingOnCancellation(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperScenarioEnv, "filtered-empty")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.WaitAndClaimReview(ctx, QueueFilters{WithoutTags: []string{"skip"}})
		done <- err
	}()
	waitForFile(t, ready)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("filtered wait error = %v, want context canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("filtered wait did not stop after cancellation")
	}
}

func TestReassignTicketUsesAtomicOwnershipTransfer(t *testing.T) {
	t.Setenv(helperScenarioEnv, "reassign")
	client := startHelperClient(t, "worker")
	defer client.Close()
	result, err := client.ReassignTicket(context.Background(), testID, "operator", MutationOptions{Handoff: "waiting for external API", Message: "Ticket-side action required"})
	if err != nil {
		t.Fatalf("ReassignTicket: %v", err)
	}
	if result.ID != testID || !result.Changed || result.Assignee != "operator" || result.State != "open" {
		t.Fatalf("ReassignTicket result = %#v", result)
	}
}

func TestReassignTicketIntegratesWithTicketCLI(t *testing.T) {
	executable, err := exec.LookPath("ticket")
	if err != nil {
		t.Skip("ticket executable is not available for protocol integration")
	}
	root := t.TempDir()
	run := func(args ...string) []byte {
		t.Helper()
		command := exec.Command(executable, args...)
		command.Dir = root
		command.Env = append(os.Environ(), "TICKET_REPOSITORY="+root, "TICKET_ACTOR=coder")
		output, runErr := command.CombinedOutput()
		if runErr != nil {
			t.Fatalf("ticket %q: %v\n%s", args, runErr, output)
		}
		return output
	}
	run("init")
	var created struct {
		ID string `json:"id"`
	}
	create := exec.Command(executable, "create", "atomic transfer", "-", "-j")
	create.Dir = root
	create.Env = append(os.Environ(), "TICKET_REPOSITORY="+root, "TICKET_ACTOR=coder")
	create.Stdin = strings.NewReader("blocked dependency\n")
	output, err := create.CombinedOutput()
	if err != nil {
		t.Fatalf("ticket create: %v\n%s", err, output)
	}
	if err := json.Unmarshal(output, &created); err != nil || created.ID == "" {
		t.Fatalf("create response = %q, err=%v", output, err)
	}
	run("claim", created.ID)
	client, err := NewWithExecutableWorkingDirAndRepository("coder", executable, root, root)
	if err != nil {
		t.Fatalf("new Ticket client: %v", err)
	}
	defer client.Close()
	if _, err := client.ReassignTicket(context.Background(), created.ID, "operator", MutationOptions{Handoff: "waiting for external API", Message: "Ticket-side action required"}); err != nil {
		t.Fatalf("ReassignTicket integration: %v", err)
	}
	var shown struct {
		ID       string `json:"id"`
		State    string `json:"state"`
		Assignee string `json:"assignee"`
	}
	if err := json.Unmarshal(run("show", created.ID, "-j"), &shown); err != nil {
		t.Fatalf("show response: %v", err)
	}
	if shown.ID != created.ID || shown.State != "open" || shown.Assignee != "operator" {
		t.Fatalf("reassigned ticket = %#v, want open/operator", shown)
	}
}

func TestTagFiltersRejectUnsafeValues(t *testing.T) {
	for _, tags := range [][]string{{"Needs-Review"}, {"needs review"}, {""}, {" trivial"}, {"to:team-a"}, {strings.Repeat("a", 65)}, make([]string, 65)} {
		if _, err := CanonicalQueueFilters(QueueFilters{Tags: tags}); err == nil {
			t.Fatalf("CanonicalQueueFilters accepted %#v", tags)
		}
	}
}

func TestCanonicalQueueFiltersSortsCopiesAndRejectsContradictions(t *testing.T) {
	input := QueueFilters{Tags: []string{"urgent", "backend"}, WithoutTags: []string{"skip"}}
	got, err := CanonicalQueueFilters(input)
	if err != nil {
		t.Fatal(err)
	}
	want := QueueFilters{Tags: []string{"backend", "urgent"}, WithoutTags: []string{"skip"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical filters = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(input, QueueFilters{Tags: []string{"urgent", "backend"}, WithoutTags: []string{"skip"}}) {
		t.Fatalf("canonicalization mutated input: %#v", input)
	}
	empty, err := CanonicalQueueFilters(QueueFilters{Tags: []string{}})
	if err != nil || !reflect.DeepEqual(empty, QueueFilters{}) {
		t.Fatalf("empty filters = %#v, %v; want zero selector", empty, err)
	}
	if _, err := CanonicalQueueFilters(QueueFilters{Tags: []string{"security"}, WithoutTags: []string{"security"}}); err == nil {
		t.Fatal("contradictory filters accepted")
	}
}

func TestReviewQueueFilterUsesSupportedTicketProtocol(t *testing.T) {
	executable := os.Getenv("TICKET_ORC_TICKET_BIN")
	if executable == "" {
		var err error
		executable, err = exec.LookPath("ticket")
		if err != nil {
			t.Skip("ticket executable is not available for protocol integration")
		}
	}
	helpOutput, err := exec.Command(executable, "help", "next").CombinedOutput()
	if err != nil || !strings.Contains(string(helpOutput), "--without-tag") {
		t.Skip("ticket executable does not expose atomic filtered next")
	}
	root := t.TempDir()
	run := func(args ...string) []byte {
		t.Helper()
		command := exec.Command(executable, args...)
		command.Dir = root
		command.Env = append(os.Environ(), "TICKET_REPOSITORY="+root, "TICKET_ACTOR=coder")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("ticket %q: %v\n%s", args, err, output)
		}
		return output
	}
	run("init")
	create := func(title string, tag bool) string {
		args := []string{"create", title, "-", "-j"}
		if tag {
			args = []string{"create", title, "-", "--tag", "trivial", "-j"}
		}
		command := exec.Command(executable, args...)
		command.Dir = root
		command.Env = append(os.Environ(), "TICKET_REPOSITORY="+root, "TICKET_ACTOR=coder")
		command.Stdin = strings.NewReader("Verify protocol\n")
		var response struct {
			ID string `json:"id"`
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("ticket %q: %v\n%s", args, err, output)
		}
		if err := json.Unmarshal(output, &response); err != nil || response.ID == "" {
			t.Fatalf("create response = %q, err=%v", output, err)
		}
		run("open", response.ID, "--claim")
		run("submit", response.ID)
		return response.ID
	}
	create("tagged", true)
	untagged := create("untagged", false)
	client, err := NewWithExecutableWorkingDirAndRepository("coder", executable, root, root)
	if err != nil {
		t.Fatalf("new Ticket client: %v", err)
	}
	defer client.Close()
	claimed, err := client.WaitAndClaimReview(context.Background(), QueueFilters{WithoutTags: []string{"trivial"}})
	if err != nil {
		t.Fatalf("filtered review wait: %v", err)
	}
	if claimed.ID != untagged || claimed.State != "review" || claimed.Assignee != "coder" {
		t.Fatalf("claimed = %#v, want untagged review owned by coder", claimed)
	}
}

func TestCloseTicketUsesExplicitID(t *testing.T) {
	t.Setenv(helperScenarioEnv, "close")
	client := startHelperClient(t, "worker")
	defer client.Close()
	transition, err := client.CloseTicket(context.Background(), testID)
	if err != nil || transition.ID != testID || transition.State != StateClosed {
		t.Fatalf("CloseTicket = %#v, %v", transition, err)
	}
}

func TestHoldTicketUsesOrdinaryWorkflowMutation(t *testing.T) {
	t.Setenv(helperScenarioEnv, "hold")
	client := startHelperClient(t, "ticket-orc.1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa")
	defer client.Close()
	result, err := client.HoldTicket(context.Background(), testID, MutationOptions{Message: "Orc circuit breaker"})
	if err != nil || result.ID != testID || result.FromState != "review" || result.State != "hold" {
		t.Fatalf("HoldTicket = %#v, %v", result, err)
	}
}

func TestShowNormalizesLegacyCompletedState(t *testing.T) {
	t.Setenv(helperScenarioEnv, "legacy-show")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ticket, err := client.Show(context.Background(), testID)
	if err != nil {
		t.Fatalf("Show: %v", err)
	}
	if ticket.State != StateClosed {
		t.Fatalf("Show state = %q, want %q", ticket.State, StateClosed)
	}
}

func TestListTicketsDelegatesBoundedProjectionAndNormalizesState(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-list")
	client := startHelperClient(t, "worker")
	defer client.Close()
	result, err := client.ListTickets(context.Background(), ListQuery{States: []string{"open", StateLegacyCompleted}, Limit: 2, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != testID || result.Items[0].State != StateClosed || !result.More {
		t.Fatalf("list result=%#v", result)
	}
}

func TestListTicketsDelegatesSearchCandidateFiltersAndParentProjection(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-list-filtered")
	client := startHelperClient(t, "worker")
	defer client.Close()
	priority := 1
	result, err := client.ListTickets(context.Background(), ListQuery{
		IDs: []string{testID, "20260919-10002"}, States: []string{"open", "review"},
		Priority: &priority, Assignee: "coder", Tags: []string{"api", "ticket-ui"}, Limit: 2, Offset: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != "20260919-10002" || result.Items[0].Parent != "20260920-00100" || len(result.Items[0].Tags) != 2 || !result.More {
		t.Fatalf("filtered list result=%#v", result)
	}
}

func TestListTicketsUsesAllStateForFilteredHintEnumeration(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-list-all-filtered")
	client := startHelperClient(t, "worker")
	defer client.Close()
	priority := 0
	result, err := client.ListTickets(context.Background(), ListQuery{Priority: &priority, Tags: []string{"api"}, Limit: 10, Offset: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Parent != "20260920-00100" || !result.More {
		t.Fatalf("all-state filtered list=%#v", result)
	}
}

func TestSearchTicketCandidatesReturnsCompleteBoundedProjection(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-grep")
	client := startHelperClient(t, "worker")
	defer client.Close()
	result, err := client.SearchTicketCandidates(context.Background(), "parser")
	if err != nil || len(result.Items) != 2 || result.More {
		t.Fatalf("search candidates=%#v err=%v", result, err)
	}
}

func TestSearchTicketsDelegatesTicketGrepAndPaginates(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-grep")
	client := startHelperClient(t, "worker")
	defer client.Close()
	result, err := client.SearchTickets(context.Background(), "parser", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != "20260919-10002" || result.More {
		t.Fatalf("search result=%#v", result)
	}
}

func TestSearchTicketsQuotesLiteralLeadingHyphenAndMetacharacters(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-grep-literal")
	client := startHelperClient(t, "worker")
	defer client.Close()
	result, err := client.SearchTickets(context.Background(), "-a.b", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ID != testID {
		t.Fatalf("search result=%#v", result)
	}
}

func TestSearchTicketsRejectsUnboundedMatchSet(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository-grep-large")
	client := startHelperClient(t, "worker")
	defer client.Close()
	if _, err := client.SearchTickets(context.Background(), "parser", 1, 0); err == nil || !strings.Contains(err.Error(), "bounded limit") {
		t.Fatalf("SearchTickets error=%v, want bounded-result error", err)
	}
}

func TestTypedTicketMutationsUseFixedCommandsAndStructuredInput(t *testing.T) {
	t.Setenv(helperScenarioEnv, "mutation-input")
	client := startHelperClient(t, "ui-actor")
	defer client.Close()
	result, err := client.CloseTicketWithOptions(context.Background(), testID, MutationOptions{Outcome: "accepted", Message: "ship it"})
	if err != nil || result.ID != testID || !result.Changed {
		t.Fatalf("close mutation=%#v err=%v", result, err)
	}
}

func TestTypedTicketReviewMutationUsesExplicitMessageFlag(t *testing.T) {
	t.Setenv(helperScenarioEnv, "mutation-review")
	client := startHelperClient(t, "ui-actor")
	defer client.Close()
	result, err := client.ReviewTicket(context.Background(), testID, MutationOptions{Message: "send it"})
	if err != nil || result.ID != testID || !result.Changed {
		t.Fatalf("review mutation=%#v err=%v", result, err)
	}
}

func TestShowDetailReturnsBoundedSectionsAndNormalizesState(t *testing.T) {
	t.Setenv(helperScenarioEnv, "detail")
	client := startHelperClient(t, "worker")
	defer client.Close()
	detail, err := client.ShowDetail(context.Background(), testID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.ID != testID || detail.State != StateClosed || detail.Sections["objective"].Text != "Inspect it" || detail.Sections["work_log"].Truncated != true || detail.Readiness == nil || detail.Readiness.Ready || len(detail.Readiness.Blockers) != 1 || detail.Created != "2026-09-19" || detail.Modified != "2026-09-20T12:30:00Z" || detail.Body != "Complete display body" || !detail.BodyTruncated || !detail.Truncated {
		t.Fatalf("detail=%#v", detail)
	}
}

func TestShowDetailReturnsReadyTicketReadiness(t *testing.T) {
	t.Setenv(helperScenarioEnv, "detail-ready")
	client := startHelperClient(t, "worker")
	defer client.Close()
	detail, err := client.ShowDetail(context.Background(), testID)
	if err != nil || detail.Readiness == nil || !detail.Readiness.Ready || len(detail.Readiness.Blockers) != 0 {
		t.Fatalf("detail=%#v err=%v", detail, err)
	}
}

func TestShowDetailFailsSafelyWhenAnyComponentFails(t *testing.T) {
	for _, scenario := range []string{"detail-fail-show", "detail-fail-status", "detail-fail-body"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, scenario)
			client := startHelperClient(t, "worker")
			defer client.Close()
			detail, err := client.ShowDetail(context.Background(), testID)
			if err == nil || detail.ID != "" {
				t.Fatalf("detail=%#v err=%v", detail, err)
			}
		})
	}
}

func TestTicketAPIVersionIsV2(t *testing.T) {
	if APIVersion != 2 {
		t.Fatalf("APIVersion = %d, want 2", APIVersion)
	}
	if NormalizeLifecycleState(StateLegacyCompleted) != StateClosed {
		t.Fatalf("legacy state was not normalized")
	}
}

func TestNewWithExecutableAndWorkingDirSetsChildDirectory(t *testing.T) {
	t.Setenv(helperScenarioEnv, "blocked-wait")
	directory := t.TempDir()
	client, err := NewWithExecutableAndWorkingDir("worker", os.Args[0], directory)
	if err != nil {
		t.Fatalf("NewWithExecutableAndWorkingDir: %v", err)
	}
	defer client.Close()
	if client.command.Dir != directory {
		t.Fatalf("child directory = %q, want %q", client.command.Dir, directory)
	}
}

func TestNewWithWorkingDirAndRepositoryOverridesChildRepository(t *testing.T) {
	t.Setenv(helperScenarioEnv, "repository")
	t.Setenv("TICKET_REPOSITORY", "inherited-repository")
	client, err := NewWithExecutableWorkingDirAndRepository("worker", os.Args[0], t.TempDir(), "configured-repository")
	if err != nil {
		t.Fatalf("NewWithExecutableWorkingDirAndRepository: %v", err)
	}
	defer client.Close()
	got, err := client.Show(context.Background(), testID)
	if err != nil || got.Title != "configured-repository" {
		t.Fatalf("repository = %#v, %v", got, err)
	}
}

func TestTargetIsolationAndScopedArgumentOrdering(t *testing.T) {
	t.Run("repository clears inherited scope", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "target")
		t.Setenv("TICKET_REPOSITORY", "inherited-repository")
		t.Setenv("TICKET_SCOPE", "inherited-scope")
		t.Setenv("TICKET_ROOT", "inherited-root")
		client, err := NewWithExecutableAndTarget("worker", os.Args[0], Target{Repository: "configured-repository"})
		if err != nil {
			t.Fatalf("direct target: %v", err)
		}
		defer client.Close()
		got, err := client.Show(context.Background(), testID)
		if err != nil || got.Title != "configured-repository||worker" {
			t.Fatalf("direct target environment = %#v, %v", got, err)
		}
	})
	t.Run("scope uses global arguments and clears inherited selection", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "target")
		t.Setenv("TICKET_REPOSITORY", "inherited-repository")
		t.Setenv("TICKET_SCOPE", "inherited-scope")
		t.Setenv("TICKET_ROOT", "inherited-root")
		t.Setenv(helperExpectedConfigEnv, "/config/ticket.json")
		t.Setenv(helperExpectedScopeEnv, "named")
		client, err := NewWithExecutableAndTarget("worker", os.Args[0], Target{Config: "/config/ticket.json", Scope: "named"})
		if err != nil {
			t.Fatalf("scoped target: %v", err)
		}
		defer client.Close()
		got, err := client.Show(context.Background(), testID)
		if err != nil || got.Title != "|named|worker" {
			t.Fatalf("scoped target environment = %#v, %v", got, err)
		}
	})
}

func TestApplyTargetEnvironmentClearsLegacyRoot(t *testing.T) {
	base := []string{"TICKET_ACTOR=inherited-actor", "TICKET_REPOSITORY=inherited-repository", "TICKET_CONFIG=inherited-config", "TICKET_SCOPE=inherited-scope", "TICKET_ROOT=inherited-root", "OTHER=preserved"}
	for _, target := range []Target{{Repository: "configured-repository"}, {Config: "/config/ticket.json", Scope: "named"}} {
		env := ApplyTargetEnvironment(base, "worker", target)
		values := map[string][]string{}
		for _, entry := range env {
			key, value, _ := strings.Cut(entry, "=")
			values[key] = append(values[key], value)
		}
		if got := values["TICKET_ACTOR"]; len(got) != 1 || got[0] != "worker" {
			t.Errorf("target %v actor = %v", target, got)
		}
		if got := values["OTHER"]; len(got) != 1 || got[0] != "preserved" {
			t.Errorf("target %v unrelated value = %v", target, got)
		}
		if strings.TrimSpace(target.Repository) != "" {
			if got := values["TICKET_REPOSITORY"]; len(got) != 1 || got[0] != target.Repository {
				t.Errorf("repository target = %v", got)
			}
			if len(values["TICKET_CONFIG"]) != 0 || len(values["TICKET_SCOPE"]) != 0 {
				t.Errorf("repository target retained scoped values: %v", values)
			}
		} else {
			if got := values["TICKET_CONFIG"]; len(got) != 1 || got[0] != target.Config {
				t.Errorf("config target = %v", got)
			}
			if got := values["TICKET_SCOPE"]; len(got) != 1 || got[0] != target.Scope {
				t.Errorf("scope target = %v", got)
			}
			if len(values["TICKET_REPOSITORY"]) != 0 {
				t.Errorf("scoped target retained repository: %v", values["TICKET_REPOSITORY"])
			}
		}
		if len(values["TICKET_ROOT"]) != 0 {
			t.Errorf("target %v retained legacy root: %v", target, values["TICKET_ROOT"])
		}
	}
}

func TestProbeInfoUsesTargetArgumentsAndParsesMetadata(t *testing.T) {
	t.Setenv(helperScenarioEnv, "probe")
	t.Setenv(helperProbeEnv, "1")
	t.Setenv(helperExpectedConfigEnv, "/config/ticket.json")
	t.Setenv(helperExpectedScopeEnv, "named")
	t.Setenv(helperResponseEnv, `{"path":"/repo","name":"Example","id":"8d1268c4-6a64-4b9b-95c9-d5598a150e86","format_version":2,"storage_version":3,"scope":"named"}`)
	info, err := ProbeInfoWithExecutable(context.Background(), "worker", os.Args[0], t.TempDir(), Target{Config: "/config/ticket.json", Scope: "named"})
	if err != nil {
		t.Fatalf("ProbeInfoWithExecutable: %v", err)
	}
	if info.Path != "/repo" || info.Name == nil || *info.Name != "Example" || info.ID != "8d1268c4-6a64-4b9b-95c9-d5598a150e86" || info.FormatVersion != 2 || info.StorageVersion != 3 || info.Scope == nil || *info.Scope != "named" {
		t.Fatalf("probe info = %#v", info)
	}
}

func TestValidRepositoryIDRequiresCanonicalUUIDv4(t *testing.T) {
	for _, test := range []struct {
		value string
		valid bool
	}{
		{value: "8d1268c4-6a64-4b9b-95c9-d5598a150e86", valid: true},
		{value: "8D1268C4-6A64-4B9B-95C9-D5598A150E86"},
		{value: "8d1268c4-6a64-1b9b-95c9-d5598a150e86"},
		{value: "8d1268c4-6a64-4b9b-75c9-d5598a150e86"},
		{value: "/repo"},
	} {
		if got := ValidRepositoryID(test.value); got != test.valid {
			t.Errorf("ValidRepositoryID(%q) = %t, want %t", test.value, got, test.valid)
		}
	}
}

func TestProbeInfoRejectsMalformedAndNonzeroResponsesSafely(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "probe")
		t.Setenv(helperProbeEnv, "1")
		t.Setenv(helperProbeMalformedEnv, "1")
		_, err := ProbeInfoWithExecutable(context.Background(), "worker", os.Args[0], "", Target{})
		var probeErr *ProbeError
		if !errors.As(err, &probeErr) || probeErr.Code != "malformed_output" {
			t.Fatalf("malformed probe error = %v", err)
		}
	})
	t.Run("nonzero", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "probe")
		t.Setenv(helperProbeEnv, "1")
		t.Setenv(helperProbeExitEnv, "1")
		t.Setenv(helperProbeErrorEnv, `{"error":{"code":"missing_config","message":"repository config unavailable"}}`)
		_, err := ProbeInfoWithExecutable(context.Background(), "worker", os.Args[0], "", Target{})
		var probeErr *ProbeError
		if !errors.As(err, &probeErr) || probeErr.Code != "missing_config" || probeErr.Message != "repository config unavailable" {
			t.Fatalf("nonzero probe error = %v", err)
		}
		if strings.Contains(err.Error(), "opaque") {
			t.Fatalf("probe leaked unsafe detail: %v", err)
		}
	})
	t.Run("scope mismatch", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "probe")
		t.Setenv(helperProbeEnv, "1")
		t.Setenv(helperExpectedConfigEnv, "/config/ticket.json")
		t.Setenv(helperExpectedScopeEnv, "named")
		t.Setenv(helperResponseEnv, `{"path":"/repo","id":"8d1268c4-6a64-4b9b-95c9-d5598a150e86","format_version":1,"storage_version":1,"scope":"other"}`)
		_, err := ProbeInfoWithExecutable(context.Background(), "worker", os.Args[0], "", Target{Config: "/config/ticket.json", Scope: "named"})
		var probeErr *ProbeError
		if !errors.As(err, &probeErr) || probeErr.Code != "scope_mismatch" {
			t.Fatalf("scope mismatch error = %v", err)
		}
	})
	t.Run("missing repository ID", func(t *testing.T) {
		t.Setenv(helperScenarioEnv, "probe")
		t.Setenv(helperProbeEnv, "1")
		t.Setenv(helperResponseEnv, `{"path":"/repo","format_version":1,"storage_version":1}`)
		_, err := ProbeInfoWithExecutable(context.Background(), "worker", os.Args[0], "", Target{})
		var probeErr *ProbeError
		if !errors.As(err, &probeErr) || probeErr.Code != "missing_repository_id" {
			t.Fatalf("missing repository ID error = %v", err)
		}
	})
}

func TestDecoderRetainsBufferedResponses(t *testing.T) {
	t.Setenv(helperScenarioEnv, "buffered")
	client := startHelperClient(t, "worker")
	defer client.Close()

	first, err := client.Show(context.Background(), "20260919-20001")
	if err != nil || first.Title != "first" {
		t.Fatalf("first Show = %#v, %v", first, err)
	}
	second, err := client.Show(context.Background(), "20260919-20002")
	if err != nil || second.Title != "second" {
		t.Fatalf("second Show = %#v, %v", second, err)
	}
}

func TestFragmentedResponse(t *testing.T) {
	t.Setenv(helperScenarioEnv, "fragmented")
	client := startHelperClient(t, "worker")
	defer client.Close()

	ticket, err := client.Show(context.Background(), testID)
	if err != nil || ticket.Title != "fragmented" {
		t.Fatalf("Show = %#v, %v", ticket, err)
	}
}

func TestCommandErrorKeepsConnectionAndMutationDetails(t *testing.T) {
	t.Setenv(helperScenarioEnv, "error-recovery")
	client := startHelperClient(t, "worker")
	defer client.Close()

	_, err := client.Release(context.Background(), testID)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("Release error = %T %v, want CommandError", err, err)
	}
	if commandErr.ExitCode != -1 || commandErr.Code != "scm_error" || commandErr.Message != "push failed" {
		t.Fatalf("command error = %#v", commandErr)
	}
	if !MutationApplied(err) {
		t.Fatalf("mutation_applied was not preserved: %#v", commandErr.Details)
	}
	var affected []string
	if decodeErr := json.Unmarshal(commandErr.Details["affected_ids"], &affected); decodeErr != nil || !reflect.DeepEqual(affected, []string{testID}) {
		t.Fatalf("affected IDs = %#v, %v", affected, decodeErr)
	}

	shown, showErr := client.Show(context.Background(), testID)
	if showErr != nil || shown.State != "open" {
		t.Fatalf("Show after command error = %#v, %v", shown, showErr)
	}
}

func TestCommandErrorCarriesStructuredTicketID(t *testing.T) {
	t.Setenv(helperScenarioEnv, "single-response")
	t.Setenv(helperResponseEnv, `{"error":{"code":"conflict","message":"wording can change","details":{"id":"20260919-12345"}}}`)
	client := startHelperClient(t, "worker")
	defer client.Close()

	_, err := client.Show(context.Background(), testID)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.TicketID != "20260919-12345" {
		t.Fatalf("structured command error = %#v, %v", commandErr, err)
	}
}

func TestCancellationPreservesReceivedMutationError(t *testing.T) {
	t.Setenv(helperScenarioEnv, "error-recovery")
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx := newCancelAfterResponseContext()

	_, err := client.Release(ctx, testID)
	if !errors.Is(err, context.Canceled) || !MutationApplied(err) {
		t.Fatalf("Release error = %v, want cancellation and mutation details", err)
	}
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Code != "scm_error" {
		t.Fatalf("Release error lost command envelope: %T %v", err, err)
	}
}

func TestCancellationStopsBlockedRequestAndClient(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperScenarioEnv, "blocked-wait")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.WaitAndClaimImplementation(ctx, QueueFilters{})
		done <- err
	}()
	waitForFile(t, ready)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled wait did not return")
	}
	if _, err := client.Show(context.Background(), testID); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped client Show error = %v, want original cancellation", err)
	}
}

func TestQueuedRequestCancellationDoesNotInterruptActiveRequest(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperScenarioEnv, "blocked-wait")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	defer client.Close()
	activeCtx, cancelActive := context.WithCancel(context.Background())
	activeDone := make(chan error, 1)
	go func() {
		_, err := client.WaitAndClaimImplementation(activeCtx, QueueFilters{})
		activeDone <- err
	}()
	waitForFile(t, ready)

	queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelQueued()
	if _, err := client.Show(queuedCtx, testID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued Show error = %v, want deadline exceeded", err)
	}
	select {
	case err := <-activeDone:
		t.Fatalf("queued cancellation interrupted active request: %v", err)
	default:
	}
	cancelActive()
	if err := <-activeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("active wait error = %v, want context canceled", err)
	}
}

func TestCloseInterruptsOutstandingWait(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperScenarioEnv, "blocked-wait")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	done := make(chan error, 1)
	go func() {
		_, err := client.WaitAndClaimImplementation(context.Background(), QueueFilters{})
		done <- err
	}()
	waitForFile(t, ready)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("wait error = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not interrupt wait")
	}
	if _, err := client.Show(context.Background(), testID); !errors.Is(err, ErrClosed) {
		t.Fatalf("Show after Close = %v, want ErrClosed", err)
	}
}

func TestResponseProtocolAndSuccessValidation(t *testing.T) {
	tests := []struct {
		name     string
		response string
		call     func(*Client) error
		want     string
	}{
		{
			name: "missing claim item", response: `{"item":null}`,
			call: func(client *Client) error {
				_, err := client.WaitAndClaimImplementation(context.Background(), QueueFilters{})
				return err
			},
			want: "no claimed item",
		},
		{
			name: "wrong claim actor", response: `{"item":{"id":"20260919-12345","state":"open","assignee":"other"}}`,
			call: func(client *Client) error {
				_, err := client.WaitAndClaimImplementation(context.Background(), QueueFilters{})
				return err
			},
			want: "belongs to actor",
		},
		{
			name: "show mismatched ID", response: `{"id":"20260919-99999","state":"open"}`,
			call: func(client *Client) error { _, err := client.Show(context.Background(), testID); return err },
			want: "expected",
		},
		{
			name: "show missing state", response: `{"id":"20260919-12345"}`,
			call: func(client *Client) error { _, err := client.Show(context.Background(), testID); return err },
			want: "no state",
		},
		{
			name: "release mismatched ID", response: `{"id":"20260919-99999","changed":true}`,
			call: func(client *Client) error { _, err := client.Release(context.Background(), testID); return err },
			want: "expected",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "single-response")
			t.Setenv(helperResponseEnv, tt.response)
			client := startHelperClient(t, "worker")
			defer client.Close()
			err := tt.call(client)
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want protocol error containing %q", err, tt.want)
			}
		})
	}
}

func TestMalformedResponseStopsConnection(t *testing.T) {
	t.Setenv(helperScenarioEnv, "malformed")
	client := startHelperClient(t, "worker")
	defer client.Close()
	_, err := client.Show(context.Background(), testID)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Show error = %v, want ErrProtocol", err)
	}
	if _, err := client.Show(context.Background(), testID); !errors.Is(err, ErrProtocol) {
		t.Fatalf("second Show error = %v, want stopped protocol error", err)
	}
}

func TestUnexpectedExitIncludesBoundedStderr(t *testing.T) {
	t.Setenv(helperScenarioEnv, "exit-with-stderr")
	client := startHelperClient(t, "worker")
	<-client.done
	defer client.Close()
	_, err := client.Show(context.Background(), testID)
	if !errors.Is(err, ErrTransport) || !strings.Contains(err.Error(), "helper failure") || !strings.Contains(err.Error(), "…") {
		t.Fatalf("Show error = %v, want bounded stderr transport error", err)
	}
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Category != "process_exit" || transportErr.ExitCode != 9 || transportErr.Signal != "" {
		t.Fatalf("Show error type = %#v, want bounded process exit metadata", transportErr)
	}
}

func TestStartupCommandErrorPreservesJSONEnvelope(t *testing.T) {
	t.Setenv(helperScenarioEnv, "startup-error")
	client := startHelperClient(t, "worker")
	defer client.Close()
	_, err := client.Show(context.Background(), testID)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Code != "repo_not_found" || commandErr.Message != "not in a repository" {
		t.Fatalf("startup error = %T %v, want repo_not_found CommandError", err, err)
	}
}

func TestActorQueueConflictExtractsSafeQueueAndTicket(t *testing.T) {
	err := &CommandError{Code: "conflict", Message: "Ticket wording may change without changing classification.", Details: map[string]json.RawMessage{"id": json.RawMessage(`"20260922-12345"`)}}
	candidate := actorQueueConflictCandidate(err, "review")
	conflict := ActorQueueConflict(candidate, "review")
	if conflict == nil || conflict.RequestedQueue != "review" || conflict.ConflictingQueue != "open" || conflict.TicketID != "20260922-12345" {
		t.Fatalf("conflict=%#v", conflict)
	}
	if unrelated := ActorQueueConflict(&CommandError{Code: "conflict", Message: "Actor already owns work in another queue."}, "review"); unrelated != nil {
		t.Fatalf("unrelated conflict classified as actor queue conflict: %#v", unrelated)
	}
}

func TestWaitAndClaimVerifiesActorQueueConflictAgainstTicketState(t *testing.T) {
	for _, test := range []struct {
		name         string
		state        string
		wantConflict bool
	}{
		{name: "other queue", state: "open", wantConflict: true},
		{name: "same queue filter conflict", state: "review", wantConflict: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(helperScenarioEnv, "queue-conflict")
			t.Setenv("TICKETCLIENT_HELPER_CONFLICT_STATE", test.state)
			client := startHelperClient(t, "worker")
			defer client.Close()
			_, err := client.WaitAndClaimReview(context.Background(), QueueFilters{WithoutTags: []string{"skip"}})
			conflict := ActorQueueConflict(err, "review")
			if (conflict != nil) != test.wantConflict {
				t.Fatalf("conflict=%#v, err=%v", conflict, err)
			}
			if test.wantConflict && (conflict.TicketID != testID || conflict.ConflictingQueue != "open") {
				t.Fatalf("verified conflict=%#v", conflict)
			}
			if !test.wantConflict {
				var commandErr *CommandError
				if !errors.As(err, &commandErr) {
					t.Fatalf("same-queue conflict lost its command error: %T %v", err, err)
				}
			}
		})
	}
}

// A process exit racing startup must not erase Ticket's original JSON error.
func TestStartupCommandErrorRacingProcessExitPreservesEnvelope(t *testing.T) {
	t.Setenv(helperScenarioEnv, "startup-error-delayed")
	client := startHelperClient(t, "worker")
	defer client.Close()
	_, err := client.Show(context.Background(), testID)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Code != "repo_not_found" {
		t.Fatalf("racing startup error = %T %v, want repo_not_found CommandError", err, err)
	}
}

func TestStartupCommandErrorSurvivesWriteFailureBeforeProcessExit(t *testing.T) {
	t.Setenv(helperScenarioEnv, "startup-error-closed-stdin")
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	defer client.Close()
	waitForFile(t, ready)
	_, err := client.Show(context.Background(), testID)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Code != "repo_not_found" {
		t.Fatalf("write-race startup error = %T %v, want repo_not_found CommandError", err, err)
	}
}

func TestStartupMalformedResponseIsProtocolError(t *testing.T) {
	t.Setenv(helperScenarioEnv, "startup-malformed")
	client := startHelperClient(t, "worker")
	defer client.Close()
	_, err := client.Show(context.Background(), testID)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("startup malformed error = %v, want ErrProtocol", err)
	}
}

func TestWriteFailureStopsConnection(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv(helperScenarioEnv, "closed-stdin")
	t.Setenv(helperReadyFileEnv, ready)
	client := startHelperClient(t, "worker")
	defer client.Close()
	waitForFile(t, ready)
	time.Sleep(20 * time.Millisecond)
	_, err := client.Show(context.Background(), testID)
	if !errors.Is(err, ErrTransport) {
		t.Fatalf("Show error = %v, want ErrTransport", err)
	}
}

func TestOversizedRequestIsNotSentAndConnectionSurvives(t *testing.T) {
	t.Setenv(helperScenarioEnv, "single-show")
	client := startHelperClient(t, "worker")
	defer client.Close()
	var ignored any
	err := client.invoke(context.Background(), []string{strings.Repeat("x", maxRequestFrameBytes)}, &ignored)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized invoke error = %v", err)
	}
	if _, err := client.Show(context.Background(), testID); err != nil {
		t.Fatalf("Show after oversized request: %v", err)
	}
}

func TestExplicitIDsAndCanceledContextDoNotWriteRequests(t *testing.T) {
	t.Setenv(helperScenarioEnv, "single-show")
	client := startHelperClient(t, "worker")
	defer client.Close()
	if _, err := client.Show(context.Background(), "12345"); err == nil {
		t.Fatal("Show accepted shorthand ID")
	}
	if _, err := client.Release(context.Background(), "20260230-12345"); err == nil {
		t.Fatal("Release accepted invalid date")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Show(ctx, testID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Show error = %v", err)
	}
	if _, err := client.Show(context.Background(), testID); err != nil {
		t.Fatalf("valid Show after local validation failures: %v", err)
	}
}

func TestClientConstructionAndStartupValidation(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("New accepted empty actor")
	}
	if _, err := New("two actors"); err == nil {
		t.Fatal("New accepted actor whitespace")
	}
	if _, err := New("bad=actor"); err == nil {
		t.Fatal("New accepted actor outside ticket's token grammar")
	}
	if _, err := NewWithExecutable("worker", " "); err == nil {
		t.Fatal("NewWithExecutable accepted empty executable")
	}
	missing := filepath.Join(t.TempDir(), "missing-ticket")
	if _, err := NewWithExecutable("worker", missing); err == nil || !strings.Contains(err.Error(), "start") {
		t.Fatalf("missing executable error = %v", err)
	}
}

type cancelAfterResponseContext struct {
	calls atomic.Int32
	done  chan struct{}
	once  sync.Once
}

func newCancelAfterResponseContext() *cancelAfterResponseContext {
	return &cancelAfterResponseContext{done: make(chan struct{})}
}

func (c *cancelAfterResponseContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterResponseContext) Done() <-chan struct{}       { return c.done }
func (c *cancelAfterResponseContext) Value(any) any               { return nil }
func (c *cancelAfterResponseContext) Err() error {
	if c.calls.Add(1) > 2 {
		c.once.Do(func() { close(c.done) })
		return context.Canceled
	}
	return nil
}

func startHelperClient(t *testing.T, actor string) *Client {
	t.Helper()
	client, err := NewWithExecutable(actor, os.Args[0])
	if err != nil {
		t.Fatalf("NewWithExecutable: %v", err)
	}
	return client
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for helper marker %s", path)
}

type helperRequest struct {
	Args  []string `json:"args"`
	Stdin *string  `json:"stdin"`
}

func runTicketHelper(scenario string) int {
	expectedArgs := []string{"-i", "-j"}
	if config := os.Getenv(helperExpectedConfigEnv); config != "" {
		command := []string{"-i", "-j"}
		if os.Getenv(helperProbeEnv) != "" {
			command = []string{"info", "-j"}
		}
		expectedArgs = append([]string{"--config", config, "--scope", os.Getenv(helperExpectedScopeEnv)}, command...)
	} else if os.Getenv(helperProbeEnv) != "" {
		expectedArgs = []string{"info", "-j"}
	}
	if !reflect.DeepEqual(os.Args[1:], expectedArgs) {
		_, _ = fmt.Fprintf(os.Stderr, "startup args = %q", os.Args[1:])
		return 80
	}
	if os.Getenv(helperProbeEnv) != "" {
		if os.Getenv(helperProbeExitEnv) != "" {
			if response := os.Getenv(helperProbeErrorEnv); response != "" {
				_, _ = io.WriteString(os.Stdout, response+"\n")
			}
			_, _ = io.WriteString(os.Stderr, "opaque probe diagnostic")
			return 7
		}
		if os.Getenv(helperProbeMalformedEnv) != "" {
			_, _ = io.WriteString(os.Stdout, "{malformed}\n")
			return 0
		}
		response := os.Getenv(helperResponseEnv)
		if response == "" {
			response = `{"path":"/repo","id":"8d1268c4-6a64-4b9b-95c9-d5598a150e86","format_version":1,"storage_version":1}`
		}
		_, _ = io.WriteString(os.Stdout, response+"\n")
		return 0
	}
	if scenario == "exit-with-stderr" {
		_, _ = io.WriteString(os.Stderr, "helper failure: "+strings.Repeat("x", 1<<20))
		return 9
	}
	if scenario == "startup-error" {
		writeHelperError("repo_not_found", "not in a repository", nil)
		return 3
	}
	if scenario == "startup-error-delayed" {
		writeHelperError("repo_not_found", "not in a repository", nil)
		time.Sleep(10 * time.Millisecond)
		return 3
	}
	if scenario == "startup-error-closed-stdin" {
		_ = os.Stdin.Close()
		writeHelperError("repo_not_found", "not in a repository", nil)
		_ = os.WriteFile(os.Getenv(helperReadyFileEnv), []byte("ready"), 0o600)
		time.Sleep(100 * time.Millisecond)
		return 3
	}
	if scenario == "startup-malformed" {
		_, _ = io.WriteString(os.Stdout, "{not-json}\n")
		return 3
	}
	if scenario == "closed-stdin" {
		_ = os.Stdin.Close()
		_ = os.WriteFile(os.Getenv(helperReadyFileEnv), []byte("ready"), 0o600)
		time.Sleep(time.Hour)
		return 0
	}

	reader := bufio.NewReader(os.Stdin)
	for count := 1; ; count++ {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return 0
		}
		if err != nil && !errors.Is(err, io.EOF) {
			_, _ = fmt.Fprintf(os.Stderr, "read request: %v", err)
			return 81
		}
		var request helperRequest
		if err := json.Unmarshal(line, &request); err != nil {
			writeHelperJSON(map[string]any{"error": map[string]any{"code": "invalid_json", "message": err.Error()}})
			continue
		}
		if code := handleHelperRequest(scenario, count, request.Args, request.Stdin); code >= 0 {
			return code
		}
	}
}

func handleHelperRequest(scenario string, count int, args []string, stdin *string) int {
	switch scenario {
	case "sequence":
		want := [][]string{
			{"wait", "--claim"},
			{"wait", "review", "--claim"},
			{"show", testID},
			{"release", testID},
		}
		if count > len(want) || !reflect.DeepEqual(args, want[count-1]) {
			writeHelperError("bad_args", fmt.Sprintf("request %d args = %q", count, args), nil)
			return -1
		}
		switch count {
		case 1:
			writeHelperJSON(map[string]any{"item": map[string]any{"id": "20260919-10001", "state": "open", "assignee": os.Getenv("TICKET_ACTOR")}})
		case 2:
			writeHelperJSON(map[string]any{"item": map[string]any{"id": "20260919-10002", "state": "review", "assignee": os.Getenv("TICKET_ACTOR")}})
		case 3:
			writeHelperJSON(map[string]any{"id": testID, "title": os.Getenv("TICKET_ACTOR") + "|" + os.Getenv("TICKET_REPOSITORY"), "state": "open"})
		case 4:
			writeHelperJSON(map[string]any{"id": testID, "changed": true, "state": "open"})
		}
	case "review-filter":
		want := []string{"next", "review", "--without-tag", "no-review", "--without-tag", "trivial", "--claim"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("review filter args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"item": map[string]any{"id": testID, "state": "review", "assignee": os.Getenv("TICKET_ACTOR"), "tags": []string{"backend"}}})
	case "ready-filters":
		want := []string{"ready", "review", "--tag", "backend", "--tag", "urgent", "--without-tag", "skip", "--limit", "2", "--fields", "id,title,state,assignee,priority"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("ready filters args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []any{}, "more": false})
	case "wait-tags":
		want := []string{"wait", "--tag", "backend", "--tag", "urgent", "--claim"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("wait tag args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"item": map[string]any{"id": testID, "state": "open", "assignee": os.Getenv("TICKET_ACTOR")}})
	case "next-filters":
		want := []string{"next", "review", "--tag", "security", "--without-tag", "trivial", "--claim"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("next filter args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"item": map[string]any{"id": testID, "state": "review", "assignee": os.Getenv("TICKET_ACTOR")}})
	case "queue-preview":
		if count == 1 {
			want := []string{"list", "--state", "review", "--assignee", "worker", "--limit", "4", "--fields", "id,title,state,assignee,priority"}
			if !reflect.DeepEqual(args, want) {
				writeHelperError("bad_args", fmt.Sprintf("active preview args = %q", args), nil)
				return -1
			}
			writeHelperJSON(map[string]any{"items": []any{map[string]any{"id": "20260919-10001", "title": "active review", "state": "review", "assignee": "worker"}}, "more": true})
		} else if count == 2 {
			want := []string{"ready", "review", "--without-tag", "no-review", "--without-tag", "trivial", "--limit", "3", "--fields", "id,title,state,assignee,priority"}
			if !reflect.DeepEqual(args, want) {
				writeHelperError("bad_args", fmt.Sprintf("ready preview args = %q", args), nil)
				return -1
			}
			writeHelperJSON(map[string]any{"items": []any{
				map[string]any{"id": "20260919-10002", "title": "first ready", "state": "review"},
				map[string]any{"id": "20260919-10003", "title": "second ready", "state": "review"},
			}, "more": true})
		} else {
			writeHelperError("bad_args", fmt.Sprintf("unexpected preview command %q", args), nil)
			return -1
		}
	case "review-filter-empty-then":
		want := []string{"next", "review", "--without-tag", "trivial", "--claim"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("review filter empty args = %q", args), nil)
			return -1
		}
		if count == 1 {
			writeHelperJSON(map[string]any{"item": nil})
		} else {
			writeHelperJSON(map[string]any{"item": map[string]any{"id": testID, "state": "review", "assignee": os.Getenv("TICKET_ACTOR"), "tags": []string{"backend"}}})
		}
	case "filtered-empty":
		want := []string{"next", "review", "--without-tag", "skip", "--claim"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("filtered empty args = %q", args), nil)
			return -1
		}
		_ = os.WriteFile(os.Getenv(helperReadyFileEnv), []byte("ready"), 0o600)
		writeHelperJSON(map[string]any{"item": nil})
	case "reassign":
		want := []string{"reassign", testID, "--input", "-"}
		if !reflect.DeepEqual(args, want) || stdin == nil || *stdin != `{"assignee":"operator","handoff":"waiting for external API","message":"Ticket-side action required"}` {
			value := "<nil>"
			if stdin != nil {
				value = *stdin
			}
			writeHelperError("bad_args", fmt.Sprintf("reassign args=%q stdin=%q", args, value), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "changed": true, "state": "open", "assignee": "operator"})
	case "close":
		if !reflect.DeepEqual(args, []string{"close", testID}) {
			writeHelperError("bad_args", fmt.Sprintf("close args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "changed": true, "state": StateClosed})
	case "hold":
		if !reflect.DeepEqual(args, []string{"hold", testID, "--message", "Orc circuit breaker"}) {
			writeHelperError("bad_args", fmt.Sprintf("hold args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "changed": true, "from_state": "review", "state": "hold"})
	case "legacy-show":
		if !reflect.DeepEqual(args, []string{"show", testID}) {
			writeHelperError("bad_args", fmt.Sprintf("show args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "state": StateLegacyCompleted})
	case "buffered":
		if count == 1 {
			writeHelperJSON(map[string]any{"id": "20260919-20001", "title": "first", "state": "open"})
			writeHelperJSON(map[string]any{"id": "20260919-20002", "title": "second", "state": "review"})
		}
	case "fragmented":
		response := `{"id":"` + testID + `","title":"fragmented","state":"open"}` + "\n"
		for _, fragment := range []string{response[:7], response[7:23], response[23:]} {
			_, _ = io.WriteString(os.Stdout, fragment)
			time.Sleep(5 * time.Millisecond)
		}
	case "error-recovery":
		if count == 1 {
			writeHelperError("scm_error", "push failed", map[string]any{"mutation_applied": true, "affected_ids": []string{testID}})
		} else {
			writeHelperJSON(map[string]any{"id": testID, "state": "open"})
		}
	case "queue-conflict":
		if count == 1 {
			writeHelperError("conflict", "message wording is irrelevant", map[string]any{"actor": os.Getenv("TICKET_ACTOR"), "id": testID})
		} else if reflect.DeepEqual(args, []string{"show", testID}) {
			writeHelperJSON(map[string]any{"id": testID, "state": os.Getenv("TICKETCLIENT_HELPER_CONFLICT_STATE")})
		} else {
			writeHelperError("bad_args", fmt.Sprintf("queue conflict verification args = %q", args), nil)
		}
	case "single-response":
		_, _ = io.WriteString(os.Stdout, os.Getenv(helperResponseEnv)+"\n")
	case "malformed":
		_, _ = io.WriteString(os.Stdout, "{not-json}\n")
		time.Sleep(time.Hour)
	case "blocked-wait":
		_ = os.WriteFile(os.Getenv(helperReadyFileEnv), []byte("ready"), 0o600)
		time.Sleep(time.Hour)
	case "single-show":
		writeHelperJSON(map[string]any{"id": testID, "state": "open"})
	case "repository":
		writeHelperJSON(map[string]any{"id": testID, "title": os.Getenv("TICKET_REPOSITORY"), "state": "open"})
	case "target":
		writeHelperJSON(map[string]any{"id": testID, "title": os.Getenv("TICKET_REPOSITORY") + "|" + os.Getenv("TICKET_SCOPE") + "|" + os.Getenv("TICKET_ACTOR"), "state": "open"})
	case "repository-list":
		want := []string{"list", "--state", "open", "--state", StateClosed, "--limit", "2", "--offset", "1", "--fields", "id,title,state,priority,assignee,tags,parent,depends_on"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("list args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []map[string]any{{"id": testID, "title": "closed", "state": StateLegacyCompleted}}, "more": true})
	case "repository-list-filtered":
		want := []string{"list", testID, "20260919-10002", "--state", "open", "--state", "review", "--priority", "1", "--assignee", "coder", "--tag", "api", "--tag", "ticket-ui", "--limit", "2", "--offset", "1", "--fields", "id,title,state,priority,assignee,tags,parent,depends_on"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("filtered list args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []map[string]any{{"id": "20260919-10002", "title": "filtered", "state": "review", "priority": 1, "assignee": "coder", "tags": []string{"api", "ticket-ui"}, "parent": "20260920-00100"}}, "more": true})
	case "repository-list-all-filtered":
		want := []string{"list", "--state", "all", "--priority", "0", "--tag", "api", "--limit", "10", "--offset", "20", "--fields", "id,title,state,priority,assignee,tags,parent,depends_on"}
		if !reflect.DeepEqual(args, want) {
			writeHelperError("bad_args", fmt.Sprintf("all filtered list args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []map[string]any{{"id": testID, "state": "closed", "parent": "20260920-00100"}}, "more": true})
	case "repository-grep":
		if !reflect.DeepEqual(args, []string{"grep", "--", "parser"}) {
			writeHelperError("bad_args", fmt.Sprintf("grep args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []map[string]any{{"id": testID, "state": "open"}, {"id": "20260919-10002", "state": "review"}}})
	case "repository-grep-literal":
		if !reflect.DeepEqual(args, []string{"grep", "--", `-a\.b`}) {
			writeHelperError("bad_args", fmt.Sprintf("grep args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"items": []map[string]any{{"id": testID, "state": "open"}}})
	case "repository-grep-large":
		if !reflect.DeepEqual(args, []string{"grep", "--", "parser"}) {
			writeHelperError("bad_args", fmt.Sprintf("grep args = %q", args), nil)
			return -1
		}
		items := make([]map[string]any, maxSearchResultItems+1)
		for i := range items {
			items[i] = map[string]any{"id": fmt.Sprintf("20260919-%05d", i), "state": "open"}
		}
		writeHelperJSON(map[string]any{"items": items})
	case "mutation-input":
		if !reflect.DeepEqual(args, []string{"close", testID, "--input", "-"}) || stdin == nil || *stdin != `{"message":"ship it","outcome":"accepted"}` {
			value := "<nil>"
			if stdin != nil {
				value = *stdin
			}
			writeHelperError("bad_args", fmt.Sprintf("mutation request args=%q stdin=%q", args, value), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "changed": true, "from_state": "signoff", "state": StateClosed})
	case "mutation-review":
		if !reflect.DeepEqual(args, []string{"review", testID, "--message", "send it"}) {
			writeHelperError("bad_args", fmt.Sprintf("review args = %q", args), nil)
			return -1
		}
		writeHelperJSON(map[string]any{"id": testID, "changed": true, "from_state": "open", "state": "review"})
	case "detail":
		switch count {
		case 1:
			if !reflect.DeepEqual(args, []string{"show", testID, "--readiness"}) {
				writeHelperError("bad_args", fmt.Sprintf("detail show args = %q", args), nil)
				return -1
			}
			writeHelperJSON(map[string]any{"id": testID, "title": "detail", "state": StateLegacyCompleted, "readiness": map[string]any{"ready": false, "blockers": []any{map[string]any{"code": "dependency_open", "id": "20260919-10002", "message": "dependency is open"}}}, "sections": map[string]any{
				"objective": map[string]any{"text": "Inspect it"},
				"work_log":  map[string]any{"text": "history", "truncated": true},
			}})
		case 2:
			if !reflect.DeepEqual(args, []string{"status", testID}) {
				writeHelperError("bad_args", fmt.Sprintf("detail status args = %q", args), nil)
				return -1
			}
			writeHelperJSON(map[string]any{"id": testID, "created": "2026-09-19", "modified": "2026-09-20T12:30:00Z"})
		case 3:
			if !reflect.DeepEqual(args, []string{"show", testID, "--full", "--max-bytes", fmt.Sprint(RepositoryDetailBodyBudgetBytes)}) {
				writeHelperError("bad_args", fmt.Sprintf("detail body args = %q", args), nil)
				return -1
			}
			writeHelperJSON(map[string]any{"id": testID, "body": "Complete display body", "truncated": true})
		}
	case "detail-ready":
		if count == 1 {
			writeHelperJSON(map[string]any{"id": testID, "title": "ready", "state": "open", "readiness": map[string]any{"ready": true}})
		} else if count == 2 {
			writeHelperJSON(map[string]any{"id": testID, "created": "2026-09-19", "modified": "2026-09-20T12:30:00Z"})
		} else {
			writeHelperJSON(map[string]any{"id": testID, "body": "Ready body"})
		}
	case "detail-fail-show":
		writeHelperError("show_failed", "private raw output", nil)
	case "detail-fail-status":
		if count == 1 {
			writeHelperJSON(map[string]any{"id": testID, "state": "open", "readiness": map[string]any{"ready": true}})
		} else {
			writeHelperError("status_failed", "private raw output", nil)
		}
	case "detail-fail-body":
		if count == 1 {
			writeHelperJSON(map[string]any{"id": testID, "state": "open", "readiness": map[string]any{"ready": true}})
		} else if count == 2 {
			writeHelperJSON(map[string]any{"id": testID, "created": "2026-09-19", "modified": "2026-09-20T12:30:00Z"})
		} else {
			writeHelperError("body_failed", "private raw output", nil)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown helper scenario %q", scenario)
		return 82
	}
	return -1
}

func writeHelperError(code, message string, details map[string]any) {
	writeHelperJSON(map[string]any{"error": map[string]any{"code": code, "message": message, "details": details}})
}

func writeHelperJSON(value any) {
	data, _ := json.Marshal(value)
	data = append(data, '\n')
	_, _ = os.Stdout.Write(data)
}
