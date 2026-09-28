package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseReviewReportConfigRequiresReviewSubcommand(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == "TICKET_ACTOR" {
			return "coder", true
		}
		return "", false
	}
	config, help, err := parseReviewReportConfig([]string{"review", "--since", "14d", "--limit=12", "--output", "json"}, lookup)
	if err != nil || help {
		t.Fatalf("config=%#v help=%v err=%v", config, help, err)
	}
	if config.Actor != "coder" || config.Since != 14*24*time.Hour || config.Limit != 12 || config.Output != string(OutputJSON) {
		t.Fatalf("config=%#v", config)
	}
	if _, _, err := parseReviewReportConfig([]string{"--since", "400d"}, lookup); err == nil {
		t.Fatal("unbounded report window accepted")
	}
	if _, _, err := parseReviewReportConfig(nil, lookup); err == nil || !strings.Contains(err.Error(), "requires the review subcommand") {
		t.Fatalf("missing subcommand error=%v", err)
	}
	if _, _, err := parseReviewReportConfig([]string{"reviews"}, lookup); err == nil {
		t.Fatal("prerelease reviews alias accepted")
	}
	if _, help, err := parseReviewReportConfig([]string{"--help"}, lookup); err != nil || !help {
		t.Fatalf("report --help: help=%v err=%v", help, err)
	}
	if _, help, err := parseReviewReportConfig([]string{"review", "--help"}, lookup); err != nil || !help {
		t.Fatalf("report review --help: help=%v err=%v", help, err)
	}
}

func TestParseMarkedReviewLogAndAggregateReviewRoundTrips(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	text := strings.Join([]string{
		"- 2026-09-01T01:00:00Z coder: ticket-orc review: submitted",
		"- 2026-09-01T03:00:00Z reviewer: ticket-orc review: approved",
		"- 2026-09-01T04:00:00Z coder: ticket-orc review: submitted",
		"- 2026-09-01T05:00:00Z reviewer: ticket-orc review: returned",
		"- 2026-09-01T06:00:00Z coder: ticket-orc review: submitted",
		"- 2026-09-01T09:00:00Z reviewer: ticket-orc review: approved",
	}, "\n")
	parsed := parseReviewLog("20260901-00001", text, start, start.Add(24*time.Hour))
	if len(parsed.Entries) != 6 || parsed.Entries[0].Actor != "coder" || parsed.Ambiguous != 0 {
		t.Fatalf("parsed=%#v", parsed)
	}
	report := reviewReport{}
	aggregateReviewReport(&report, map[string]reviewTicketEvidence{"20260901-00001": {Entries: parsed.Entries}})
	if report.FirstPassApprovals != 1 || report.ReturnedToOpen != 1 || report.Resubmissions != 1 || report.RoundTrips != 3 || report.TicketsWithIncomplete != 0 {
		t.Fatalf("aggregate=%#v", report)
	}
	wantDurations := []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour}
	for i, want := range wantDurations {
		if report.ElapsedRoundTrips[i] != want {
			t.Fatalf("duration[%d]=%s want=%s", i, report.ElapsedRoundTrips[i], want)
		}
	}
}

func TestReviewReportIgnoresArbitraryMessagesAndMarksIncompleteEvidence(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	arbitrary := parseReviewLog("20260901-00002", strings.Join([]string{
		"- 2026-09-01T01:00:00Z coder: ready for review",
		"- 2026-09-01T02:00:00Z reviewer: approved first pass",
	}, "\n"), start, start.Add(24*time.Hour))
	if len(arbitrary.Entries) != 0 || arbitrary.Ambiguous != 0 || arbitrary.Missing || !arbitrary.Unmarked {
		t.Fatalf("arbitrary messages became evidence: %#v", arbitrary)
	}
	missing := parseReviewLog("20260901-00003", "", start, start.Add(24*time.Hour))
	malformed := parseReviewLog("20260901-00004", "- 2026-09-01T01:00:00Z reviewer: ticket-orc review: approved with caveat", start, start.Add(24*time.Hour))
	if !missing.Missing || malformed.Ambiguous != 1 {
		t.Fatalf("missing=%#v malformed=%#v", missing, malformed)
	}
	entries := parseReviewLog("20260901-00005", "- 2026-09-01T01:00:00Z coder: ticket-orc review: submitted", start, start.Add(24*time.Hour))
	report := reviewReport{}
	aggregateReviewReport(&report, map[string]reviewTicketEvidence{
		"20260901-00002": {Unmarked: arbitrary.Unmarked},
		"20260901-00003": {Missing: missing.Missing},
		"20260901-00004": {Ambiguous: malformed.Ambiguous},
		"20260901-00005": {Entries: entries.Entries},
	})
	if report.MissingWorkLogs != 1 || report.UnmarkedWorkLogs != 1 || report.AmbiguousWorkLogEntries != 1 || report.IncompleteReviewRoundTrips != 1 || report.TicketsWithIncomplete != 4 {
		t.Fatalf("incomplete evidence aggregate=%#v", report)
	}
}

func TestReviewReportRequiresExactMarkersAndOrder(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	caseVariant := parseReviewLog("20260901-00006", "- 2026-09-01T01:00:00Z coder: Ticket-Orc review: submitted", start, start.Add(24*time.Hour))
	if len(caseVariant.Entries) != 0 || caseVariant.Markers != 0 || !caseVariant.Unmarked {
		t.Fatalf("capitalized marker became evidence: %#v", caseVariant)
	}
	returned := parseReviewLog("20260901-00007", "- 2026-09-01T01:00:00Z reviewer: ticket-orc review: returned", start, start.Add(24*time.Hour))
	report := reviewReport{}
	aggregateReviewReport(&report, map[string]reviewTicketEvidence{"20260901-00007": {Entries: returned.Entries}})
	if report.ReturnedToOpen != 1 || report.AmbiguousWorkLogEntries != 1 || report.TicketsWithIncomplete != 1 {
		t.Fatalf("out-of-order return was treated as complete: %#v", report)
	}
}

func TestRenderReviewReportHandlesNoDataAndStatesLimitations(t *testing.T) {
	var output bytes.Buffer
	renderReviewReport(&output, reviewReport{WindowStart: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), WindowEnd: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)})
	text := output.String()
	for _, want := range []string{"No review activity was found", "Markers:", "no actor score", "code-quality"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q: %q", want, text)
		}
	}
}
