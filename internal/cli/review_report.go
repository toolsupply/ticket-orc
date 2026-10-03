package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	defaultReviewReportWindow = 30 * 24 * time.Hour
	maxReviewReportWindow     = 365 * 24 * time.Hour
	defaultReviewReportLimit  = 512
	maxReviewReportLimit      = 4096
)

type ReviewReportConfig struct {
	Since      time.Duration
	Actor      string
	WorkingDir string
	Target     ticketclient.Target
	Limit      int
	Output     string
	Subcommand string
}

type reviewReport struct {
	WindowStart                time.Time       `json:"window_start"`
	WindowEnd                  time.Time       `json:"window_end"`
	TicketsExamined            int             `json:"tickets_examined"`
	TicketsWithActivity        int             `json:"tickets_with_review_activity"`
	FirstPassApprovals         int             `json:"first_pass_approvals"`
	ReturnedToOpen             int             `json:"returned_to_open"`
	Resubmissions              int             `json:"resubmissions"`
	RoundTrips                 int             `json:"review_round_trips"`
	ElapsedRoundTrips          []time.Duration `json:"-"`
	AverageRoundTripSec        int64           `json:"average_round_trip_seconds,omitempty"`
	MinRoundTripSec            int64           `json:"min_round_trip_seconds,omitempty"`
	MaxRoundTripSec            int64           `json:"max_round_trip_seconds,omitempty"`
	MissingWorkLogs            int             `json:"missing_work_logs,omitempty"`
	UnmarkedWorkLogs           int             `json:"unmarked_work_logs,omitempty"`
	TruncatedWorkLogs          int             `json:"truncated_work_logs,omitempty"`
	AmbiguousWorkLogEntries    int             `json:"ambiguous_work_log_entries,omitempty"`
	IncompleteReviewRoundTrips int             `json:"incomplete_review_round_trips,omitempty"`
	TicketsWithIncomplete      int             `json:"tickets_with_incomplete_evidence,omitempty"`
}

type reviewLogEntry struct {
	TicketID string
	At       time.Time
	Actor    string
	Message  string
	Kind     reviewEventKind
}

type reviewEventKind string

const (
	reviewEventSubmit  reviewEventKind = "submitted"
	reviewEventApprove reviewEventKind = "approved"
	reviewEventReturn  reviewEventKind = "returned"
)

var reviewLogTimestamp = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2}T[^ ]+)\s+(.+)$`)

const reviewMarkerPrefix = "ticket-orc review:"

type reviewLogParse struct {
	Entries   []reviewLogEntry
	Missing   bool
	Unmarked  bool
	Ambiguous int
	Markers   int
}

type reviewTicketEvidence struct {
	Entries   []reviewLogEntry
	Missing   bool
	Unmarked  bool
	Truncated bool
	Ambiguous int
}

func parseReviewReportConfig(args []string, lookupEnv envLookup) (ReviewReportConfig, bool, error) {
	workingDir, _ := os.Getwd()
	config := ReviewReportConfig{Since: defaultReviewReportWindow, Limit: defaultReviewReportLimit, Output: string(OutputCompact), WorkingDir: workingDir}
	if lookupEnv != nil {
		config.Actor, _ = lookupEnv("TICKET_ACTOR")
	}
	if config.Actor == "" {
		config.Actor = "ticket-orc-report"
	}
	seen := make(map[string]bool)
	help := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" {
			help = true
			continue
		}
		if !strings.HasPrefix(arg, "--") {
			if config.Subcommand != "" || arg != "review" {
				return ReviewReportConfig{}, false, fmt.Errorf("report accepts only the review subcommand")
			}
			config.Subcommand = arg
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if seen[name] {
			return ReviewReportConfig{}, false, fmt.Errorf("duplicate flag --%s", name)
		}
		seen[name] = true
		if name != "since" && name != "actor" && name != "repository" && name != "limit" && name != "output" {
			return ReviewReportConfig{}, false, fmt.Errorf("unknown flag --%s", name)
		}
		if !hasValue {
			i++
			if i >= len(args) {
				return ReviewReportConfig{}, false, fmt.Errorf("flag --%s requires a value", name)
			}
			value = args[i]
		}
		if strings.TrimSpace(value) == "" {
			return ReviewReportConfig{}, false, fmt.Errorf("--%s must not be empty", name)
		}
		switch name {
		case "since":
			duration, err := parseReviewDuration(value)
			if err != nil {
				return ReviewReportConfig{}, false, err
			}
			config.Since = duration
		case "actor":
			config.Actor = value
		case "repository":
			config.Target = ticketclient.Target{Repository: value}
		case "limit":
			limit, err := strconv.Atoi(value)
			if err != nil || limit < 1 || limit > maxReviewReportLimit {
				return ReviewReportConfig{}, false, fmt.Errorf("--limit must be between 1 and %d", maxReviewReportLimit)
			}
			config.Limit = limit
		case "output":
			if value != string(OutputCompact) && value != string(OutputJSON) {
				return ReviewReportConfig{}, false, fmt.Errorf("--output must be compact or json")
			}
			config.Output = value
		}
	}
	if config.Subcommand == "" && !help {
		return ReviewReportConfig{}, false, fmt.Errorf("report requires the review subcommand")
	}
	if help {
		return ReviewReportConfig{}, true, nil
	}
	if config.Since <= 0 || config.Since > maxReviewReportWindow {
		return ReviewReportConfig{}, false, fmt.Errorf("--since must be between 1h and 365d")
	}
	return config, false, nil
}

func parseReviewDuration(value string) (time.Duration, error) {
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(value, "d"), 64)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("--since must be a positive duration such as 30d or 720h")
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("--since must be a positive duration such as 30d or 720h")
	}
	return duration, nil
}

func executeReviewReport(ctx context.Context, config ReviewReportConfig, stdout io.Writer) error {
	client, err := ticketclient.NewWithWorkingDirAndTarget(config.Actor, config.WorkingDir, config.Target)
	if err != nil {
		return fmt.Errorf("start Ticket report session: %w", err)
	}
	defer client.Close()
	now := time.Now().UTC()
	report := reviewReport{WindowStart: now.Add(-config.Since), WindowEnd: now}
	tickets, err := client.ListAllForReport(ctx, config.Limit)
	if err != nil {
		return fmt.Errorf("list Ticket history: %w", err)
	}
	entriesByTicket := make(map[string]reviewTicketEvidence)
	for _, ticket := range tickets {
		report.TicketsExamined++
		view, showErr := client.ShowWorkLog(ctx, ticket.ID)
		if showErr != nil {
			return fmt.Errorf("read Ticket work log for %s: %w", ticket.ID, showErr)
		}
		parsed := parseReviewLog(view.ID, view.WorkLog, report.WindowStart, report.WindowEnd)
		entriesByTicket[view.ID] = reviewTicketEvidence{Entries: parsed.Entries, Missing: parsed.Missing, Unmarked: parsed.Unmarked, Truncated: view.Truncated, Ambiguous: parsed.Ambiguous}
		if len(parsed.Entries) > 0 {
			report.TicketsWithActivity++
		}
	}
	aggregateReviewReport(&report, entriesByTicket)
	if config.Output == string(OutputJSON) {
		return json.NewEncoder(stdout).Encode(report)
	}
	renderReviewReport(stdout, report)
	return nil
}

func parseReviewLog(ticketID, text string, start, end time.Time) reviewLogParse {
	parsed := reviewLogParse{}
	if strings.TrimSpace(text) == "" {
		parsed.Missing = true
		return parsed
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		match := reviewLogTimestamp.FindStringSubmatch(line)
		if len(match) != 3 {
			if strings.Contains(strings.ToLower(line), reviewMarkerPrefix) {
				parsed.Ambiguous++
			}
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, match[1])
		separator := strings.Index(match[2], ":")
		if separator <= 0 {
			if strings.Contains(strings.ToLower(match[2]), reviewMarkerPrefix) {
				parsed.Ambiguous++
			}
			continue
		}
		actor := strings.TrimSpace(match[2][:separator])
		message := strings.TrimSpace(match[2][separator+1:])
		kind, explicit := parseReviewMarker(message)
		if !explicit {
			continue
		}
		parsed.Markers++
		if err != nil || actor == "" || kind == "" {
			parsed.Ambiguous++
			continue
		}
		if at.Before(start) || at.After(end) {
			continue
		}
		parsed.Entries = append(parsed.Entries, reviewLogEntry{TicketID: ticketID, At: at, Actor: actor, Message: message, Kind: kind})
	}
	if parsed.Markers == 0 {
		parsed.Unmarked = true
	}
	sort.SliceStable(parsed.Entries, func(i, j int) bool { return parsed.Entries[i].At.Before(parsed.Entries[j].At) })
	return parsed
}

func parseReviewMarker(message string) (reviewEventKind, bool) {
	normalized := strings.TrimSpace(message)
	if !strings.HasPrefix(normalized, reviewMarkerPrefix) {
		return "", false
	}
	switch strings.TrimSpace(strings.TrimPrefix(normalized, reviewMarkerPrefix)) {
	case "submitted":
		return reviewEventSubmit, true
	case "approved":
		return reviewEventApprove, true
	case "returned":
		return reviewEventReturn, true
	default:
		return "", true
	}
}

func aggregateReviewReport(report *reviewReport, evidenceByTicket map[string]reviewTicketEvidence) {
	for _, evidence := range evidenceByTicket {
		incomplete := evidence.Missing || evidence.Truncated || evidence.Ambiguous > 0
		if evidence.Missing {
			report.MissingWorkLogs++
		}
		if evidence.Unmarked {
			report.UnmarkedWorkLogs++
			incomplete = true
		}
		if evidence.Truncated {
			report.TruncatedWorkLogs++
		}
		report.AmbiguousWorkLogEntries += evidence.Ambiguous
		ambiguous := evidence.Ambiguous
		entries := evidence.Entries
		returned := false
		firstApproval := false
		var pending *reviewLogEntry
		for i := range entries {
			entry := entries[i]
			switch entry.Kind {
			case reviewEventSubmit:
				if pending != nil {
					ambiguous++
					pending = nil
				}
				if returned {
					report.Resubmissions++
				}
				pending = &entry
			case reviewEventApprove:
				if pending == nil {
					ambiguous++
					continue
				}
				if !returned && !firstApproval {
					report.FirstPassApprovals++
					firstApproval = true
				}
				report.RoundTrips++
				report.ElapsedRoundTrips = append(report.ElapsedRoundTrips, entry.At.Sub(pending.At))
				pending = nil
			case reviewEventReturn:
				report.ReturnedToOpen++
				if pending == nil {
					ambiguous++
				}
				returned = true
				if pending != nil {
					report.RoundTrips++
					report.ElapsedRoundTrips = append(report.ElapsedRoundTrips, entry.At.Sub(pending.At))
					pending = nil
				}
			}
		}
		if pending != nil {
			report.IncompleteReviewRoundTrips++
			incomplete = true
		}
		if ambiguous > evidence.Ambiguous {
			report.AmbiguousWorkLogEntries += ambiguous - evidence.Ambiguous
			incomplete = true
		}
		if incomplete {
			report.TicketsWithIncomplete++
		}
	}
	sort.Slice(report.ElapsedRoundTrips, func(i, j int) bool { return report.ElapsedRoundTrips[i] < report.ElapsedRoundTrips[j] })
	if len(report.ElapsedRoundTrips) > 0 {
		var total time.Duration
		for _, elapsed := range report.ElapsedRoundTrips {
			total += elapsed
		}
		report.AverageRoundTripSec = int64((total / time.Duration(len(report.ElapsedRoundTrips))).Round(time.Second) / time.Second)
		report.MinRoundTripSec = int64(report.ElapsedRoundTrips[0].Round(time.Second) / time.Second)
		report.MaxRoundTripSec = int64(report.ElapsedRoundTrips[len(report.ElapsedRoundTrips)-1].Round(time.Second) / time.Second)
	}
}

func renderReviewReport(out io.Writer, report reviewReport) {
	fmt.Fprintln(out, "Ticket review flow report")
	fmt.Fprintf(out, "Window: %s to %s\n", report.WindowStart.Local().Format(time.RFC3339), report.WindowEnd.Local().Format(time.RFC3339))
	fmt.Fprintf(out, "Tickets examined: %d\n", report.TicketsExamined)
	fmt.Fprintf(out, "Tickets with review activity: %d\n", report.TicketsWithActivity)
	fmt.Fprintf(out, "First-pass approvals/signoffs: %d\n", report.FirstPassApprovals)
	fmt.Fprintf(out, "Returned to open: %d\n", report.ReturnedToOpen)
	fmt.Fprintf(out, "Resubmissions after return: %d\n", report.Resubmissions)
	fmt.Fprintf(out, "Review round trips: %d\n", report.RoundTrips)
	fmt.Fprintf(out, "Incomplete review round trips: %d\n", report.IncompleteReviewRoundTrips)
	fmt.Fprintf(out, "Tickets with incomplete evidence: %d\n", report.TicketsWithIncomplete)
	if len(report.ElapsedRoundTrips) == 0 {
		fmt.Fprintln(out, "Elapsed review round trips: no completed round trips")
	} else {
		var total time.Duration
		for _, elapsed := range report.ElapsedRoundTrips {
			total += elapsed
		}
		fmt.Fprintf(out, "Elapsed review round trips: average %s, min %s, max %s\n", (total / time.Duration(len(report.ElapsedRoundTrips))).Round(time.Second), report.ElapsedRoundTrips[0].Round(time.Second), report.ElapsedRoundTrips[len(report.ElapsedRoundTrips)-1].Round(time.Second))
	}
	if report.TicketsWithActivity == 0 {
		fmt.Fprintln(out, "No review activity was found in the selected window.")
	}
	if report.TruncatedWorkLogs > 0 {
		fmt.Fprintf(out, "Note: %d work log(s) reached Ticket's response budget and may be incomplete.\n", report.TruncatedWorkLogs)
	}
	if report.MissingWorkLogs > 0 || report.UnmarkedWorkLogs > 0 || report.AmbiguousWorkLogEntries > 0 {
		fmt.Fprintf(out, "Unknown evidence: %d work log(s) missing; %d unmarked; %d marker entries ambiguous.\n", report.MissingWorkLogs, report.UnmarkedWorkLogs, report.AmbiguousWorkLogEntries)
	}
	fmt.Fprintln(out, "Markers: only exact `ticket-orc review: submitted`, `ticket-orc review: approved`, and `ticket-orc review: returned` entries are counted; arbitrary prose is ignored. The report produces no actor score or code-quality judgment.")
	fmt.Fprintln(out, "Limitations: lifecycle changes without one of these explicit Work log markers cannot be recovered from the public Ticket projection.")
}
