package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/executionlog"
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	MaxLogRecordBytes   = 1 << 20
	maxLogQueryBytes    = 16 << 10
	maxLogFilterValues  = 256 // Counts supplied values, including duplicates, across fields.
	maxLogWorkerBytes   = 128
	maxLogSubscriptions = 16
	logWriteTimeout     = 5 * time.Second
)

// LogSource is wired by the runtime composition layer. Inventory only copies
// authoritative in-memory state; it must not probe Ticket or perform file IO.
// LocalDir is private server configuration, never a request parameter.
type LogSource struct {
	LocalDir  string
	Inventory func() LogInventory
}

type LogInventory struct {
	Workers       []executionlog.Worker
	RepositoryIDs []string
}

type LogCapabilities struct {
	Stream         bool     `json:"stream"`
	SchemaVersion  int      `json:"schema_version,omitempty"`
	Kinds          []string `json:"kinds,omitempty"`
	DefaultKinds   []string `json:"default_kinds,omitempty"`
	Harnesses      []string `json:"harnesses,omitempty"`
	Replay         bool     `json:"replay"`
	MaxRecordBytes int      `json:"max_record_bytes,omitempty"`
	// External steer sessions do not expose managed execution logs.
	ExternalSteer bool `json:"external_steer"`
}

type LogsReady struct {
	SchemaVersion int      `json:"schema_version"`
	InstanceID    string   `json:"instance_id"`
	Workers       []string `json:"workers"`
	Kinds         []string `json:"kinds"`
	Replay        bool     `json:"replay"`
}

// LogRecord is a transport projection of one semantic observation. Provider
// content remains JSON data; terminal rendering and display limits do not apply.
type LogRecord struct {
	InstanceID   string    `json:"instance_id"`
	Worker       string    `json:"worker"`
	RepositoryID string    `json:"repository_id"`
	Harness      string    `json:"harness"`
	TicketID     string    `json:"ticket_id"`
	ExecutionID  string    `json:"execution_id"`
	ObservedAt   time.Time `json:"observed_at"`
	harness.LogRecord
}

type LogsError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func logKinds() []string        { return []string{"message", "diagnostic", "command", "file_change"} }
func logDefaultKinds() []string { return []string{"message", "diagnostic"} }

type logFilters struct{ workers, repositories, kinds []string }

func parseLogFilters(raw string) (logFilters, error) {
	invalid := errors.New("invalid log filters")
	if len(raw) > maxLogQueryBytes {
		return logFilters{}, invalid
	}
	query, err := url.ParseQuery(raw)
	if err != nil {
		return logFilters{}, invalid
	}
	f := logFilters{}
	count := 0
	for key, values := range query {
		count += len(values)
		if count > maxLogFilterValues {
			return f, invalid
		}
		var target *[]string
		switch key {
		case "worker":
			target = &f.workers
		case "repository_id":
			target = &f.repositories
		case "kind":
			target = &f.kinds
		default:
			return f, invalid
		}
		for _, value := range values {
			switch key {
			case "worker":
				if !validLogWorker(value) {
					return f, invalid
				}
			case "repository_id":
				if !ticketclient.ValidRepositoryID(value) {
					return f, invalid
				}
			case "kind":
				if !slices.Contains(logKinds(), value) {
					return f, invalid
				}
			}
			*target = append(*target, value)
		}
		slices.Sort(*target)
		*target = slices.Compact(*target)
	}
	if len(f.kinds) == 0 {
		f.kinds = logDefaultKinds()
	}
	// Canonical protocol order, independent of query spelling/order.
	kinds := []string{}
	for _, kind := range logKinds() {
		if slices.Contains(f.kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	f.kinds = kinds
	return f, nil
}

func validLogWorker(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > maxLogWorkerBytes {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// selectWorkers also retains explicitly named but intersected-out definitions,
// so a change to those identities invalidates an otherwise idle subscription.
func (f logFilters) selectWorkers(inventory LogInventory) (selected []string, definitions []executionlog.Worker, code string) {
	selected = []string{}
	known := map[string]bool{}
	repositories := map[string]bool{}
	for _, id := range inventory.RepositoryIDs {
		repositories[id] = true
	}
	for _, worker := range inventory.Workers {
		known[worker.Name] = true
		repositories[worker.RepositoryID] = true
		if len(f.workers) != 0 && !slices.Contains(f.workers, worker.Name) {
			continue
		}
		definitions = append(definitions, worker)
		if len(f.repositories) == 0 || slices.Contains(f.repositories, worker.RepositoryID) {
			selected = append(selected, worker.Name)
		}
	}
	for _, name := range f.workers {
		if !known[name] {
			return nil, nil, "worker_not_found"
		}
	}
	for _, id := range f.repositories {
		if !repositories[id] {
			return nil, nil, "repository_not_found"
		}
	}
	slices.Sort(selected)
	slices.SortFunc(definitions, func(a, b executionlog.Worker) int { return strings.Compare(a.Name, b.Name) })
	return selected, definitions, ""
}

var errLogInventoryChanged = errors.New("log inventory changed")

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.config.Logs == nil || s.config.Logs.Inventory == nil {
		writeError(w, http.StatusUnprocessableEntity, "capability_unavailable", "log streaming is unavailable")
		return
	}
	for _, value := range r.Header.Values("Last-Event-ID") {
		if value != "" {
			writeError(w, http.StatusBadRequest, "resume_unsupported", "log streams do not support resumption")
			return
		}
	}
	filters, err := parseLogFilters(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", "invalid log filters or query limits exceeded")
		return
	}
	inventory := s.config.Logs.Inventory()
	if executionlog.ValidateCatalog(inventory.Workers) != nil {
		writeError(w, http.StatusServiceUnavailable, "log_source_unavailable", "managed log inventory is unavailable")
		return
	}
	selected, definitions, code := filters.selectWorkers(inventory)
	if code != "" {
		writeError(w, http.StatusNotFound, code, "unknown log worker or repository")
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "daemon_stopping", "daemon is stopping")
		return
	}
	if s.logSubscriptions >= maxLogSubscriptions {
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, "subscription_limit", "too many log subscriptions")
		return
	}
	s.logSubscriptions++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.logSubscriptions--; s.mu.Unlock() }()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if s.lifecycle != nil {
		stop := context.AfterFunc(s.lifecycle, cancel)
		defer stop()
	}
	controller := http.NewResponseController(w)
	// Establish deadline support before committing streaming headers.
	if controller.SetWriteDeadline(time.Now().Add(logWriteTimeout)) != nil {
		writeError(w, http.StatusInternalServerError, "stream_unavailable", "bounded log streaming is unavailable")
		return
	}
	defer controller.SetWriteDeadline(time.Time{})
	canceledWrite := make(chan struct{})
	stopWrite := context.AfterFunc(ctx, func() { _ = controller.SetWriteDeadline(time.Now()); close(canceledWrite) })
	defer func() {
		if !stopWrite() {
			<-canceledWrite
		}
	}()
	checkInventory := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := s.config.Logs.Inventory()
		if executionlog.ValidateCatalog(current.Workers) != nil {
			return errLogInventoryChanged
		}
		names, defs, code := filters.selectWorkers(current)
		if code != "" || !slices.Equal(names, selected) || !slices.Equal(defs, definitions) {
			return errLogInventoryChanged
		}
		return nil
	}
	writeFrame := func(frame []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := controller.SetWriteDeadline(time.Now().Add(logWriteTimeout)); err != nil {
			return err
		}
		// Cancellation may have fired just before setting the new deadline. Do not
		// rearm a canceled writer after its cancellation callback expired it.
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := w.Write(frame); err != nil {
			return err
		}
		return controller.Flush()
	}
	write := func(event string, data []byte) error {
		frame := append([]byte("event: "+event+"\ndata: "), data...)
		return writeFrame(append(frame, '\n', '\n'))
	}
	follower, err := executionlog.New(s.config.Logs.LocalDir, inventory.Workers, selected, func(observation executionlog.Observation) error {
		if err := checkInventory(); err != nil {
			return err
		}
		if !slices.Contains(filters.kinds, observation.Record.Kind) {
			return nil
		}
		data, err := marshalLogRecord(s.config.InstanceID, observation)
		if err != nil {
			return err
		}
		return write("log", data)
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "log_source_unavailable", "managed log source is unavailable")
		return
	}
	defer follower.Close()
	if err := checkInventory(); err != nil {
		writeError(w, http.StatusConflict, "inventory_changed", "managed log inventory changed; refresh and reconnect")
		return
	}
	finish := func(err error) {
		if ctx.Err() != nil {
			return
		}
		failure := LogsError{Code: "log_source_unavailable", Message: "managed log source is unavailable"}
		if errors.Is(err, errLogInventoryChanged) {
			failure = LogsError{Code: "inventory_changed", Message: "managed log inventory changed; refresh and reconnect"}
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			failure = LogsError{Code: "slow_consumer", Message: "log consumer exceeded the write deadline"}
			// A timed-out transport may still permit a small final error. Never wait
			// for another full deadline just to report a slow client to itself.
			_ = controller.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			data, _ := json.Marshal(failure)
			_, _ = w.Write(append(append([]byte("event: logs.error\ndata: "), data...), '\n', '\n'))
			_ = controller.Flush()
			return
		}
		data, _ := json.Marshal(failure)
		_ = write("logs.error", data)
	}
	ready, _ := json.Marshal(LogsReady{SchemaVersion: 1, InstanceID: s.config.InstanceID, Workers: selected, Kinds: filters.kinds})
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	if err := write("logs.ready", ready); err != nil {
		finish(err)
		return
	}
	poll := time.NewTicker(executionlog.PollInterval)
	defer poll.Stop()
	heartbeat := time.NewTicker(s.config.EventHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-poll.C:
			if err := checkInventory(); err != nil {
				finish(err)
				return
			}
			if err := follower.Poll(now); err != nil {
				finish(err)
				return
			}
		case <-heartbeat.C:
			if err := writeFrame([]byte(": heartbeat\n\n")); err != nil {
				finish(err)
				return
			}
		}
	}
}

func marshalLogRecord(instanceID string, observation executionlog.Observation) ([]byte, error) {
	record := LogRecord{InstanceID: instanceID, Worker: observation.Worker, RepositoryID: observation.RepositoryID, Harness: observation.Harness, TicketID: observation.TicketID, ExecutionID: observation.ExecutionID, ObservedAt: observation.ObservedAt.UTC(), LogRecord: observation.Record}
	// The follower preserves its terminal viewer's diagnostic convention. The
	// HTTP API uses a fixed bounded diagnostic without parser/provider details.
	if record.Code == "invalid_event" {
		record.LogRecord = harness.LogRecord{Kind: "diagnostic", Level: "warning", Code: "malformed_event", Text: "malformed provider event"}
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(data) <= MaxLogRecordBytes {
		return data, nil
	}
	record.LogRecord = harness.LogRecord{Kind: "diagnostic", Level: "warning", Code: "record_too_large", Text: "semantic record exceeds 1048576 JSON bytes"}
	return json.Marshal(record)
}
