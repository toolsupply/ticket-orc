// Package contextheadroom defines normalized, optional context-window
// telemetry shared across harness adapters, orchestration, and persisted Orc
// state. It contains no adapter-specific acquisition logic.
package contextheadroom

import (
	"math"
	"time"
)

// Telemetry is one authoritative observation of a session's active context.
// Unknown observations contain no numeric estimates.
type Telemetry struct {
	Known      bool      `json:"known"`
	Used       int64     `json:"used,omitempty"`
	Window     int64     `json:"window,omitempty"`
	Remaining  int64     `json:"remaining,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
}

// Valid reports whether the observation is structurally safe to use. Invalid
// observations are treated as unknown by policy rather than estimated.
func (t Telemetry) Valid() bool {
	if !t.Known {
		return t.Used == 0 && t.Window == 0 && t.Remaining == 0 && t.ObservedAt.IsZero()
	}
	const maxWindow = math.MaxInt64 / 100
	return t.Window > 0 && t.Window <= maxWindow && t.Used >= 0 && t.Used <= t.Window &&
		t.Remaining >= 0 && t.Remaining <= t.Window && !t.ObservedAt.IsZero()
}

// ReuseAllowed applies an optional minimum remaining-context percentage. It
// returns known=false for unknown, malformed, or stale observations; those
// cases preserve the non-context Stage 1 reuse decision. Fresh authoritative
// data below the threshold requires session replacement.
func ReuseAllowed(t Telemetry, minimumPercent int, now time.Time) (known, allowed bool) {
	if !t.Known || !t.Valid() || minimumPercent < 0 || minimumPercent > 100 || now.Before(t.ObservedAt) || now.Sub(t.ObservedAt) > 24*time.Hour {
		return false, true
	}
	return true, t.Remaining*100 >= t.Window*int64(minimumPercent)
}
