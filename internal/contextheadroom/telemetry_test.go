package contextheadroom

import (
	"testing"
	"time"
)

func TestReuseAllowed(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		value   Telemetry
		minimum int
		known   bool
		allowed bool
	}{
		{"above threshold", Telemetry{Known: true, Used: 600, Window: 1000, Remaining: 400, ObservedAt: now}, 20, true, true},
		{"at threshold", Telemetry{Known: true, Used: 800, Window: 1000, Remaining: 200, ObservedAt: now}, 20, true, true},
		{"below threshold", Telemetry{Known: true, Used: 801, Window: 1000, Remaining: 199, ObservedAt: now}, 20, true, false},
		{"unknown stays stage one", Telemetry{}, 20, false, true},
		{"malformed stays stage one", Telemetry{Known: true, Used: 1100, Window: 1000, Remaining: 100, ObservedAt: now}, 20, false, true},
		{"stale stays stage one", Telemetry{Known: true, Used: 900, Window: 1000, Remaining: 100, ObservedAt: now.Add(-25 * time.Hour)}, 20, false, true},
		{"future observation stays stage one", Telemetry{Known: true, Used: 900, Window: 1000, Remaining: 100, ObservedAt: now.Add(time.Second)}, 20, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			known, allowed := ReuseAllowed(test.value, test.minimum, now)
			if known != test.known || allowed != test.allowed {
				t.Fatalf("ReuseAllowed = (%v, %v), want (%v, %v)", known, allowed, test.known, test.allowed)
			}
		})
	}
}
