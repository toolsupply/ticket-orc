package orc

import "testing"

func TestAdvanceEvidenceTable(t *testing.T) {
	cases := []struct {
		name          string
		state         SteerDeliveryState
		active, ready bool
		next          SteerDeliveryState
		queue, retire bool
	}{
		{"idle ready sends", SteerIdle, false, true, SteerSending, true, false},
		{"idle active is busy", SteerIdle, true, true, SteerIdle, false, false},
		{"queued no active waits", SteerQueued, false, true, SteerQueued, false, false},
		{"queued active consumes", SteerQueued, true, true, SteerConsumed, false, false},
		{"consumed active busy", SteerConsumed, true, true, SteerConsumed, false, false},
		{"consumed ready sends fresh", SteerConsumed, false, true, SteerSending, true, true},
		{"consumed idle retires", SteerConsumed, false, false, SteerIdle, false, true},
		{"uncertain never resends", SteerSending, false, true, SteerSending, false, false},
		{"degraded never retries", SteerDegraded, false, true, SteerDegraded, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := AdvanceSteerDelivery(tc.state, Evidence{Active: tc.active, Ready: tc.ready})
			if got.Next != tc.next || got.Queue != tc.queue || got.Retire != tc.retire {
				t.Fatalf("Advance = %#v", got)
			}
		})
	}
}
