package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSteerRuntimeTargetQueueReservationAndRetirement(t *testing.T) {
	for _, queue := range []string{"signoff", "hold"} {
		for _, outcome := range []string{"clear", "reject"} {
			t.Run(queue+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				reg, _, _, err := NewRegistrationStore(dir).Join(ctx, testSteerRegistration(t))
				if err != nil {
					t.Fatal(err)
				}
				store := NewSteerRuntimeStore(dir)
				if err := store.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
					t.Fatal(err)
				}
				const target = "20261025-12345"
				if reserved, err := store.ReserveSignoffTarget(ctx, reg, target, queue); err != nil || !reserved {
					t.Fatalf("reserve=%t err=%v", reserved, err)
				}
				// A separately constructed store must observe both fields before any send.
				snapshot, err := NewSteerRuntimeStore(dir).Snapshot(ctx)
				if err != nil || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].TargetTicket != target || snapshot.Deliveries[0].TargetQueue != queue || snapshot.Deliveries[0].State != "sending" {
					t.Fatalf("reservation=%#v err=%v", snapshot, err)
				}
				if outcome == "clear" {
					if complete, err := store.CompleteSignoffDeliveryWithIntents(ctx, reg, target, true, false); err != nil || !complete {
						t.Fatalf("complete=%t err=%v", complete, err)
					}
					snapshot, err = NewSteerRuntimeStore(dir).Snapshot(ctx)
					if err != nil || snapshot.Deliveries[0].TargetQueue != queue || snapshot.Deliveries[0].State != "queued" {
						t.Fatalf("accepted target=%#v err=%v", snapshot, err)
					}
					if cleared, err := store.ClearSignoffTarget(ctx, reg, target); err != nil || !cleared {
						t.Fatalf("clear=%t err=%v", cleared, err)
					}
				} else if rejected, err := store.RejectSignoffDelivery(ctx, reg, target); err != nil || !rejected {
					t.Fatalf("reject=%t err=%v", rejected, err)
				}
				data, err := os.ReadFile(filepath.Join(dir, steerRuntimeFileName))
				if err != nil {
					t.Fatal(err)
				}
				retainedHold := queue == "hold" && outcome == "reject"
				if retainedHold && (!strings.Contains(string(data), "target_ticket") || !strings.Contains(string(data), "target_queue")) {
					t.Fatalf("rejected hold lost target evidence: %s", data)
				}
				if !retainedHold && (strings.Contains(string(data), "target_ticket") || strings.Contains(string(data), "target_queue")) {
					t.Fatalf("retirement left target fields: %s", data)
				}
				if _, err := NewSteerRuntimeStore(dir).Snapshot(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestSteerRuntimeRejectsInvalidReservationQueue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	reg, _, _, err := NewRegistrationStore(dir).Join(ctx, testSteerRegistration(t))
	if err != nil {
		t.Fatal(err)
	}
	store := NewSteerRuntimeStore(dir)
	if err := store.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{"", "review", "open", "Signoff", "hold "} {
		if reserved, err := store.ReserveSignoffTarget(ctx, reg, "20261025-12345", queue); err == nil || reserved {
			t.Fatalf("invalid queue=%q reserved=%t err=%v", queue, reserved, err)
		}
	}
	after, err := store.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("invalid reservation changed state: %#v err=%v", after, err)
	}
}

func TestSteerRuntimeMigratesPublishedV4ExactTargetsAsSignoff(t *testing.T) {
	for _, phase := range []string{"sending", "queued"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			reg, _, _, err := NewRegistrationStore(dir).Join(ctx, testSteerRegistration(t))
			if err != nil {
				t.Fatal(err)
			}
			store := NewSteerRuntimeStore(dir)
			if err := store.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			old, err := store.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			old.Version = 4
			old.Deliveries[0].TargetTicket = "20261025-12345"
			old.Deliveries[0].State = phase
			old.Deliveries[0].Code = "queue_uncertain"
			old.Deliveries[0].RecoveryPending = true
			data, err := json.Marshal(old)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, steerRuntimeFileName)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			migrated, err := store.Snapshot(ctx)
			expected := old.Deliveries[0]
			expected.TargetQueue = "signoff"
			if err != nil || migrated.Version != 5 || !migrated.migrated || len(migrated.Deliveries) != 1 || migrated.Deliveries[0] != expected {
				t.Fatalf("migration lost fields: %#v err=%v", migrated, err)
			}
			if unchanged, err := os.ReadFile(path); err != nil || string(unchanged) != string(data) {
				t.Fatalf("read-only migration rewrote v4: %s err=%v", unchanged, err)
			}
			if err := store.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			persisted, err := NewSteerRuntimeStore(dir).Snapshot(ctx)
			if err != nil || persisted.Version != 5 || persisted.migrated || len(persisted.Deliveries) != 1 || persisted.Deliveries[0] != expected {
				t.Fatalf("migration not persisted: %#v err=%v", persisted, err)
			}
		})
	}
}

func TestSteerRuntimeValidatesTargetQueueWireShape(t *testing.T) {
	tests := []struct {
		name      string
		change    func(map[string]any, map[string]any)
		wantError string
	}{
		{"missing queue", func(_ map[string]any, d map[string]any) { delete(d, "target_queue") }, "exact target_queue must be signoff or hold"},
		{"unknown queue", func(_ map[string]any, d map[string]any) { d["target_queue"] = "review" }, "exact target_queue must be signoff or hold"},
		{"empty queue", func(_ map[string]any, d map[string]any) { d["target_queue"] = "" }, "exact target_queue must be signoff or hold"},
		{"null queue", func(_ map[string]any, d map[string]any) { d["target_queue"] = nil }, "exact target_queue must be signoff or hold"},
		{"wrong queue type", func(_ map[string]any, d map[string]any) { d["target_queue"] = true }, "cannot unmarshal"},
		{"queue without target", func(_ map[string]any, d map[string]any) { delete(d, "target_ticket") }, "target_queue must be absent without target_ticket"},
		{"empty queue without target", func(_ map[string]any, d map[string]any) { delete(d, "target_ticket"); d["target_queue"] = "" }, "target_queue must be absent without target_ticket"},
		{"null queue without target", func(_ map[string]any, d map[string]any) { delete(d, "target_ticket"); d["target_queue"] = nil }, "target_queue must be absent without target_ticket"},
		{"unknown field", func(_ map[string]any, d map[string]any) { d["target_kind"] = "hold" }, "unknown field"},
		{"v4 with queue", func(s map[string]any, _ map[string]any) { s["version"] = 4 }, "v4 delivery 0 must not contain target_queue"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			reg, _, _, err := NewRegistrationStore(dir).Join(ctx, testSteerRegistration(t))
			if err != nil {
				t.Fatal(err)
			}
			store := NewSteerRuntimeStore(dir)
			if err := store.Reconcile(ctx, []SteerRegistration{reg}); err != nil {
				t.Fatal(err)
			}
			if reserved, err := store.ReserveSignoffTarget(ctx, reg, "20261025-12345", "hold"); err != nil || !reserved {
				t.Fatalf("reserve=%t err=%v", reserved, err)
			}
			path := filepath.Join(dir, steerRuntimeFileName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			delivery := wire["deliveries"].([]any)[0].(map[string]any)
			test.change(wire, delivery)
			data, err = json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Snapshot(ctx); !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("invalid record error=%v want %q", err, test.wantError)
			}
			if unchanged, err := os.ReadFile(path); err != nil || string(unchanged) != string(data) {
				t.Fatalf("malformed pending record was changed: %s err=%v", unchanged, err)
			}
		})
	}
}
