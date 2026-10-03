package cli

import (
	"context"
	"errors"
	"maps"

	"github.com/toolsupply/ticket-orc/internal/state"
)

type registrationObservation struct {
	Registrations []state.SteerRegistration
	Code          string
	Present       bool
	Err           error
}

// registrationObserver distinguishes the normal pre-join absence from loss
// after a valid file has been observed, and retains only the last valid set.
type registrationObserver struct {
	store     *state.RegistrationStore
	observed  bool
	malformed bool
	last      []state.SteerRegistration
}

func newRegistrationObserver(store *state.RegistrationStore) *registrationObserver {
	return &registrationObserver{store: store}
}

func (o *registrationObserver) Observe(ctx context.Context) registrationObservation {
	snapshot, present, err := o.store.SnapshotWithPresence(ctx)
	if err != nil {
		if errors.Is(err, state.ErrMalformed) {
			o.malformed = true
		}
		code := "registration_state_unavailable"
		if o.malformed {
			code = "registration_state_malformed"
		}
		return registrationObservation{Registrations: o.lastRegistrations(), Code: code, Err: err}
	}
	if !present {
		if o.malformed {
			return registrationObservation{Registrations: o.lastRegistrations(), Code: "registration_state_malformed"}
		}
		if o.observed {
			return registrationObservation{Registrations: o.lastRegistrations(), Code: "registration_state_lost"}
		}
		return registrationObservation{}
	}
	o.observed = true
	o.malformed = false
	o.last = append(o.last[:0], snapshot.Registrations...)
	return registrationObservation{Registrations: append([]state.SteerRegistration(nil), o.last...), Present: true}
}

func (o *registrationObserver) Seed(observation registrationObservation) {
	if observation.Present {
		o.observed = true
		o.last = append(o.last[:0], observation.Registrations...)
	}
	if observation.Code == "registration_state_malformed" {
		o.malformed = true
	}
}

func (o *registrationObserver) lastRegistrations() []state.SteerRegistration {
	return append([]state.SteerRegistration(nil), o.last...)
}

// currentSteerRegistration verifies that a queued operation still names the
// registration incarnation stored for this repository and actor. This is a
// session-validity check, not a Codex liveness probe: the supported Codex queue
// command can accept work for unloaded threads, so success cannot establish
// that a session is online. Abort therefore also requires active-claim or
// outstanding-delivery evidence before it queues an emergency message.
func currentSteerRegistration(ctx context.Context, store *state.RegistrationStore, expected state.SteerRegistration) (bool, error) {
	current, found, err := store.Find(ctx, expected.RepositoryID, expected.Actor)
	if err != nil || !found {
		return false, err
	}
	return sameSteerRegistrationIncarnation(current, expected), nil
}

func sameSteerRegistrationIncarnation(left, right state.SteerRegistration) bool {
	return left.RepositoryID == right.RepositoryID && left.Actor == right.Actor &&
		left.RegistrationID == right.RegistrationID && left.IncarnationID == right.IncarnationID &&
		left.RepositoryPath == right.RepositoryPath && left.Role == right.Role &&
		left.Harness == right.Harness && left.SessionID == right.SessionID &&
		left.Transport.Kind == right.Transport.Kind && maps.Equal(left.Transport.Params, right.Transport.Params)
}

func sameSteerRegistration(left, right state.SteerRegistration) bool {
	return left.RepositoryName == right.RepositoryName && sameSteerRegistrationIncarnation(left, right)
}
