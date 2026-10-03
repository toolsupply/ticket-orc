// Package steertransport delivers logical messages to externally-owned
// interactive sessions. It is separate from managed harness lifecycle.
package steertransport

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/state"
)

type MessageKind string

const (
	MessageSteer MessageKind = "steer"
	MessageStop  MessageKind = "stop"
)

// Message contains the policy-produced content and its purpose. Session and
// transport routing always come from the registration supplied alongside it.
type Message struct {
	Kind           MessageKind
	RegistrationID string
	IncarnationID  string
	Harness        string
	SessionID      string
	Text           string
}

// Endpoint is a transport preparation descriptor. Fields not used by a
// transport remain empty.
type Endpoint struct {
	Kind     string
	Protocol int
	Root     string
	Pending  string
	Control  string
	Rejected string
}

type Transport interface {
	Kind() string
	Prepare(context.Context, string, state.SteerRegistration) (Endpoint, error)
	Verify(context.Context, string, state.SteerRegistration) (Endpoint, error)
	Deliver(context.Context, string, state.SteerRegistration, Message) error
	Retire(context.Context, string, state.SteerRegistration) error
}

type Router struct {
	transports map[string]Transport
}

func NewRouter(transports ...Transport) (*Router, error) {
	router := &Router{transports: make(map[string]Transport, len(transports))}
	for _, transport := range transports {
		if transport == nil || strings.TrimSpace(transport.Kind()) == "" {
			return nil, errors.New("steer transport and kind must not be empty")
		}
		if _, exists := router.transports[transport.Kind()]; exists {
			return nil, fmt.Errorf("duplicate steer transport kind %q", transport.Kind())
		}
		router.transports[transport.Kind()] = transport
	}
	return router, nil
}

func (r *Router) Prepare(ctx context.Context, localRoot string, registration state.SteerRegistration) (Endpoint, error) {
	transport, err := r.resolve(registration)
	if err != nil {
		return Endpoint{}, err
	}
	return transport.Prepare(ctx, localRoot, registration)
}

// Verify resolves an existing endpoint without creating or repairing it.
func (r *Router) Verify(ctx context.Context, localRoot string, registration state.SteerRegistration) (Endpoint, error) {
	transport, err := r.resolve(registration)
	if err != nil {
		return Endpoint{}, err
	}
	return transport.Verify(ctx, localRoot, registration)
}

func (r *Router) Deliver(ctx context.Context, localRoot string, registration state.SteerRegistration, message Message) error {
	if message.Kind != MessageSteer && message.Kind != MessageStop {
		return fmt.Errorf("unsupported steer message kind %q", message.Kind)
	}
	if message.Text == "" || strings.ContainsRune(message.Text, '\x00') {
		return errors.New("steer message must not be empty or contain NUL")
	}
	transport, err := r.resolve(registration)
	if err != nil {
		return err
	}
	message.RegistrationID = registration.RegistrationID
	message.IncarnationID = registration.IncarnationID
	message.Harness = registration.Harness
	message.SessionID = registration.SessionID
	return transport.Deliver(ctx, localRoot, registration, message)
}

func (r *Router) Retire(ctx context.Context, localRoot string, registration state.SteerRegistration) error {
	transport, err := r.resolve(registration)
	if err != nil {
		return err
	}
	return transport.Retire(ctx, localRoot, registration)
}

func (r *Router) resolve(registration state.SteerRegistration) (Transport, error) {
	if r == nil {
		return nil, errors.New("steer transport router is unavailable")
	}
	transport, ok := r.transports[registration.Transport.Kind]
	if !ok {
		return nil, fmt.Errorf("unsupported steer transport kind %q", registration.Transport.Kind)
	}
	return transport, nil
}

type deliveryOutcome uint8

const (
	uncertain deliveryOutcome = iota
	rejected
)

type deliveryError struct {
	outcome deliveryOutcome
	err     error
}

func (e *deliveryError) Error() string { return e.err.Error() }
func (e *deliveryError) Unwrap() error { return e.err }

// Rejected marks a delivery failure that proves the transport did not accept
// the message. Unclassified errors remain uncertain.
func Rejected(err error) error {
	if err == nil {
		return nil
	}
	return &deliveryError{outcome: rejected, err: err}
}

func IsRejected(err error) bool {
	var classified *deliveryError
	return errors.As(err, &classified) && classified.outcome == rejected
}

func IsUncertain(err error) bool { return err != nil && !IsRejected(err) }
