package steertransport

import (
	"context"
	"errors"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/state"
)

type recordingTransport struct {
	prepared     int
	delivered    []Message
	retired      int
	registration state.SteerRegistration
}

func (*recordingTransport) Kind() string { return "recording" }
func (transport *recordingTransport) Prepare(_ context.Context, _ string, registration state.SteerRegistration) (Endpoint, error) {
	transport.prepared++
	transport.registration = registration
	return Endpoint{Kind: transport.Kind()}, nil
}
func (transport *recordingTransport) Verify(_ context.Context, _ string, registration state.SteerRegistration) (Endpoint, error) {
	transport.registration = registration
	return Endpoint{Kind: transport.Kind()}, nil
}
func (transport *recordingTransport) Deliver(_ context.Context, _ string, registration state.SteerRegistration, message Message) error {
	transport.registration = registration
	transport.delivered = append(transport.delivered, message)
	return nil
}
func (transport *recordingTransport) Retire(_ context.Context, _ string, registration state.SteerRegistration) error {
	transport.retired++
	transport.registration = registration
	return nil
}

func TestRouterRoutesPrepareDeliverAndRetireByTransportKind(t *testing.T) {
	transport := &recordingTransport{}
	router, err := NewRouter(transport)
	if err != nil {
		t.Fatal(err)
	}
	registration := state.SteerRegistration{RepositoryID: "repo", Actor: "actor", SessionID: "session", Transport: state.SteerTransportRoute{Kind: transport.Kind()}}
	endpoint, err := router.Prepare(context.Background(), "/orc", registration)
	if err != nil || endpoint.Kind != "recording" || transport.prepared != 1 {
		t.Fatalf("Prepare endpoint=%#v count=%d err=%v", endpoint, transport.prepared, err)
	}
	for _, kind := range []MessageKind{MessageSteer, MessageStop} {
		message := Message{Kind: kind, Text: "message"}
		if err := router.Deliver(context.Background(), "/orc", registration, message); err != nil {
			t.Fatal(err)
		}
	}
	if len(transport.delivered) != 2 || transport.delivered[0].Kind != MessageSteer || transport.delivered[1].Kind != MessageStop ||
		transport.delivered[0].SessionID != registration.SessionID || transport.delivered[0].RegistrationID != registration.RegistrationID ||
		transport.delivered[0].IncarnationID != registration.IncarnationID || transport.delivered[0].Harness != registration.Harness ||
		transport.registration.SessionID != registration.SessionID {
		t.Fatalf("delivered=%#v registration=%#v", transport.delivered, transport.registration)
	}
	if err := router.Retire(context.Background(), "/orc", registration); err != nil || transport.retired != 1 {
		t.Fatalf("Retire count=%d err=%v", transport.retired, err)
	}
}

func TestRouterRejectsUnsupportedTransportAndMalformedMessages(t *testing.T) {
	router, err := NewRouter(&recordingTransport{})
	if err != nil {
		t.Fatal(err)
	}
	registration := state.SteerRegistration{Transport: state.SteerTransportRoute{Kind: "missing"}}
	if err := router.Deliver(context.Background(), "", registration, Message{Kind: MessageSteer, Text: "x"}); err == nil {
		t.Fatal("unsupported transport was accepted")
	}
	registration.Transport.Kind = "recording"
	for _, message := range []Message{{Kind: "other", Text: "x"}, {Kind: MessageSteer}, {Kind: MessageStop, Text: "bad\x00text"}} {
		if err := router.Deliver(context.Background(), "", registration, message); err == nil {
			t.Errorf("malformed message %#v was accepted", message)
		}
	}
}

func TestDeliveryErrorsDefaultToUncertainAndCanBeRejected(t *testing.T) {
	if IsRejected(nil) || IsUncertain(nil) {
		t.Fatal("nil delivery error had an outcome")
	}
	unknown := errors.New("delivery may have been accepted")
	if IsRejected(unknown) || !IsUncertain(unknown) {
		t.Fatal("unclassified delivery error was not uncertain")
	}
	definite := Rejected(unknown)
	if !IsRejected(definite) || IsUncertain(definite) || !errors.Is(definite, unknown) {
		t.Fatalf("rejected outcome was not preserved: %v", definite)
	}
}
