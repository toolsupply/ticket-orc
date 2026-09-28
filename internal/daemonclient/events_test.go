package daemonclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEventStreamRejectsUnsupportedContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"worker.state"}`))
	}))
	defer server.Close()
	stateDir := t.TempDir()
	writeClientEndpoint(t, stateDir, server.URL, 1, 1, "token")
	client, err := New(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Events(context.Background()); !hasClientErrorKind(err, ErrorProtocol) {
		t.Fatalf("error=%v, want protocol", err)
	}
}
