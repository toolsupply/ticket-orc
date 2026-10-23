package harness_test

import (
	"context"
	"errors"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
)

func TestUnadvertisedAdaptersRejectLiveOutput(t *testing.T) {
	for _, adapter := range []harness.Harness{claude.New(), pi.New()} {
		request := harness.RunRequest{Events: make(chan harness.OutputEvent, 1)}
		if _, err := adapter.Run(context.Background(), request); !errors.Is(err, harness.ErrOutputUnsupported) {
			t.Fatalf("%T: error = %v", adapter, err)
		}
		caps := adapter.(harness.PreflightAdapter).Capabilities()
		if caps.OutputGranularity != "" {
			t.Fatalf("%T advertises unsupported output", adapter)
		}
	}
}
