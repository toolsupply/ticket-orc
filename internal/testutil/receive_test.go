package testutil

import (
	"strings"
	"testing"
	"time"
)

func TestReceiveValueReturnsChannelValue(t *testing.T) {
	ch := make(chan string, 1)
	ch <- "ready"
	if got := Receive(t, time.Second, "child readiness", ch); got != "ready" {
		t.Fatalf("Receive = %q, want ready", got)
	}
}

func TestReceiveValueReportsClosedChannel(t *testing.T) {
	ch := make(chan int)
	close(ch)
	got, err := receiveValue(time.Second, "process exit", ch)
	if err == nil || !strings.Contains(err.Error(), "process exit") || !strings.Contains(err.Error(), "closed before a value") {
		t.Fatalf("receiveValue = %d, %v; want closed-channel diagnostic", got, err)
	}
}

func TestReceiveValueTimeoutNamesAwaitedOperation(t *testing.T) {
	ch := make(chan struct{})
	_, err := receiveValue(10*time.Millisecond, "observer startup", ch)
	if err == nil || !strings.Contains(err.Error(), "observer startup") || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("receiveValue error = %v, want timeout diagnostic", err)
	}
}
