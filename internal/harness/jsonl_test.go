package harness

import (
	"errors"
	"strings"
	"testing"
)

func TestLineFramerHandlesChunksAndFinalLine(t *testing.T) {
	var framer LineFramer
	var got []string
	consume := func(line []byte) error {
		got = append(got, string(line))
		return nil
	}
	if err := framer.Write([]byte("first\nsec"), consume); err != nil {
		t.Fatal(err)
	}
	if err := framer.Write([]byte("ond\nlast"), consume); err != nil {
		t.Fatal(err)
	}
	if err := framer.Finish(consume); err != nil {
		t.Fatal(err)
	}
	if want := []string{"first", "second", "last"}; !equalLines(got, want) {
		t.Fatalf("lines = %#v, want %#v", got, want)
	}
}

func TestLineFramerBoundsTerminatedAndPartialEvents(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "terminated", data: []byte(strings.Repeat("x", MaxEventBytes+1) + "\n")},
		{name: "partial", data: []byte(strings.Repeat("x", MaxEventBytes+1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			var framer LineFramer
			if err := framer.Write(test.data, func([]byte) error { return nil }); !errors.Is(err, ErrEventTooLarge) {
				t.Fatalf("Write error = %v, want ErrEventTooLarge", err)
			}
		})
	}
}

func equalLines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
