package terminaltitle

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type terminalInfo struct{}

func (terminalInfo) Name() string       { return "terminal" }
func (terminalInfo) Size() int64        { return 0 }
func (terminalInfo) Mode() os.FileMode  { return os.ModeCharDevice }
func (terminalInfo) ModTime() time.Time { return time.Time{} }
func (terminalInfo) IsDir() bool        { return false }
func (terminalInfo) Sys() any           { return nil }

func TestSanitize(t *testing.T) {
	got := sanitize("ticket\norc\r\t\x1b\x07\x00\x1f\x7f\u0085✓")
	if got != "ticket orc  ✓" {
		t.Fatalf("sanitize=%q", got)
	}
	if got := sanitize(strings.Repeat("界", maxTitleRunes+1)); len([]rune(got)) != maxTitleRunes || !strings.HasSuffix(got, "界") {
		t.Fatalf("unicode truncation len=%d", len([]rune(got)))
	}
	if got := sanitize("\x00\x1b\x07\x7f"); got != "" {
		t.Fatalf("control-only title=%q", got)
	}
}

func TestSetSupportAndOutput(t *testing.T) {
	stat := func() (os.FileInfo, error) { return terminalInfo{}, nil }
	var output string
	setForOS("linux", "ticket — repo", "xterm", stat, func(value string) error { output = value; return nil })
	if output != "\x1b]2;ticket — repo\x1b\\" {
		t.Fatalf("title output=%q", output)
	}
	for _, test := range []struct {
		name string
		goos string
		term string
		stat func() (os.FileInfo, error)
	}{
		{name: "unset term", goos: "linux", term: "", stat: stat},
		{name: "dumb term", goos: "linux", term: "dumb", stat: stat},
		{name: "stat failure", goos: "linux", term: "xterm", stat: func() (os.FileInfo, error) { return nil, errors.New("stat failed") }},
	} {
		output = ""
		setForOS(test.goos, "ticket", test.term, test.stat, func(value string) error { output = value; return nil })
		if output != "" {
			t.Fatalf("%s emitted %q", test.name, output)
		}
	}
	if !supportedForOS("windows", "", stat) || !supportedForOS("windows", "dumb", stat) {
		t.Fatal("Windows support incorrectly depends on TERM")
	}
}

func TestSetIgnoresWriteFailure(t *testing.T) {
	setForOS("linux", "ticket", "xterm", func() (os.FileInfo, error) { return terminalInfo{}, nil }, func(string) error { return errors.New("write failed") })
}
