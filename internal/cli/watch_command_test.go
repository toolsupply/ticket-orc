package cli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
)

func TestStandaloneWorkerWatchUsesSharedFollower(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("verbose=%t", verbose), func(t *testing.T) {
			root := t.TempDir()
			output := &standaloneWatchOutput{started: make(chan struct{}), matched: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- runStandaloneWorkerWatch(ctx, root, daemon.Status{Workers: []daemon.WorkerStatus{{Name: "coder", Role: "coder", Harness: "codex", RepositoryID: joinTestRepositoryID}}}, watchOptions{selector: "coder", verbose: verbose}, output)
			}()
			finished := false
			defer func() {
				cancel()
				if !finished {
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("standalone worker watcher did not stop during cleanup")
					}
				}
			}()
			waitWorkerWatchSignal(t, output.started, "standalone worker watch startup")
			log := openWatchTestLog(t, root, "coder", time.Now())
			if _, err := log.Write([]byte(watchDetailEvents + "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"standalone output\"}}\n")); err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			waitWorkerWatchSignal(t, output.matched, "standalone worker output")
			cancel()
			select {
			case err := <-done:
				finished = true
				if err != nil {
					t.Fatalf("standalone worker watcher returned error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("standalone worker watcher did not stop after cancellation")
			}
			if got := output.String(); !strings.Contains(got, "Watching worker coder output") || !strings.Contains(got, "[11111111…:coder] standalone output") {
				t.Fatalf("standalone watch output=%q", got)
			}
			if strings.Contains(output.String(), "noisy-command") != verbose || strings.Contains(output.String(), "changed.go") != verbose {
				t.Fatalf("standalone verbosity=%t output=%q", verbose, output.String())
			}
		})
	}
}

func TestStandaloneWorkerWatchRejectsRemoteOutput(t *testing.T) {
	for _, selector := range [][]string{nil, {"--verbose"}, {"all"}, {"all", "--verbose"}, {"coder"}} {
		var stdout, stderr bytes.Buffer
		code := executeWatchCommand(append(selector, "--endpoint", "http://127.0.0.1:1/capability"), &stdout, &stderr, emptyEnv)
		if code == 0 || !strings.Contains(stderr.String(), remoteWorkerWatchError) {
			t.Fatalf("remote worker watch %q code=%d stdout=%q stderr=%q", selector, code, stdout.String(), stderr.String())
		}
	}
}

func waitWorkerWatchSignal(t *testing.T, signal <-chan struct{}, milestone string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", milestone)
	}
}

type standaloneWatchOutput struct {
	mu      sync.Mutex
	data    bytes.Buffer
	started chan struct{}
	matched chan struct{}
}

func (out *standaloneWatchOutput) Write(data []byte) (int, error) {
	out.mu.Lock()
	defer out.mu.Unlock()
	n, err := out.data.Write(data)
	if err != nil {
		return n, err
	}
	text := out.data.String()
	if strings.Contains(text, "Watching worker coder output") {
		select {
		case <-out.started:
		default:
			close(out.started)
		}
	}
	if strings.Contains(text, "[11111111…:coder] standalone output") {
		select {
		case <-out.matched:
		default:
			close(out.matched)
		}
	}
	return n, nil
}

func (out *standaloneWatchOutput) String() string {
	out.mu.Lock()
	defer out.mu.Unlock()
	return out.data.String()
}

func TestWatchOptionsConsoleAndCLIParity(t *testing.T) {
	for _, args := range [][]string{nil, {"orc"}, {"all"}, {"coder"}, {"--verbose"}, {"all", "--verbose"}, {"--verbose", "coder"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			console, err := parseConsoleCommand("watch " + strings.Join(args, " "))
			if err != nil {
				t.Fatal(err)
			}
			_, positional, help, err := parseDaemonOptions(append(append([]string{}, args...), "--endpoint", "http://127.0.0.1:1/cap"), emptyEnv, false, true)
			if err != nil || help {
				t.Fatalf("CLI parse=%v help=%t", err, help)
			}
			standalone, err := parseWatchOptions(positional)
			if err != nil || standalone != console.watch {
				t.Fatalf("CLI=%#v console=%#v err=%v", standalone, console.watch, err)
			}
		})
	}
	for _, args := range [][]string{{"--verbose", "all", "--verbose"}, {"--unknown"}, {"--raw"}, {"--verbose=true"}, {"coder", "all"}} {
		if _, err := parseConsoleCommand("watch " + strings.Join(args, " ")); err == nil {
			t.Errorf("console accepted %q", args)
		}
		var out, errors bytes.Buffer
		if code := executeWatchCommand(append(append([]string{}, args...), "--endpoint", "http://127.0.0.1:1/cap"), &out, &errors, emptyEnv); code != 2 {
			t.Errorf("CLI invalid args %q code=%d stderr=%q", args, code, errors.String())
		}
	}
	for _, args := range [][]string{{"--help"}, {"-h"}, {"all", "--verbose", "--help"}, {"--help", "--verbose", "coder"}} {
		console, err := parseConsoleCommand("watch " + strings.Join(args, " "))
		if err != nil || console.name != "help" {
			t.Fatalf("console help %q = %#v, %v", args, console, err)
		}
		var out, errors bytes.Buffer
		if code := executeWatchCommand(args, &out, &errors, emptyEnv); code != 0 {
			t.Fatalf("CLI help %q code=%d stderr=%q", args, code, errors.String())
		}
		var consoleHelp bytes.Buffer
		writeConsoleTopicHelp(&consoleHelp, "watch")
		for _, text := range []string{watchHelp, consoleHelp.String()} {
			for _, want := range []string{"watch [orc|all|WORKER] [--verbose]", "observation time", "commands and file changes", "[repo:orc]", "[repo:actor]", "legacy Orc-only table", "watch orc"} {
				if !strings.Contains(text, want) {
					t.Errorf("help lacks %q: %q", want, text)
				}
			}
		}
	}
}

func TestWatchVerbosePreservesEndpointAndConfigOptions(t *testing.T) {
	path := t.TempDir() + "/orc.json"
	writeConfigFixture(t, path, `{"version":1,"local_dir":"runtime"}`)
	for _, configFlag := range []string{"-c", "--config"} {
		options, positional, help, err := parseDaemonOptions([]string{"--verbose", configFlag, path, "all", "--endpoint=http://127.0.0.1:1/cap"}, emptyEnv, false, true)
		if err != nil || help {
			t.Fatalf("parse=%v help=%t", err, help)
		}
		watch, err := parseWatchOptions(positional)
		if err != nil || !watch.verbose || watch.selector != "all" || options.configPath != path || options.localDir == "" || options.endpoint != "http://127.0.0.1:1/cap" {
			t.Fatalf("options=%#v watch=%#v err=%v", options, watch, err)
		}
	}
	if _, _, _, err := parseDaemonCommandOptions([]string{"--verbose"}, emptyEnv, false); err == nil {
		t.Fatal("watch verbosity leaked into other daemon commands")
	}
}
