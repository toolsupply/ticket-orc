package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/testutil"
)

func writeDaemonReleaseConfig(t *testing.T, configPath, localDir string, includeWorker, includeGroups bool) {
	t.Helper()
	config := map[string]any{
		"version":   1,
		"id":        "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa",
		"local_dir": localDir,
		"supervisor": map[string]any{
			"startup_groups": []string{},
			"port":           0,
		},
	}
	if includeWorker {
		worker := map[string]any{
			"role":            "coder",
			"actor":           "integration-coder",
			"required_skills": []string{"ticket"},
		}
		if includeGroups {
			worker["groups"] = []string{"default"}
		}
		config["default_role"] = "coder"
		config["roles"] = map[string]any{
			"coder":    map[string]string{"ticket_queue": "open", "nudge_prompt": "Coding."},
			"reviewer": map[string]string{"ticket_queue": "review", "nudge_prompt": "Review."},
		}
		config["defaults"] = map[string]string{"harness": "codex", "output": "quiet"}
		config["workers"] = map[string]any{"worker": worker}
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

type processWaiter struct {
	done chan struct{}
	err  error
}

func waitForProcess(cmd *exec.Cmd) *processWaiter {
	waiter := &processWaiter{done: make(chan struct{})}
	go func() {
		waiter.err = cmd.Wait()
		close(waiter.done)
	}()
	return waiter
}

func (waiter *processWaiter) wait(timeout time.Duration) (error, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-waiter.done:
		return waiter.err, true
	case <-timer.C:
		return nil, false
	}
}

func (waiter *processWaiter) exited() bool {
	select {
	case <-waiter.done:
		return true
	default:
		return false
	}
}

// Managed workers remain controllable through the authenticated daemon API.
func TestRunAuthenticatedDaemonControlAndEvents(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signal-backed foreground worker control is covered on Unix")
	}
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	repository := filepath.Join(root, "tickets")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	stateDir := filepath.Join(root, "state")
	writeDaemonReleaseConfig(t, configPath, stateDir, true, true)
	codexLog := filepath.Join(root, "codex.jsonl")
	codexBlocked := filepath.Join(root, "codex.blocked")
	ticketLog := filepath.Join(root, "ticket.log")
	cmd := exec.Command(binary, "run", "--config", configPath, "--worker", "worker")
	var runStdout, runStderr synchronizedBuffer
	cmd.Stdout, cmd.Stderr = &runStdout, &runStderr
	cmd.Env = testEnv(helperDir, map[string]string{
		"TICKET_ORC_FAKE_TICKET_ID":          integrationTicketID,
		"TICKET_ORC_FAKE_TICKET_STATE":       "open",
		"TICKET_ORC_FAKE_CLAIM_STATE":        "open",
		"TICKET_ORC_FAKE_QUEUE":              "open",
		"TICKET_ORC_FAKE_READY":              "0",
		"TICKET_ORC_FAKE_ACTIVE":             "0",
		"TICKET_ORC_FAKE_CODEX_BLOCK":        "1",
		"TICKET_ORC_FAKE_CODEX_BLOCKED_FILE": codexBlocked,
		"TICKET_ORC_FAKE_CODEX_LOG":          codexLog,
		"TICKET_ORC_FAKE_TICKET_LOG":         ticketLog,
	}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waiter := waitForProcess(cmd)
	t.Cleanup(func() {
		if waiter.exited() {
			return
		}
		_ = cmd.Process.Signal(os.Interrupt)
		if _, done := waiter.wait(6 * time.Second); done {
			return
		}
		_ = cmd.Process.Kill()
		if _, done := waiter.wait(time.Second); !done {
			t.Error("daemon process did not exit after forced cleanup")
		}
	})
	endpoint := waitForDaemonEndpoint(t, stateDir, runStderr.String)
	client := &http.Client{Timeout: 5 * time.Second}

	eventsContext, cancelEvents := context.WithCancel(context.Background())
	defer cancelEvents()
	eventsRequest, err := http.NewRequestWithContext(eventsContext, http.MethodGet, endpoint.CapabilityURL()+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	eventsResponse, err := client.Do(eventsRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer eventsResponse.Body.Close()
	if eventsResponse.StatusCode != http.StatusOK || !strings.HasPrefix(eventsResponse.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("events response=%d content-type=%q", eventsResponse.StatusCode, eventsResponse.Header.Get("Content-Type"))
	}
	reader := bufio.NewReader(eventsResponse.Body)
	idLine, readErr := reader.ReadString('\n')
	if readErr != nil {
		t.Fatalf("event sync id line=%q err=%v", idLine, readErr)
	}
	eventLine, readErr := reader.ReadString('\n')
	if readErr != nil || eventLine != "event: stream.sync\n" {
		t.Fatalf("event sync type line=%q err=%v", eventLine, readErr)
	}
	dataLine, readErr := reader.ReadString('\n')
	if readErr != nil || !strings.HasPrefix(dataLine, "data: ") {
		t.Fatalf("event sync data line=%q err=%v", dataLine, readErr)
	}
	if line, readErr := reader.ReadString('\n'); readErr != nil || line != "\n" {
		t.Fatalf("event sync terminator=%q err=%v", line, readErr)
	}
	var syncEvent daemon.Event
	if err := json.Unmarshal([]byte(strings.TrimPrefix(dataLine, "data: ")), &syncEvent); err != nil {
		t.Fatalf("decode sync payload %q: %v", dataLine, err)
	}
	var syncID uint64
	if _, err := fmt.Sscanf(idLine, "id: %d\n", &syncID); err != nil || syncEvent.Type != "stream.sync" || syncEvent.Seq != syncID {
		t.Fatalf("sync frame id=%q event=%#v err=%v", idLine, syncEvent, err)
	}
	// Keep the fake Codex turn active and wait for its explicit blocked marker.
	// This test exercises pausing a live worker; endpoint publication and
	// stream.sync can both happen before the worker reaches its turn.
	testutil.Await(t, 5*time.Second, "fake Codex turn to block", func() bool {
		_, err := os.Stat(codexBlocked)
		return err == nil
	}, func() string {
		return "codex log tail=" + readDiagnosticFileTail(codexLog, 2048) + " daemon stderr tail=" + boundedDiagnosticText(runStderr.String(), 2048)
	})
	status := daemon.Status{}
	statusResponse := authenticatedRequest(t, client, endpoint, http.MethodGet, "/v1/status", "")
	statusErr := json.NewDecoder(statusResponse.Body).Decode(&status)
	_ = statusResponse.Body.Close()
	if statusResponse.StatusCode != http.StatusOK || statusErr != nil || len(status.Workers) != 1 || status.Workers[0].Name != "worker" || status.Workers[0].State != "running" {
		t.Fatalf("status=%#v response=%d decode_err=%v daemon stdout (tail): %s daemon stderr (tail): %s",
			status, statusResponse.StatusCode, statusErr,
			boundedDiagnosticText(runStdout.String(), 2048), boundedDiagnosticText(runStderr.String(), 2048),
		)
	}
	if _, err := postDaemon(t, client, endpoint, "/v1/workers/worker/pause", ""); err != nil {
		t.Fatalf("pause request failed: %v\ndaemon stdout (tail): %s\ndaemon stderr (tail): %s\nfake Ticket log (tail): %s\nfake Codex log (tail): %s",
			err,
			boundedDiagnosticText(runStdout.String(), 2048),
			boundedDiagnosticText(runStderr.String(), 2048),
			readDiagnosticFileTail(ticketLog, 2048),
			readDiagnosticFileTail(codexLog, 2048),
		)
	}
	if !readEventContaining(t, reader, "worker.control", cancelEvents) {
		t.Fatal("pause control event missing")
	}
	paused := daemon.Status{}
	pausedResponse := authenticatedRequest(t, client, endpoint, http.MethodGet, "/v1/status", "")
	pausedErr := json.NewDecoder(pausedResponse.Body).Decode(&paused)
	_ = pausedResponse.Body.Close()
	if pausedResponse.StatusCode != http.StatusOK || pausedErr != nil || len(paused.Workers) != 1 || paused.Workers[0].State != "paused" {
		t.Fatalf("paused status=%#v response=%d decode_err=%v", paused, pausedResponse.StatusCode, pausedErr)
	}
	if _, err := postDaemon(t, client, endpoint, "/v1/shutdown", ""); err != nil {
		t.Fatal(err)
	}
	_ = eventsResponse.Body.Close()
	waitErr, exited := waiter.wait(6 * time.Second)
	if !exited {
		t.Fatal("daemon process did not exit after shutdown; cleanup will signal and reap it")
	}
	if waitErr != nil {
		t.Fatalf("run shutdown: %v", waitErr)
	}
}

func TestRunStartsAuthenticatedDaemonWithoutWorkers(t *testing.T) {
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	configPath := filepath.Join(root, "config.json")
	writeDaemonReleaseConfig(t, configPath, stateDir, false, false)
	cmd := exec.Command(binary, "run", "--config", configPath)
	cmd.Env = testEnv(helperDir, nil, map[string]string{})
	var stderr synchronizedBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	endpoint := waitForDaemonEndpoint(t, stateDir, stderr.String)
	response := authenticatedRequest(t, &http.Client{Timeout: 3 * time.Second}, endpoint, http.MethodGet, "/v1/status", "")
	var status daemon.Status
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&status) != nil || len(status.Workers) != 0 {
		response.Body.Close()
		t.Fatalf("zero-worker status=%#v response=%d stderr=%q", status, response.StatusCode, stderr.String())
	}
	response.Body.Close()
	repositories := authenticatedRequest(t, &http.Client{Timeout: 3 * time.Second}, endpoint, http.MethodGet, "/v1/repositories", "")
	if repositories.StatusCode != http.StatusOK {
		repositories.Body.Close()
		t.Fatalf("zero-worker repository endpoint status=%d", repositories.StatusCode)
	}
	repositories.Body.Close()
	if _, err := postDaemon(t, &http.Client{Timeout: 3 * time.Second}, endpoint, "/v1/shutdown", ""); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("zero-worker daemon shutdown: %v", err)
	}
}

func TestRunPublishesConfiguredInstanceIDAcrossRestart(t *testing.T) {
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	const instanceID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	stateDir := filepath.Join(root, "state")
	configPath := filepath.Join(root, "config.json")
	firstReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer firstReservation.Close()
	secondReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer secondReservation.Close()
	firstPort := firstReservation.Addr().(*net.TCPAddr).Port
	secondPort := secondReservation.Addr().(*net.TCPAddr).Port
	versionData, err := os.ReadFile(filepath.Join(filepath.Dir(mustCallerFile()), "../..", "VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	wantVersion := strings.TrimSpace(string(versionData))

	writeConfig := func(port int) {
		t.Helper()
		config := map[string]any{
			"version":   1,
			"id":        instanceID,
			"local_dir": stateDir,
			"supervisor": map[string]any{
				"listen":         "127.0.0.1",
				"port":           port,
				"startup_groups": []string{},
			},
		}
		data, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	type runningDaemon struct {
		cmd      *exec.Cmd
		endpoint daemon.Endpoint
	}
	start := func(port int) runningDaemon {
		t.Helper()
		writeConfig(port)
		cmd := exec.Command(binary, "run", "--config", configPath)
		var stderr synchronizedBuffer
		cmd.Stderr = &stderr
		cmd.Env = testEnv(helperDir, nil, nil)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		endpoint := waitForDaemonEndpoint(t, stateDir, stderr.String)
		endpointJSON, err := os.ReadFile(daemon.EndpointPath(stateDir))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(endpointJSON), `"instance_id":"`+instanceID+`"`) || !strings.Contains(string(endpointJSON), `"endpoint_key":"`+endpoint.EndpointKey+`"`) {
			t.Fatalf("endpoint JSON = %s, want configured instance ID and endpoint key", endpointJSON)
		}
		response := authenticatedRequest(t, &http.Client{Timeout: 3 * time.Second}, endpoint, http.MethodGet, "/v1/status", "")
		defer response.Body.Close()
		statusJSON, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		var status daemon.Status
		if response.StatusCode != http.StatusOK || json.Unmarshal(statusJSON, &status) != nil {
			t.Fatalf("status response=%d JSON=%s", response.StatusCode, statusJSON)
		}
		if status.InstanceID != instanceID || status.Version != wantVersion ||
			!strings.Contains(string(statusJSON), `"instance_id":"`+instanceID+`"`) ||
			!strings.Contains(string(statusJSON), `"version":"`+wantVersion+`"`) {
			t.Fatalf("status JSON = %s, want instance %q and runtime version %q", statusJSON, instanceID, wantVersion)
		}
		if strings.Contains(string(statusJSON), endpoint.EndpointKey) {
			t.Fatalf("status JSON leaked endpoint key: %s", statusJSON)
		}
		return runningDaemon{cmd: cmd, endpoint: endpoint}
	}
	stop := func(running runningDaemon) {
		t.Helper()
		if _, err := postDaemon(t, &http.Client{Timeout: 3 * time.Second}, running.endpoint, "/v1/shutdown", ""); err != nil {
			t.Fatal(err)
		}
		if err := running.cmd.Wait(); err != nil {
			t.Fatalf("daemon shutdown: %v", err)
		}
	}

	if err := firstReservation.Close(); err != nil {
		t.Fatal(err)
	}
	first := start(firstPort)
	stop(first)
	if err := secondReservation.Close(); err != nil {
		t.Fatal(err)
	}
	second := start(secondPort)
	defer stop(second)
	if first.endpoint.InstanceID != instanceID || second.endpoint.InstanceID != instanceID || first.endpoint.InstanceID != second.endpoint.InstanceID {
		t.Fatalf("endpoint instance IDs across restart = %q, %q; want %q", first.endpoint.InstanceID, second.endpoint.InstanceID, instanceID)
	}
	if first.endpoint.Port == second.endpoint.Port || first.endpoint.EndpointKey != second.endpoint.EndpointKey {
		t.Fatalf("restart settings unexpectedly changed: first=%#v second=%#v", first.endpoint, second.endpoint)
	}
}

func TestDefaultRuntimeRootIsSharedAndBoundToConfigID(t *testing.T) {
	binary, helperDir := buildFixture(t)
	configDir := t.TempDir()
	const firstID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	const secondID = "2e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
	writeConfig := func(path, id string) {
		t.Helper()
		config := fmt.Sprintf(`{"version":1,"id":%q,"supervisor":{"startup_groups":[],"port":0}}`, id)
		if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	start := func(configPath, localDir string) (*exec.Cmd, daemon.Endpoint) {
		t.Helper()
		cmd := exec.Command(binary, "run", "--config", configPath)
		var stderr synchronizedBuffer
		cmd.Stderr = &stderr
		cmd.Env = testEnv(helperDir, nil, nil)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		return cmd, waitForDaemonEndpoint(t, localDir, stderr.String)
	}
	stop := func(cmd *exec.Cmd, endpoint daemon.Endpoint) {
		t.Helper()
		if _, err := postDaemon(t, &http.Client{Timeout: 3 * time.Second}, endpoint, "/v1/shutdown", ""); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err != nil {
			t.Fatalf("daemon shutdown: %v", err)
		}
	}

	firstConfig := filepath.Join(configDir, "first.json")
	secondConfig := filepath.Join(configDir, "second.json")
	sameConfig := filepath.Join(configDir, "same-id.json")
	writeConfig(firstConfig, firstID)
	writeConfig(secondConfig, secondID)
	writeConfig(sameConfig, firstID)
	firstLocal := filepath.Join(configDir, ".local")
	firstCmd, firstEndpoint := start(firstConfig, firstLocal)
	secondCmd := exec.Command(binary, "run", "--config", secondConfig)
	secondCmd.Env = testEnv(helperDir, nil, nil)
	if output, err := secondCmd.CombinedOutput(); err == nil || !strings.Contains(string(output), "runtime belongs to instance "+firstID) || !strings.Contains(string(output), "config declares "+secondID) {
		t.Fatalf("different-ID daemon error=%v output=%q, want shared-root ownership rejection", err, output)
	}
	if _, err := os.Stat(filepath.Join(firstLocal, "run", "lock")); err != nil {
		t.Fatalf("daemon lock missing under %s: %v", firstLocal, err)
	}
	if _, err := os.Stat(daemon.EndpointKeyPath(firstLocal)); err != nil {
		t.Fatalf("endpoint capability key missing under %s: %v", firstLocal, err)
	}
	if _, err := os.Stat(filepath.Join(firstLocal, "instance.json")); err != nil {
		t.Fatalf("runtime ownership marker missing under %s: %v", firstLocal, err)
	}
	firstEndpointData, err := os.ReadFile(daemon.EndpointPath(firstLocal))
	if err != nil {
		t.Fatal(err)
	}
	sameID := exec.Command(binary, "run", "--config", sameConfig)
	sameID.Env = testEnv(helperDir, nil, nil)
	if output, err := sameID.CombinedOutput(); err == nil || !strings.Contains(string(output), "already running") {
		t.Fatalf("same-ID daemon error=%v output=%q, want existing daemon conflict", err, output)
	}
	if after, err := os.ReadFile(daemon.EndpointPath(firstLocal)); err != nil || string(after) != string(firstEndpointData) {
		t.Fatalf("same-ID attempt changed the existing endpoint: error=%v", err)
	}
	configEntries, err := os.ReadDir(configDir)
	if err != nil || len(configEntries) != 5 || configEntries[0].Name() != ".local" || configEntries[1].Name() != ".local.guard" || configEntries[2].Name() != "first.json" || configEntries[3].Name() != "same-id.json" || configEntries[4].Name() != "second.json" {
		t.Fatalf("unexpected entries beside portable configs: entries=%v error=%v", configEntries, err)
	}
	if _, err := os.Stat(daemon.EndpointKeyPath(configDir)); !os.IsNotExist(err) {
		t.Fatalf("endpoint capability key leaked beside portable configs: %v", err)
	}

	cloneDir := filepath.Join(t.TempDir(), ".ticket-orc")
	cloneConfig := filepath.Join(cloneDir, "config.json")
	if err := os.MkdirAll(cloneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeConfig(cloneConfig, firstID)
	cloneLocal := filepath.Join(cloneDir, ".local")
	if _, err := os.Stat(cloneLocal); !os.IsNotExist(err) {
		t.Fatalf("portable config clone already has local runtime data: %v", err)
	}
	cloneCmd, cloneEndpoint := start(cloneConfig, cloneLocal)
	if cloneEndpoint.URL == firstEndpoint.URL {
		t.Fatalf("portable config clone reused source endpoint %q", cloneEndpoint.URL)
	}
	if _, err := os.Stat(filepath.Join(cloneLocal, "daemon-control.json")); !os.IsNotExist(err) {
		t.Fatalf("portable config clone inherited daemon control state: %v", err)
	}
	if _, err := os.Stat(daemon.EndpointPath(cloneDir)); !os.IsNotExist(err) {
		t.Fatalf("clone leaked endpoint metadata beside config.json: %v", err)
	}
	cloneEntries, err := os.ReadDir(cloneDir)
	if err != nil || len(cloneEntries) != 3 || cloneEntries[0].Name() != ".local" || cloneEntries[1].Name() != ".local.guard" || cloneEntries[2].Name() != "config.json" {
		t.Fatalf("unexpected entries beside cloned config: entries=%v error=%v", cloneEntries, err)
	}
	stop(cloneCmd, cloneEndpoint)
	stop(firstCmd, firstEndpoint)
}

// The first interactive Ctrl-C returns to the console without stopping its daemon.
func TestInteractiveRunCtrlCLeavesDaemonForExplicitShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix signal behavior is covered on supported platforms")
	}
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	configPath := filepath.Join(root, "config.json")
	writeDaemonReleaseConfig(t, configPath, stateDir, true, false)
	cmd := exec.Command(binary, "run", "-i", "--config", configPath, "--worker", "worker")
	configureInteractiveCommand(cmd)
	var stdout, stderr synchronizedBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.Env = testEnv(helperDir, map[string]string{
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
		"TICKET_ORC_FAKE_TICKET_STATE": "open",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_QUEUE":        "open",
		"TICKET_ORC_FAKE_CODEX_BLOCK":  "1",
	}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	endpoint := waitForDaemonEndpoint(t, stateDir, stderr.String)
	// The complete run -i startup view is emitted only after the supervisor
	// has started its workers and the console has obtained its first snapshot.
	testutil.Await(t, 3*time.Second, "interactive console prompt", func() bool {
		return strings.Contains(stdout.String(), "ticket-orc> ")
	}, func() string { return "stdout=" + stdout.String() + " stderr=" + stderr.String() })
	startup := stdout.String()
	loadedLine := "[ticket-orc] loaded " + configPath + "\n"
	endpointLine := strings.Index(startup, "[ticket-orc] endpoint: orc://")
	consoleBanner := strings.Index(startup, "\n[ticket-orc] interactive console; type help for commands.\n")
	if !strings.HasPrefix(startup, loadedLine) || endpointLine < len(loadedLine) || consoleBanner <= endpointLine || strings.Contains(startup, "\n\n[ticket-orc] interactive console; type help for commands.") || !strings.Contains(startup, "Daemon mode: running") || !strings.Contains(startup, "Managed workers:\n") {
		t.Fatalf("interactive startup output=%q", startup)
	}
	if strings.Count(startup, "Managed workers:\n") != 1 || strings.Contains(stderr.String(), "[ticket-orc] ready workers:") {
		t.Fatalf("interactive startup emitted duplicate/daemon summary: stdout=%q stderr=%q", startup, stderr.String())
	}
	if err := signalInteractiveGroup(cmd.Process); err != nil {
		t.Fatal(err)
	}
	// Wait for the console's post-signal prompt, then check the authoritative
	// daemon status. This proves Ctrl-C was consumed without relying on a delay.
	testutil.Await(t, 3*time.Second, "interactive console prompt after Ctrl-C", func() bool {
		return strings.Count(stdout.String(), "ticket-orc> ") >= 2
	}, func() string { return "stdout=" + stdout.String() + " stderr=" + stderr.String() })
	statusResponse := authenticatedRequest(t, &http.Client{Timeout: 3 * time.Second}, endpoint, http.MethodGet, "/v1/status", "")
	var status daemon.Status
	if statusResponse.StatusCode != http.StatusOK || json.NewDecoder(statusResponse.Body).Decode(&status) != nil || len(status.Workers) != 1 || status.Workers[0].State != "running" {
		statusResponse.Body.Close()
		t.Fatalf("status after Ctrl-C=%#v response=%d stderr=%q", status, statusResponse.StatusCode, stderr.String())
	}
	statusResponse.Body.Close()
	if !strings.Contains(stdout.String(), "^C") || !strings.Contains(stdout.String(), "ticket-orc> ") {
		t.Fatalf("interactive output=%q", stdout.String())
	}
	if _, err := postDaemon(t, &http.Client{Timeout: 3 * time.Second}, endpoint, "/v1/shutdown", ""); err != nil {
		t.Fatal(err)
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("interactive shutdown: %v stderr=%q", err, stderr.String())
	}
}

type synchronizedBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, data...)
	return len(data), nil
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.data...))
}

// Ending a detached console client leaves the foreground supervisor available.
func TestAttachEOFLeavesForegroundDaemonRunning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("foreground process lifecycle is covered on Unix")
	}
	binary, helperDir := buildFixture(t)
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	configPath := filepath.Join(root, "config.json")
	writeDaemonReleaseConfig(t, configPath, stateDir, true, false)
	run := exec.Command(binary, "run", "--config", configPath, "--worker", "worker")
	run.Env = testEnv(helperDir, map[string]string{
		"TICKET_ORC_FAKE_TICKET_ID":    integrationTicketID,
		"TICKET_ORC_FAKE_TICKET_STATE": "open",
		"TICKET_ORC_FAKE_CLAIM_STATE":  "open",
		"TICKET_ORC_FAKE_QUEUE":        "open",
	}, map[string]string{"TICKET_ORC_ACTOR": "integration-coder"})
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = run.Process.Kill()
		_ = run.Wait()
	}()
	endpoint := waitForDaemonEndpoint(t, stateDir)
	attach := exec.Command(binary, "attach", "--endpoint", endpoint.CapabilityURL())
	attach.Stdin = strings.NewReader("")
	output, err := attach.CombinedOutput()
	if err != nil {
		t.Fatalf("attach EOF: %v output=%q", err, output)
	}
	if !strings.Contains(string(output), "detached") {
		t.Fatalf("attach output=%q", output)
	}
	statusResponse := authenticatedRequest(t, &http.Client{Timeout: 3 * time.Second}, endpoint, http.MethodGet, "/v1/status", "")
	if statusResponse.StatusCode != http.StatusOK {
		statusResponse.Body.Close()
		t.Fatalf("status after attach EOF=%d", statusResponse.StatusCode)
	}
	statusResponse.Body.Close()
	if _, err := postDaemon(t, &http.Client{Timeout: 3 * time.Second}, endpoint, "/v1/shutdown", ""); err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(); err != nil {
		t.Fatalf("foreground shutdown: %v", err)
	}
}

func waitForDaemonEndpoint(t *testing.T, stateDir string, diagnostics ...func() string) daemon.Endpoint {
	t.Helper()
	var endpoint daemon.Endpoint
	testutil.Await(t, 5*time.Second, "daemon endpoint file", func() bool {
		current, err := daemon.ReadEndpoint(stateDir)
		if err != nil {
			return false
		}
		endpoint = current
		return true
	}, func() string {
		if len(diagnostics) > 0 && diagnostics[0] != nil {
			return "child stderr=" + diagnostics[0]()
		}
		return "state directory=" + stateDir
	})
	return endpoint
}

func authenticatedRequest(t *testing.T, client *http.Client, endpoint daemon.Endpoint, method, path, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, endpoint.CapabilityURL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func postDaemon(t *testing.T, client *http.Client, endpoint daemon.Endpoint, path, body string) (map[string]any, error) {
	t.Helper()
	response := authenticatedRequest(t, client, endpoint, http.MethodPost, path, body)
	defer response.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return payload, httpStatusErrorFromPayload(response.StatusCode, "POST "+path, payload)
	}
	return payload, nil
}

func httpStatusErrorFromPayload(status int, operation string, payload map[string]any) *httpStatusError {
	statusErr := &httpStatusError{status: status, operation: operation}
	errorBody, _ := payload["error"].(map[string]any)
	if errorBody == nil {
		return statusErr
	}
	statusErr.code = boundedDiagnosticText(stringValue(errorBody["code"]), 64)
	statusErr.message = boundedDiagnosticText(stringValue(errorBody["message"]), 256)
	statusErr.state = boundedDiagnosticText(stringValue(errorBody["state"]), 64)
	if applied, ok := errorBody["mutation_applied"].(bool); ok {
		statusErr.mutationApplied = &applied
	}
	return statusErr
}

type httpStatusError struct {
	status          int
	operation       string
	code            string
	message         string
	state           string
	mutationApplied *bool
}

func (e *httpStatusError) Error() string {
	detail := "daemon HTTP status " + fmt.Sprint(e.status) + " for " + e.operation
	if e.code != "" {
		detail += " code=" + e.code
	}
	if e.message != "" {
		detail += " message=" + e.message
	}
	if e.mutationApplied != nil {
		detail += " mutation_applied=" + fmt.Sprint(*e.mutationApplied)
	}
	if e.state != "" {
		detail += " state=" + e.state
	}
	return detail
}

func TestHTTPStatusErrorIncludesBoundedControlEvidence(t *testing.T) {
	err := httpStatusErrorFromPayload(http.StatusServiceUnavailable, "POST /v1/workers/worker/pause", map[string]any{
		"error": map[string]any{
			"code": "mutation_timeout", "message": "worker pause timed out",
			"mutation_applied": true, "state": "paused", "endpoint_key": "secret-capability",
		},
	})
	got := err.Error()
	for _, want := range []string{"503", "POST /v1/workers/worker/pause", "mutation_timeout", "worker pause timed out", "mutation_applied=true", "state=paused"} {
		if !strings.Contains(got, want) {
			t.Errorf("status error %q omitted %q", got, want)
		}
	}
	if strings.Contains(got, "secret-capability") {
		t.Fatalf("status error leaked unapproved response data: %q", got)
	}
}

func TestHTTPStatusErrorBoundsResponseFields(t *testing.T) {
	long := strings.Repeat("x", 512)
	err := httpStatusErrorFromPayload(http.StatusServiceUnavailable, "POST /v1/workers/worker/pause", map[string]any{
		"error": map[string]any{"code": long, "message": long, "state": long},
	})
	if len(err.code) > 67 || len(err.message) > 259 || len(err.state) > 67 {
		t.Fatalf("unbounded response fields: code=%d message=%d state=%d", len(err.code), len(err.message), len(err.state))
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func boundedDiagnosticText(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return "…" + value[len(value)-limit:]
}

func readDiagnosticFileTail(path string, limit int64) string {
	file, err := os.Open(path)
	if err != nil {
		return "<unavailable>"
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "<unavailable>"
	}
	truncated := info.Size() > limit
	if truncated {
		if _, err := file.Seek(-limit, io.SeekEnd); err != nil {
			return "<unavailable>"
		}
	}
	data, err := io.ReadAll(io.LimitReader(file, limit))
	if err != nil {
		return "<unavailable>"
	}
	if truncated {
		return "[earlier output omitted]\n" + string(data)
	}
	return string(data)
}

const eventReadTimeout = 3 * time.Second

func readEventContaining(t *testing.T, reader *bufio.Reader, text string, cancelStream func()) bool {
	t.Helper()
	timer := time.AfterFunc(eventReadTimeout, cancelStream)
	defer timer.Stop()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			cancelStream()
			return false
		}
		if strings.Contains(line, text) {
			return true
		}
	}
}

func TestReadEventContainingCancelsQuietStreamAtDeadline(t *testing.T) {
	streamReader, streamWriter := net.Pipe()
	defer streamWriter.Close()
	cancelStream := func() { _ = streamReader.Close() }
	started := time.Now()
	if readEventContaining(t, bufio.NewReader(streamReader), "worker.control", cancelStream) {
		t.Fatal("quiet stream unexpectedly contained the requested event")
	}
	if elapsed := time.Since(started); elapsed > eventReadTimeout+time.Second {
		t.Fatalf("quiet stream read took %s, want bounded by %s", elapsed, eventReadTimeout)
	}
}
