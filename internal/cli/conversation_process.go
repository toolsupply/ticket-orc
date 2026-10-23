package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func productionConversationHooks(configPath, stateDir string) supervisor.ConversationProcessHooks {
	hooks := supervisor.ConversationHarnessHooks(stateDir, func(c state.Conversation) (harness.Harness, error) {
		return codex.NewManaged("codex", conversationEnvironment(os.Environ(), configPath, c), runConversationProcess)
	})
	hooks.Validate = validateConversationLaunch
	return hooks
}

func validateConversationLaunch(ctx context.Context, c state.Conversation) error {
	e := c.Execution
	fail := func() error { return &orc.ConversationError{Code: "profile_unavailable"} }
	if e.Harness != "codex" || ticketclient.ValidateActor(e.Actor) != nil || e.MaxDuration <= 0 {
		return fail()
	}
	for _, dir := range []string{e.WorkingDir, e.Workspace} {
		if !filepath.IsAbs(dir) {
			return fail()
		}
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			return fail()
		}
	}
	if err := codex.New().Validate(harness.PreflightConfig{Model: e.Model, Reasoning: e.Reasoning, OutputMode: "quiet", CodexSandbox: e.CodexSandbox, CodexServiceTier: e.CodexServiceTier}); err != nil {
		return fail()
	}
	if _, err := exec.LookPath("codex"); err != nil {
		return fail()
	}
	for _, r := range e.Repositories {
		info, err := ticketclient.ProbeInfo(ctx, e.Actor, e.WorkingDir, ticketclient.Target{Repository: r.Path})
		if err != nil || info.ID != r.ID {
			return &orc.ConversationError{Code: "repository_unavailable"}
		}
	}
	return nil
}

func conversationEnvironment(base []string, configPath string, c state.Conversation) []string {
	// Use normal private instance discovery, without copying bearer secrets into
	// prompts or registering this session as a scheduled worker. Inherited client
	// endpoint overrides must not redirect ordinary management away from this host.
	env := make([]string, 0, len(base)+2)
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "TICKET_ORC_ENDPOINT", "TICKET_ORC_WORKER", "TICKET_ORC_ACTOR", supervisedRoleEnv, launchSnapshotEnv, "TICKET_ORC_EVENT_STREAM", "TICKET_ORC_CONFIG", "TICKET_ORC_STATE_DIR", "TICKET_ORC_DAEMON_TOKEN", "TICKET_ORC_ENDPOINT_TOKEN", "TICKET_ORC_BEARER_TOKEN", "TICKET_REPOSITORY", "TICKET_CONFIG", "TICKET_SCOPE", "TICKET_ROOT", "TICKET_CURRENT":
			continue
		}
		env = append(env, entry)
	}
	env = setChildEnvironment(env, "TICKET_ORC", filepath.Dir(configPath))
	target := ticketclient.Target{}
	if len(c.Execution.Repositories) > 0 {
		target.Repository = c.Execution.Repositories[0].Path
	}
	return ticketclient.ApplyTargetEnvironment(env, c.Execution.Actor, target)
}

// runConversationProcess owns the complete process group/job, just as managed
// workers do. Cancellation kills the owned group and Wait drains output before
// the supervisor can publish a terminal turn. Raw stderr is never public.
func runConversationProcess(ctx context.Context, req harness.ProcessRequest) (harness.ProcessResult, error) {
	cmd := exec.Command(req.Executable, req.Args...)
	cmd.Dir, cmd.Env = req.Dir, req.Env
	configureChildProcess(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return harness.ProcessResult{ExitCode: -1}, err
	}
	cmd.Stderr = io.Discard
	if err := ctx.Err(); err != nil {
		return harness.ProcessResult{ExitCode: -1}, err
	}
	if err := cmd.Start(); err != nil {
		return harness.ProcessResult{ExitCode: -1}, err
	}
	containment, err := attachChildContainment(cmd.Process)
	if err != nil {
		_ = forceChildStop(cmd.Process)
		_ = cmd.Wait()
		return harness.ProcessResult{ExitCode: -1}, err
	}
	defer containment.close()
	copyDone := make(chan error, 1)
	go func() {
		dst := req.Stdout
		if dst == nil {
			dst = io.Discard
		}
		_, err := io.Copy(dst, stdout)
		if err != nil {
			_ = containment.force(cmd.Process)
		}
		copyDone <- err
	}()
	stopped := make(chan struct{})
	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		select {
		case <-ctx.Done():
			_ = containment.force(cmd.Process)
		case <-stopped:
		}
	}()
	if waitForChildExitBeforeReap(cmd.Process) {
		_ = containment.forceAfterLeaderExit(cmd.Process)
	} else {
		// Failed exit observation cannot justify leaving descendants running.
		_ = containment.force(cmd.Process)
	}
	copyErr := <-copyDone
	// Join cancellation before reaping so the group identity cannot be reused.
	close(stopped)
	<-cancelDone
	err = cmd.Wait()
	if copyErr != nil {
		err = copyErr
	}
	return conversationProcessResult(err)
}

func conversationProcessResult(err error) (harness.ProcessResult, error) {
	code := 0
	if err != nil {
		code = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		}
	}
	return harness.ProcessResult{ExitCode: code}, err
}
