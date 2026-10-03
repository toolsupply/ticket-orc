package cli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/harness/claude"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/harness/pi"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func executeRole(config supervisor.RoleConfig) (err error) {
	if err := ensureRuntimeOwnershipIfKnown(config.StateDir, config.InstanceID, config.LocalDirConfigured); err != nil {
		return err
	}
	eventSink := roleEventSink()
	defer func() {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		emitRoleFailure(eventSink, config.WorkerName, string(config.Role), err)
	}()
	if config.Role != RoleCoder && config.Role != RoleReviewer {
		return unavailableRoleExecutor(config)
	}
	supervised := os.Getenv(supervisedRoleEnv) == "1"
	_ = os.Unsetenv(supervisedRoleEnv)
	// The launch snapshot is only for this role process to decode. Remove it
	// before a harness child inherits the environment so resolved prompts and
	// policy never become part of an external harness environment.
	_ = os.Unsetenv(launchSnapshotEnv)
	ctx, stop := roleSignalContext(context.Background())
	defer stop()
	leaseContext, cancelLease := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancelLease()
	lease, err := state.AcquireLock(leaseContext, workerLeasePath(config), 250*time.Millisecond)
	if err != nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "worker", Operation: "lease acquisition", Phase: "lease acquisition"}, fmt.Errorf("another worker process owns this worker lease: %w", errors.Join(ErrWorkerLeaseConflict, err)))
	}
	defer lease.Release()
	agent, err := newHarness(config)
	if err != nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "harness", Operation: "construction", Phase: "harness construction"}, fmt.Errorf("startup phase harness construction: %w", err))
	}
	if err := preflightRoleHarness(config, agent); err != nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "harness", Operation: "preflight", Phase: "harness construction"}, fmt.Errorf("startup phase harness construction: %w", err))
	}
	workingDir := config.WorkingDir
	if workingDir == "" {
		workingDir, err = os.Getwd()
		if err != nil {
			return orc.NewFailureContextError(orc.FailureContext{Origin: "worker", Operation: "working directory resolution", Phase: "worker startup"}, fmt.Errorf("resolve working directory: %w", err))
		}
	}
	tickets, err := ticketclient.NewWithWorkingDirAndTarget(config.Actor, workingDir, ticketTarget(config))
	if err != nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "client startup", Phase: "Ticket client startup"}, fmt.Errorf("startup phase Ticket client startup: %w", err))
	}
	defer tickets.Close()
	if err := ctx.Err(); err != nil {
		return quietSupervisedCancellation(supervised, ctx, err)
	}

	store := roleStateStore(ctx, config, workingDir)
	if store == nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "ticket", Operation: "repository identity", Phase: "Ticket client startup"}, fmt.Errorf("startup phase Ticket client startup: resolve Ticket repository identity failed"))
	}
	cleaner, err := orc.NewCleaner(
		store,
		tickets,
		map[string]orc.SessionCleaner{config.Harness: agent},
		harness.CleanupPolicy(config.SessionCleanup),
		os.Stderr,
	)
	if err != nil {
		return orc.NewFailureContextError(orc.FailureContext{Origin: "orc_state", Operation: "cleanup setup", Phase: "worker startup"}, fmt.Errorf("startup phase cleanup setup: %w", err))
	}
	if config.Role == RoleReviewer {
		err = orc.RunReviewer(ctx, orc.ReviewerConfig{
			WorkerName:                 config.WorkerName,
			Actor:                      config.Actor,
			Harness:                    config.Harness,
			Model:                      config.Model,
			Reasoning:                  config.Reasoning,
			MaxBounces:                 config.MaxBounces,
			SessionPolicy:              string(config.SessionPolicy),
			SessionCleanup:             string(config.SessionCleanup),
			MinimumReuseContextPercent: config.MinimumReuseContextPercent,
			StateDir:                   config.StateDir,
			OutputMode:                 string(config.Output),
			ReviewCompletion:           config.ReviewCompletion,
			QueueFilters: ticketclient.QueueFilters{
				Tags:        splitTicketTags(config.TicketTags),
				WithoutTags: splitReviewSkipTags(config.ReviewSkipTags),
			},
			TicketPrompt:         config.TicketPrompt,
			CodexSandbox:         config.Codex.Sandbox,
			PiProvider:           config.Pi.Provider,
			ClaudePermissionMode: config.Claude.PermissionMode,
			WorkingDir:           workingDir,
			Operator:             os.Stdout,
			Diagnostics:          os.Stderr,
			EventSink:            eventSink,
		}, tickets, agent, store, cleaner)
		if err != nil {
			err = orc.NewFailureContextError(orc.FailureContext{Origin: "reviewer", Operation: "worker execution", Phase: "worker execution"}, err)
		}
		return quietSupervisedCancellation(supervised, ctx, err)
	}
	err = orc.RunCoder(ctx, orc.CoderConfig{
		WorkerName:                 config.WorkerName,
		Actor:                      config.Actor,
		Harness:                    config.Harness,
		Model:                      config.Model,
		Reasoning:                  config.Reasoning,
		MaxBounces:                 config.MaxBounces,
		SessionPolicy:              string(config.SessionPolicy),
		SessionCleanup:             string(config.SessionCleanup),
		MinimumReuseContextPercent: config.MinimumReuseContextPercent,
		StateDir:                   config.StateDir,
		OutputMode:                 string(config.Output),
		ReviewCompletion:           config.ReviewFinalGate,
		ReviewSkipTags:             splitReviewSkipTags(config.ReviewSkipTags),
		QueueFilters:               ticketclient.QueueFilters{Tags: splitTicketTags(config.TicketTags)},
		CodexSandbox:               config.Codex.Sandbox,
		TicketPrompt:               config.TicketPrompt,
		PiProvider:                 config.Pi.Provider,
		ClaudePermissionMode:       config.Claude.PermissionMode,
		WorkingDir:                 workingDir,
		Operator:                   os.Stdout,
		Diagnostics:                os.Stderr,
		EventSink:                  eventSink,
	}, tickets, agent, store, cleaner)
	if err != nil {
		err = orc.NewFailureContextError(orc.FailureContext{Origin: "coder", Operation: "worker execution", Phase: "worker execution"}, err)
	}
	return quietSupervisedCancellation(supervised, ctx, err)
}

func quietSupervisedCancellation(supervised bool, ctx context.Context, err error) error {
	if supervised && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}

func workerLeasePath(config supervisor.RoleConfig) string {
	owner := config.WorkerName
	if owner == "" {
		owner = config.Actor
	}
	// Role, working directory, and resolved Ticket target scope the lease.
	// Harness and model are execution settings and never identify the worker.
	owner = strings.Join([]string{owner, string(config.Role), config.WorkingDir, config.RepositoryIdentity, config.Repository, config.Ticket.Config, config.Ticket.Scope}, "\x00")
	digest := sha256.Sum256([]byte(owner))
	return filepath.Join(config.StateDir, "workers", fmt.Sprintf("%x.lease", digest[:]))
}

func roleStateStore(ctx context.Context, config supervisor.RoleConfig, workingDir string) *state.Store {
	// Every worker path must resolve a repository identity before opening
	// ticket-scoped state. This includes implicit targets, whose identity comes
	// from Ticket's authoritative info command and may differ by working dir.
	identity := config.RepositoryIdentity
	if !ticketclient.ValidRepositoryID(identity) {
		info, err := ticketclient.ProbeInfo(ctx, config.Actor, workingDir, ticketTarget(config))
		if err != nil {
			return nil
		}
		identity = repositoryIdentity(info)
	}
	if !ticketclient.ValidRepositoryID(identity) {
		return nil
	}
	return state.NewForRepository(config.StateDir, identity)
}

func roleEventSink() orc.EventSink {
	if os.Getenv("TICKET_ORC_EVENT_STREAM") != "1" {
		return nil
	}
	return func(event orc.Event) {
		data, err := json.Marshal(event)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "%s%s\n", orc.EventStreamPrefix, data)
	}
}

func newHarness(config supervisor.RoleConfig) (harness.Harness, error) {
	switch config.Harness {
	case "codex":
		return codex.New(), nil
	case "pi":
		return pi.New(), nil
	case "claude":
		return claude.New(), nil
	default:
		return nil, fmt.Errorf("unsupported harness %q", config.Harness)
	}
}
