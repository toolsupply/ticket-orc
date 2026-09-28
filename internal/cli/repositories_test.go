package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

func TestValidateResolvedRepositoriesRejectsDuplicateTicketIDs(t *testing.T) {
	path := "/tmp/ticket-repository"
	nameA, nameB := "alpha", "beta"
	registry := supervisor.RepositoryRegistry{
		nameA: {Key: nameA, ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: path, ID: joinTestRepositoryID}},
		nameB: {Key: nameB, ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/different/mount", ID: joinTestRepositoryID}},
	}
	if err := validateResolvedRepositories(registry); err == nil {
		t.Fatal("duplicate canonical repository targets were accepted")
	}
}

func TestValidateResolvedRepositoriesAcceptsDistinctTicketIDsForSamePath(t *testing.T) {
	registry := supervisor.RepositoryRegistry{
		"alpha": {Key: "alpha", ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/tmp/ticket-repository", ID: joinTestRepositoryID}},
		"beta":  {Key: "beta", ID: joinOtherRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/tmp/ticket-repository", ID: joinOtherRepositoryID}},
	}
	if err := validateResolvedRepositories(registry); err != nil {
		t.Fatalf("validateResolvedRepositories: %v", err)
	}
}

func TestValidateResolvedRepositoriesRequiresTicketReportedID(t *testing.T) {
	registry := supervisor.RepositoryRegistry{
		"main": {Key: "main", ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: "/tmp/ticket-repository", ID: joinOtherRepositoryID}},
	}
	if err := validateResolvedRepositories(registry); err == nil {
		t.Fatal("runtime repository identity that disagrees with Ticket was accepted")
	}
}

func TestRepositoryReloadKeepsActiveRegistryWhenCandidateProbeFails(t *testing.T) {
	root := t.TempDir()
	localDir := defaultTestLocalDir(root)
	configPath := filepath.Join(root, "config.json")
	writeConfigFixture(t, configPath, `{"version":1,"repositories":{"main":{"repository":"new"}}}`)
	old := supervisor.RepositoryRegistry{"main": {Key: "main", Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: filepath.Join(root, "old")}, ID: joinTestRepositoryID, Info: &ticketclient.RepositoryInfo{Path: filepath.Join(root, "old"), ID: joinTestRepositoryID}}}
	manager := newWorkerManager(context.Background(), RunConfig{ConfigPath: configPath, StateDir: localDir, Repositories: old, repositoryProbe: func(_ context.Context, repository supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
		return ticketclient.RepositoryInfo{}, errors.New("candidate unavailable")
	}}, "unused", io.Discard, io.Discard, nil)
	if _, err := manager.reload(context.Background()); err == nil {
		t.Fatal("reload with unavailable candidate repository succeeded")
	}
	manager.configMu.Lock()
	defer manager.configMu.Unlock()
	if got := manager.repositories["main"].ID; got != joinTestRepositoryID {
		t.Fatalf("active repository identity = %q, want old registry preserved", got)
	}
}

func TestRunSupervisorPreflightsZeroWorkerRepository(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var probes atomic.Int32
	registry := supervisor.RepositoryRegistry{"ui": {
		Key:    "ui",
		Target: supervisor.TicketTarget{Mode: TicketTargetRepository, Repository: "/tmp/ticket-ui"},
	}}
	config := RunConfig{
		StateDir:     t.TempDir(),
		Repositories: registry,
		repositoryProbe: func(context.Context, supervisor.ConfiguredRepository) (ticketclient.RepositoryInfo, error) {
			probes.Add(1)
			cancel()
			return ticketclient.RepositoryInfo{Path: "/tmp/ticket-ui", ID: joinTestRepositoryID}, nil
		},
	}
	if err := runSupervisor(ctx, config, "unused", io.Discard, io.Discard); err == nil {
		t.Fatal("runSupervisor with canceled context succeeded")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("repository probes = %d, want one zero-worker probe", got)
	}
}
