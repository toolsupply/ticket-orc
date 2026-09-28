package cli

import (
	"github.com/toolsupply/ticket-orc/internal/harness"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

// preflightRoleConfig applies runtime harness checks after pure configuration
// resolution. It may inspect or invoke an external harness and must not be
// called by parsers or declarative worker resolution.
func preflightRoleConfig(config supervisor.RoleConfig) error {
	agent, err := newHarness(config)
	if err != nil {
		return err
	}
	return preflightRoleHarness(config, agent)
}

func preflightRoleHarness(config supervisor.RoleConfig, agent harness.Harness) error {
	if err := harness.Preflight(agent, harness.PreflightConfig{
		Model:                config.Model,
		Reasoning:            config.Reasoning,
		SessionPolicy:        string(config.SessionPolicy),
		SessionCleanup:       harness.CleanupPolicy(config.SessionCleanup),
		OutputMode:           string(config.Output),
		CodexSandbox:         config.Codex.Sandbox,
		PiProvider:           config.Pi.Provider,
		ClaudePermissionMode: config.Claude.PermissionMode,
	}); err != nil {
		return err
	}
	return nil
}
