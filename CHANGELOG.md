# Changelog

All notable user-facing changes to `ticket-orc` are documented here.

## [0.1.1] - 2026-10-03

### Added

- Added role-level `ticket_tags` routing.
- Added `doctor --reset-local` to discard local runtime state.

### Changed

- Fixed excessive polling related resource consumption.
- Fixed stale reviewer claim after Codex startup failure.
- Fixed Codex stderr in managed worker failure output.
- Managed mode adds --skip-git-repo-check to codex exec.
- Changed to local time in human consumed timestamps.

## [0.1.0] - 2026-09-28

### Added

- Initial release

[0.1.1]: https://github.com/toolsupply/ticket-orc/releases/tag/v0.1.1
[0.1.0]: https://github.com/toolsupply/ticket-orc/releases/tag/v0.1.0
