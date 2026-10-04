package ticketclient

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const MinimumSupportedVersion = "0.2.4"

var minimumVersionChecks sync.Map // absolute executable path -> *versionCheck

type versionCheck struct {
	once sync.Once
	err  error
}

// RequireMinimumVersion checks the Ticket executable selected by PATH once
// per resolved executable path in this Orc process.
func RequireMinimumVersion(ctx context.Context) error {
	executable, err := exec.LookPath(defaultExecutable)
	if err != nil {
		return &VersionError{Code: "executable_not_found"}
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return &VersionError{Code: "invalid_executable"}
	}
	return requireMinimumVersionCached(ctx, executable)
}

func requireMinimumVersionCached(ctx context.Context, executable string) error {
	value, _ := minimumVersionChecks.LoadOrStore(executable, &versionCheck{})
	check := value.(*versionCheck)
	check.once.Do(func() { check.err = RequireMinimumVersionWithExecutable(ctx, executable) })
	return check.err
}

// VersionError describes a safe failure while checking Ticket compatibility.
// It never includes arbitrary output from the Ticket executable.
type VersionError struct {
	Code string
}

func (e *VersionError) Error() string {
	message := "could not determine Ticket version; ticket-orc requires ticket >= v" + MinimumSupportedVersion
	if e == nil || e.Code == "" {
		return message
	}
	return message + " (" + e.Code + ")"
}

// TicketVersionWithExecutable runs only the Ticket version command and returns
// its parsed semantic version. Output is bounded and errors do not expose it.
func TicketVersionWithExecutable(ctx context.Context, executable string) (string, error) {
	if ctx == nil {
		return "", &VersionError{Code: "invalid_context"}
	}
	if strings.TrimSpace(executable) == "" {
		return "", &VersionError{Code: "invalid_executable"}
	}
	command := exec.CommandContext(ctx, executable, "--version")
	stdout := newBoundedBuffer(maxProbeOutputBytes)
	stderr := newBoundedBuffer(maxStderrBytes)
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", &VersionError{Code: "timed_out"}
	}
	if stdout.Truncated() || stderr.Truncated() {
		return "", &VersionError{Code: "output_too_large"}
	}
	if err != nil {
		return "", &VersionError{Code: "command_failed"}
	}
	version, ok := parseTicketVersion(stdout.String())
	if !ok {
		return "", &VersionError{Code: "malformed_output"}
	}
	return version, nil
}

// RequireMinimumVersionWithExecutable verifies the selected executable against
// Orc's minimum supported Ticket release.
func RequireMinimumVersionWithExecutable(ctx context.Context, executable string) error {
	version, err := TicketVersionWithExecutable(ctx, executable)
	if err != nil {
		return err
	}
	if compareSemanticVersions(version, MinimumSupportedVersion) < 0 {
		return fmt.Errorf("incompatible Ticket version: ticket-orc requires ticket >= v%s; found v%s", MinimumSupportedVersion, version)
	}
	return nil
}

func parseTicketVersion(output string) (string, bool) {
	output = strings.TrimSuffix(output, "\n")
	output = strings.TrimSuffix(output, "\r")
	if strings.ContainsAny(output, "\r\n") || !strings.HasPrefix(output, "ticket ") {
		return "", false
	}
	rest := strings.TrimPrefix(output, "ticket ")
	marker := " (api "
	i := strings.Index(rest, marker)
	if i < 0 {
		return "", false
	}
	version := strings.TrimPrefix(rest[:i], "v")
	suffix := rest[i+len(marker):]
	if !strings.HasSuffix(suffix, ")") {
		return "", false
	}
	suffix = strings.TrimSuffix(suffix, ")")
	api, storage, ok := strings.Cut(suffix, ", storage ")
	if !ok || !allDecimal(api) || !allDecimal(storage) {
		return "", false
	}
	if _, err := strconv.ParseUint(api, 10, 32); err != nil {
		return "", false
	}
	if _, err := strconv.ParseUint(storage, 10, 32); err != nil {
		return "", false
	}
	if _, ok := parseSemanticVersion(version); !ok {
		return "", false
	}
	return version, true
}

func allDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

type semanticVersion struct {
	major, minor, patch uint64
	prerelease          []string
}

func parseSemanticVersion(value string) (semanticVersion, bool) {
	var version semanticVersion
	coreAndPre, _, _ := strings.Cut(value, "+")
	if strings.Contains(value, "+") {
		build := strings.SplitN(value, "+", 2)[1]
		if !validIdentifiers(build, false) {
			return version, false
		}
	}
	core, pre, hasPre := strings.Cut(coreAndPre, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return version, false
	}
	values := []*uint64{&version.major, &version.minor, &version.patch}
	for i, part := range parts {
		if !canonicalDecimal(part) {
			return version, false
		}
		parsed, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return version, false
		}
		*values[i] = parsed
	}
	if hasPre {
		if !validIdentifiers(pre, true) {
			return version, false
		}
		version.prerelease = strings.Split(pre, ".")
	}
	return version, true
}

func canonicalDecimal(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	return allDecimal(value)
}

func validIdentifiers(value string, rejectLeadingZeroNumeric bool) bool {
	if value == "" {
		return false
	}
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		numeric := true
		for _, r := range identifier {
			if !((r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '-') {
				return false
			}
			if r < '0' || r > '9' {
				numeric = false
			}
		}
		if rejectLeadingZeroNumeric && numeric && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func compareSemanticVersions(left, right string) int {
	a, _ := parseSemanticVersion(left)
	b, _ := parseSemanticVersion(right)
	for _, pair := range [][2]uint64{{a.major, b.major}, {a.minor, b.minor}, {a.patch, b.patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if len(a.prerelease) == 0 && len(b.prerelease) == 0 {
		return 0
	}
	if len(a.prerelease) == 0 {
		return 1
	}
	if len(b.prerelease) == 0 {
		return -1
	}
	for i := 0; i < len(a.prerelease) && i < len(b.prerelease); i++ {
		leftID, rightID := a.prerelease[i], b.prerelease[i]
		leftIsNum := allDecimal(leftID)
		rightIsNum := allDecimal(rightID)
		switch {
		case leftIsNum && rightIsNum:
			if len(leftID) < len(rightID) {
				return -1
			}
			if len(leftID) > len(rightID) {
				return 1
			}
			if leftID < rightID {
				return -1
			}
			if leftID > rightID {
				return 1
			}
		case leftIsNum:
			return -1
		case rightIsNum:
			return 1
		default:
			if leftID < rightID {
				return -1
			}
			if leftID > rightID {
				return 1
			}
		}
	}
	if len(a.prerelease) < len(b.prerelease) {
		return -1
	}
	if len(a.prerelease) > len(b.prerelease) {
		return 1
	}
	return 0
}
