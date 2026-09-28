# `ticket-orc` build entry point. Keep build commands visible here and in
# GitHub Actions; Go does not need a project-specific shell wrapper.

GO       ?= go
override GO_VERSION := $(shell awk '$$1 == "go" { print $$2; exit }' go.mod)
ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
RACE_PKGS ?= ./...
BIN      := bin
DIST     := dist
VERSION  := $(strip $(shell cat VERSION 2>/dev/null))
COMMIT   ?= $(shell git rev-parse --verify HEAD 2>/dev/null || printf source-archive)
LDFLAGS  ?= -s -w
TARGETS  := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64
ARCHIVES := $(DIST)/release
ARCHIVE_ROOT := $(abspath $(ARCHIVES))
SKILL    := skills/ticket-orc-worker
PKG      := ./cmd/ticket-orc
BUILD_LDFLAGS = $(LDFLAGS) -X github.com/toolsupply/ticket-orc/internal/cli.Version=$(VERSION) -X github.com/toolsupply/ticket-orc/internal/cli.Commit=$(COMMIT)

ifeq ($(VERSION),)
$(error VERSION must contain a release version)
endif

.PHONY: build dist archives release test race race-fresh vet fmt clean

build:
	@mkdir -p $(BIN)
	@test "$$($(GO) version | awk '{print $$3}')" = "go$(GO_VERSION)"
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(BUILD_LDFLAGS)" -o "$(BIN)/ticket-orc" $(PKG)

dist:
	@mkdir -p $(DIST)
	@test "$$($(GO) version | awk '{print $$3}')" = "go$(GO_VERSION)"
	@for t in $(TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; \
		out=$(DIST)/ticket-orc-$${os}-$${arch}; \
		case $$os in windows) out=$${out}.exe;; esac; \
		echo "build $$os/$$arch -> $$out"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath -ldflags "$(BUILD_LDFLAGS)" -o "$$out" $(PKG) || exit 1; \
	done

# Package the five supported binaries and worker Skill under dist/release/.
# Keep these ordinary Make recipes visible instead of hiding them in scripts.
archives: dist
	@mkdir -p "$(ARCHIVE_ROOT)"
	@rm -f "$(ARCHIVE_ROOT)"/ticket-orc-linux-amd64.tar.gz "$(ARCHIVE_ROOT)"/ticket-orc-linux-arm64.tar.gz "$(ARCHIVE_ROOT)"/ticket-orc-darwin-amd64.tar.gz "$(ARCHIVE_ROOT)"/ticket-orc-darwin-arm64.tar.gz "$(ARCHIVE_ROOT)"/ticket-orc-windows-amd64.zip "$(ARCHIVE_ROOT)"/ticket-orc-worker.zip "$(ARCHIVE_ROOT)"/SHA256SUMS
	@for t in $(TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; \
		name=ticket-orc-$$os-$$arch; \
		stage="$(ARCHIVE_ROOT)/.stage-$$os-$$arch"; \
		rm -rf "$$stage"; mkdir -p "$$stage"; \
		if [ "$$os" = windows ]; then \
			cp "$(DIST)/$$name.exe" "$$stage/ticket-orc.exe"; \
			(cd "$$stage" && zip -q "$(ARCHIVE_ROOT)/$$name.zip" ticket-orc.exe) || exit 1; \
		else \
			cp "$(DIST)/$$name" "$$stage/ticket-orc"; \
			tar -czf "$(ARCHIVE_ROOT)/$$name.tar.gz" -C "$$stage" ticket-orc || exit 1; \
		fi; \
		rm -rf "$$stage"; \
	done
	@set -eu; \
		skill_stage="$(ARCHIVE_ROOT)/.stage-ticket-orc-worker"; \
		rm -rf "$$skill_stage"; mkdir -p "$$skill_stage/ticket-orc-worker"; \
		cp -R "$(SKILL)/." "$$skill_stage/ticket-orc-worker/"; \
		(cd "$$skill_stage" && zip -q -r "$(ARCHIVE_ROOT)/ticket-orc-worker.zip" ticket-orc-worker); \
		rm -rf "$$skill_stage"
	@set -eu; \
	for t in $(TARGETS); do \
		os=$${t%/*}; arch=$${t#*/}; name=ticket-orc-$$os-$$arch; \
		if [ "$$os" = windows ]; then \
			archive="$(ARCHIVE_ROOT)/$$name.zip"; entries=$$(unzip -Z1 "$$archive"); actual=$$entries; expected='ticket-orc.exe'; \
			unzip -tq "$$archive" >/dev/null; \
		else \
			archive="$(ARCHIVE_ROOT)/$$name.tar.gz"; entries=$$(tar -tzf "$$archive"); actual=$$entries; expected='ticket-orc'; \
		fi; \
		if printf '%s\n' "$$entries" | grep -Eq '(^|/)(tickets|tickets-archive|\.ticket-orc)(/|$$)'; then echo "unexpected Ticket data in $$archive" >&2; exit 1; fi; \
		test "$$actual" = "$$expected" || { echo "unexpected entries in $$archive: $$actual" >&2; exit 1; }; \
	done; \
	skill_archive="$(ARCHIVE_ROOT)/ticket-orc-worker.zip"; \
	entries=$$(unzip -Z1 "$$skill_archive"); \
	if printf '%s\n' "$$entries" | grep -Eq '(^|/)(tickets|tickets-archive|\.ticket-orc)(/|$$)'; then echo "unexpected Ticket data in $$skill_archive" >&2; exit 1; fi; \
	unzip -tq "$$skill_archive" >/dev/null; \
	expected=$$({ cd "$(SKILL)" && find . -type f -print | sed 's#^\./#ticket-orc-worker/#'; } | LC_ALL=C sort); \
	actual=$$(printf '%s\n' "$$entries" | sed '/\/$$/d' | LC_ALL=C sort); \
		test "$$actual" = "$$expected" || { echo "unexpected entries in $$skill_archive: $$actual" >&2; exit 1; }
	@cd "$(ARCHIVE_ROOT)" && { \
		if command -v sha256sum >/dev/null 2>&1; then \
			sha256sum *.tar.gz *.zip > SHA256SUMS; \
			sha256sum -c SHA256SUMS; \
		else \
			shasum -a 256 *.tar.gz *.zip > SHA256SUMS; \
			shasum -a 256 -c SHA256SUMS; \
		fi; \
	}
	@set -eu; \
		expected=$$(printf '%s\n' 'SHA256SUMS' 'ticket-orc-darwin-amd64.tar.gz' 'ticket-orc-darwin-arm64.tar.gz' 'ticket-orc-linux-amd64.tar.gz' 'ticket-orc-linux-arm64.tar.gz' 'ticket-orc-windows-amd64.zip' 'ticket-orc-worker.zip' | LC_ALL=C sort); \
		actual=$$(cd "$(ARCHIVE_ROOT)" && find . -mindepth 1 -maxdepth 1 -print | sed 's#^\./##' | LC_ALL=C sort); \
		if [ "$$actual" != "$$expected" ]; then echo "unexpected files in $(ARCHIVE_ROOT)" >&2; printf 'expected:\n%s\nactual:\n%s\n' "$$expected" "$$actual" >&2; exit 1; fi
	@test "$$($(DIST)/ticket-orc-linux-amd64 --version)" = "ticket-orc $(VERSION) ($(COMMIT))"

release: archives

test:
	$(GO) test ./... -count=1

race:
	$(GO) test -race $(RACE_PKGS)

race-fresh:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	@bad=$$(gofmt -l .); if [ -n "$$bad" ]; then echo "gofmt needed:"; echo "$$bad"; exit 1; fi; echo "gofmt clean"

clean:
	rm -rf -- "$(ROOT)/bin" "$(ROOT)/dist"
	rm -f -- "$(ROOT)/ticket-orc" "$(ROOT)/ticket-orc.exe"
