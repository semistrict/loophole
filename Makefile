.PHONY: build loophole loophole-cached install check fmt test clean deps e2e e2e-requirenbd e2e-nonbd bench-fuse fly-bin

.DEFAULT_GOAL := loophole

BINDIR := bin
GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

BUILDTAGS :=

# Build loophole library and CLI
build:
	go build -tags "$(BUILDTAGS)" ./...

# Build loophole binary (also builds loophole-cached, which loophole spawns)
loophole: loophole-cached
	go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-$(GOOS)-$(GOARCH) ./cmd/loophole

# Build loophole-cached daemon binary
loophole-cached:
	go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-cached-$(GOOS)-$(GOARCH) ./cmd/loophole-cached

# Build e2e test binary
e2e.test:
	go test -tags "$(BUILDTAGS)" -c -o e2e.test ./e2e

install: loophole
	cp $(BINDIR)/loophole-$(GOOS)-$(GOARCH) $(shell go env GOPATH)/bin/loophole
ifeq ($(GOOS),darwin)
	codesign -s - $(shell go env GOPATH)/bin/loophole
endif

# Static checks: lint + vet + build
check:
	golangci-lint run --build-tags "$(BUILDTAGS)" ./...

# Format all Go source files
fmt:
	gofmt -w -s $$(find . -name '*.go' -not -path './old/*')

# Remove generated binaries and test artifacts.
clean:
	rm -rf $(BINDIR)

# Run unit tests (excludes e2e/linuxutil which require Linux)
# Usage: make test [RUN=TestName]
UNIT_TEST_TIMEOUT := 120s
UNIT_PKGS ?= $(shell go list -tags "$(BUILDTAGS)" ./... | grep -v -E '/e2e$$|/linuxutil$$|/containerstorage$$')
test:
	go test -race -tags "$(BUILDTAGS)" -timeout $(UNIT_TEST_TIMEOUT) $(if $(RUN),-run '$(RUN)') $(UNIT_PKGS)

# Build linux/amd64 binary for Fly test machine
fly-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/loophole-linux-amd64 ./cmd/loophole

# Download third-party dependencies
deps:
	./download-deps.sh

# Run e2e tests (losetup + kernel ext4 over FUSE).
# Usage: make e2e [RUN=TestName]
E2E_TEST_TIMEOUT ?= 1800s
e2e:
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout $(E2E_TEST_TIMEOUT) $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests with NBD required. This fails fast if NBD is unavailable.
e2e-requirenbd:
	LOOPHOLE_OPTIONS=requirenbd $(MAKE) e2e RUN='$(RUN)'

# Run e2e tests with NBD explicitly disabled, forcing the FUSE path.
e2e-nonbd:
	LOOPHOLE_OPTIONS=nonbd $(MAKE) e2e RUN='$(RUN)'

# Run FUSE benchmarks.
# Usage: make bench-fuse
#        make bench-fuse COUNT=5   (for benchstat: run 5 times)
bench-fuse:
	LOG_LEVEL=error go test -tags "$(BUILDTAGS)" -bench=. -run=^$$ -benchmem -count=$(or $(COUNT),1) -timeout 600s ./e2e/
