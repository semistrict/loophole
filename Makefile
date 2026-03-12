.PHONY: build install check fmt test deps e2e e2e-fuse bench-fuse cf-demo-bin cf-demo-control-bin cf-demo-assets fly-bin

.DEFAULT_GOAL := loophole

BINDIR := bin
GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

BUILDTAGS :=

# Build loophole library and CLI
build:
	go build -tags "$(BUILDTAGS)" ./...

# Build loophole binary
loophole:
	go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-$(GOOS)-$(GOARCH) ./cmd/loophole

# Build e2e test binary
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
	gofmt -w -s $$(find . -name '*.go' -not -path './third_party/*' -not -path './old/*')

# Run unit tests (excludes e2e/linuxutil which require Linux)
# Usage: make test [RUN=TestName]
UNIT_PKGS := $(shell go list -tags "$(BUILDTAGS)" ./... | grep -v -E '/e2e$$|/linuxutil$$|/containerstorage$$')
test:
	go test -tags "$(BUILDTAGS)" $(if $(RUN),-run '$(RUN)') $(UNIT_PKGS)

# Build linux/amd64 binary for cf-demo container.
cf-demo-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cf-demo/bin/loophole ./cmd/loophole

# Build linux/amd64 stable control plane for cf-demo container.
cf-demo-control-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o cf-demo/bin/container-control ./cmd/container-control

cf-demo-assets: cf-demo-control-bin

# Build linux/amd64 binary for Fly test machine
fly-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/loophole-linux-amd64 ./cmd/loophole

# Download third-party dependencies
deps:
	./download-deps.sh

# Run unit tests plus the Linux FUSE e2e suite.
e2e: test e2e-fuse

# Run e2e tests (FUSE + losetup + kernel ext4)
# Usage: make e2e-fuse [RUN=TestName]
e2e-fuse:
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run FUSE benchmarks.
# Usage: make bench-fuse
#        make bench-fuse COUNT=5   (for benchstat: run 5 times)
bench-fuse:
	LOG_LEVEL=error go test -tags "$(BUILDTAGS)" -bench=. -run=^$$ -benchmem -count=$(or $(COUNT),1) -timeout 600s ./e2e/
