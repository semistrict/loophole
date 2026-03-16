.PHONY: build loophole loophole-sandboxd runsc install check fmt test clean deps e2e bench-fuse create-zygote cf-demo-bin cf-demo-control-bin cf-demo-sandboxd-bin cf-demo-runsc-bin cf-demo-rootfs-tar cf-demo-assets cf-demo-assets-local cf-demo-smoke-local fly-bin

.DEFAULT_GOAL := loophole

BINDIR := bin
GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)
RUNSC_GOOS := linux
CF_DEMO_GOARCH ?= amd64
CF_DEMO_BASE_URL ?= http://localhost:7935

BUILDTAGS :=

# Build loophole library and CLI
build:
	go build -tags "$(BUILDTAGS)" ./...

# Build loophole binary
loophole:
	go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-$(GOOS)-$(GOARCH) ./cmd/loophole

# Build sandbox daemon binary
loophole-sandboxd:
	CGO_ENABLED=0 go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-sandboxd-$(GOOS)-$(GOARCH) ./cmd/loophole-sandboxd

# Build runsc from the vendored gVisor tree.
runsc:
	mkdir -p $(BINDIR)
	GOWORK=off CGO_ENABLED=0 GOOS=$(RUNSC_GOOS) GOARCH=$(GOARCH) go build -C third_party/gvisor -o ../../$(BINDIR)/runsc-$(RUNSC_GOOS)-$(GOARCH) ./runsc

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
	gofmt -w -s $$(find . -name '*.go' -not -path './third_party/*' -not -path './old/*')

# Remove generated binaries and test artifacts.
clean:
	rm -rf $(BINDIR)
	rm -f cf-demo/bin/loophole cf-demo/bin/container-control cf-demo/bin/ubuntu-rootfs.tar

# Run unit tests (excludes e2e/linuxutil which require Linux)
# Usage: make test [RUN=TestName]
UNIT_PKGS := $(shell go list -tags "$(BUILDTAGS)" ./... | grep -v -E '/e2e$$|/linuxutil$$|/containerstorage$$')
test:
	go test -tags "$(BUILDTAGS)" $(if $(RUN),-run '$(RUN)') $(UNIT_PKGS)

# Build linux/amd64 binary for cf-demo container.
cf-demo-bin:
	mkdir -p cf-demo/bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(CF_DEMO_GOARCH) go build -o cf-demo/bin/loophole ./cmd/loophole

# Build linux/amd64 stable control plane for cf-demo container.
cf-demo-control-bin:
	mkdir -p cf-demo/bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(CF_DEMO_GOARCH) go build -trimpath -ldflags="-s -w" -o cf-demo/bin/container-control ./cmd/container-control

cf-demo-sandboxd-bin:
	mkdir -p cf-demo/bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(CF_DEMO_GOARCH) go build -o cf-demo/bin/loophole-sandboxd ./cmd/loophole-sandboxd


cf-demo-runsc-bin:
	$(MAKE) runsc GOARCH=$(CF_DEMO_GOARCH)
	mkdir -p cf-demo/bin
	cp bin/runsc-linux-$(CF_DEMO_GOARCH) cf-demo/bin/runsc

# Create a zygote volume in R2 from Ubuntu 24.04 rootfs.
# Usage: make create-zygote [ZYGOTE=name] [PROFILE=r2] [PLATFORM=linux/amd64]
create-zygote:
	LOOPHOLE_PROFILE=$(or $(PROFILE),r2) PLATFORM=$(or $(PLATFORM),linux/amd64) scripts/create-zygote.sh $(or $(ZYGOTE),ubuntu-2404-v4)

cf-demo-rootfs-tar:
	mkdir -p cf-demo/bin
	CID=$$(docker create --platform linux/$(CF_DEMO_GOARCH) ubuntu:24.04 /bin/true) && \
	trap 'docker rm -f "$$CID" >/dev/null 2>&1 || true' EXIT INT TERM && \
	docker export "$$CID" -o cf-demo/bin/ubuntu-rootfs.tar

cf-demo-assets: CF_DEMO_GOARCH=amd64
cf-demo-assets: cf-demo-bin cf-demo-control-bin cf-demo-sandboxd-bin cf-demo-runsc-bin

cf-demo-assets-local: CF_DEMO_GOARCH=amd64
cf-demo-assets-local: cf-demo-bin cf-demo-control-bin cf-demo-sandboxd-bin cf-demo-runsc-bin

cf-demo-smoke-local:
	CF_DEMO_BASE_URL=$(CF_DEMO_BASE_URL) CF_DEMO_SMOKE_VOLUME=$(CF_DEMO_SMOKE_VOLUME) pnpm -C cf-demo run smoke:local

# Build linux/amd64 binary for Fly test machine
fly-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/loophole-linux-amd64 ./cmd/loophole

# Download third-party dependencies
deps:
	./download-deps.sh

# Run e2e tests (losetup + kernel ext4 over FUSE).
# Usage: make e2e [RUN=TestName]
e2e:
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 600s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run FUSE benchmarks.
# Usage: make bench-fuse
#        make bench-fuse COUNT=5   (for benchstat: run 5 times)
bench-fuse:
	LOG_LEVEL=error go test -tags "$(BUILDTAGS)" -bench=. -run=^$$ -benchmem -count=$(or $(COUNT),1) -timeout 600s ./e2e/
