.PHONY: build install check fmt test test-lwext4-c podman deps test-containerstorage test-containerstorage-nbd e2e e2e-fuse e2e-nbd e2e-testnbdtcp e2e-lwext4fuse e2e-inprocess e2e-sqlite bench-fuse liblwext4 liblwext4-wasm clean-lwext4 wasm wasm-lwext4

.DEFAULT_GOAL := loophole

# --- lwext4 static library ---
LWEXT4_SRCDIR  := third_party/lwext4/src
LWEXT4_INCDIR  := third_party/lwext4/include
BUILD_PLATFORM := $(shell go env GOOS)-$(shell go env GOARCH)
LWEXT4_BUILDDIR := build/$(BUILD_PLATFORM)/lwext4
LWEXT4_OBJDIR  := $(LWEXT4_BUILDDIR)/obj
LWEXT4_LIB     := $(LWEXT4_BUILDDIR)/liblwext4.a

CC  ?= cc
AR  ?= ar

BINDIR := bin
GOOS   := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

BUILDTAGS :=

LWEXT4_CFLAGS := -O2 -Wall \
  -DCONFIG_USE_DEFAULT_CFG=1 \
  -DCONFIG_EXT4_BLOCKDEVS_COUNT=16 \
  -DCONFIG_EXT4_MOUNTPOINTS_COUNT=16 \
  -DCONFIG_HAVE_OWN_OFLAGS=1 \
  -DCONFIG_HAVE_OWN_ERRNO=0 \
  -DCONFIG_HAVE_OWN_ASSERT=1 \
  -DCONFIG_DEBUG_PRINTF=0 \
  -DCONFIG_DEBUG_ASSERT=0 \
  -I$(LWEXT4_INCDIR)

LWEXT4_SRCS := $(wildcard $(LWEXT4_SRCDIR)/ext4_*.c) $(LWEXT4_SRCDIR)/ext4.c
LWEXT4_OBJS := $(patsubst $(LWEXT4_SRCDIR)/%.c,$(LWEXT4_OBJDIR)/%.o,$(LWEXT4_SRCS))


liblwext4: $(LWEXT4_LIB)

$(LWEXT4_LIB): $(LWEXT4_OBJS)
	$(AR) rcs $@ $^

$(LWEXT4_OBJDIR)/%.o: $(LWEXT4_SRCDIR)/%.c | $(LWEXT4_OBJDIR)
	$(CC) $(LWEXT4_CFLAGS) -c $< -o $@

$(LWEXT4_OBJDIR):
	mkdir -p $(LWEXT4_OBJDIR)

clean-lwext4:
	rm -rf $(LWEXT4_BUILDDIR)

# Build loophole library and CLI
build: liblwext4
	go build -tags "$(BUILDTAGS)" ./...

# Build loophole binary
loophole: liblwext4
	go build -tags "$(BUILDTAGS)" -o $(BINDIR)/loophole-$(GOOS)-$(GOARCH) ./cmd/loophole

# Build e2e test binary
e2e.test: liblwext4
	go test -tags "$(BUILDTAGS)" -c -o $(BINDIR)/e2e.test-$(GOOS)-$(GOARCH) ./e2e/

# Install loophole CLI into $GOPATH/bin
install: loophole
	cp $(BINDIR)/loophole-$(GOOS)-$(GOARCH) $(shell go env GOPATH)/bin/loophole
ifeq ($(GOOS),darwin)
	codesign -s - $(shell go env GOPATH)/bin/loophole
endif

# Static checks: lint + vet + build
check: liblwext4
	golangci-lint run --build-tags "$(BUILDTAGS)" ./...

# Format all Go source files
fmt:
	gofmt -w -s $$(find . -name '*.go' -not -path './third_party/*' -not -path './old/*')

# Run unit tests (excludes e2e/linuxutil which require Linux)
# Usage: make test [RUN=TestName]
UNIT_PKGS := $(shell go list -tags "$(BUILDTAGS)" ./... | grep -v -E '/e2e$$|/linuxutil$$|/containerstorage$$')
test: liblwext4
	go test -tags "$(BUILDTAGS)" $(if $(RUN),-run '$(RUN)') $(UNIT_PKGS)

# Build and run lwext4 C tests (client-server model)
# Requires: cmake, make, e2fsprogs (mkfs.ext4)
test-lwext4-c:
	@echo "=== Building lwext4 C test tools ==="
	mkdir -p third_party/lwext4/build_test
	cd third_party/lwext4/build_test && cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 && make -j$$(nproc)
	@echo "=== Creating 128MB ext4 test images ==="
	mkdir -p third_party/lwext4/build_test/ext_images
	dd if=/dev/zero of=third_party/lwext4/build_test/ext_images/ext4 bs=1M count=128 2>/dev/null
	cd third_party/lwext4/build_test && ./fs_test/lwext4-mkfs -i ext_images/ext4 -e 4 -b 4096
	@echo "=== Running lwext4-generic tests ==="
	cd third_party/lwext4/build_test && \
		./fs_test/lwext4-generic -i ext_images/ext4 -s 1048576 -c 10 -d 100 -l -b -t
	@echo "=== Running orphan inode tests ==="
	dd if=/dev/zero of=third_party/lwext4/build_test/ext_images/ext4_orphan bs=1M count=128 2>/dev/null
	cd third_party/lwext4/build_test && ./fs_test/lwext4-mkfs -i ext_images/ext4_orphan -e 4 -b 4096
	cd third_party/lwext4/build_test && \
		./fs_test/lwext4-orphan-test ext_images/ext4_orphan

# Build podman with loophole storage driver
podman:
	cd third_party/podman && \
	go build -tags "exclude_graphdriver_btrfs exclude_graphdriver_devicemapper containers_image_openpgp" \
	-o $(CURDIR)/$(BINDIR)/podman-$(GOOS)-$(GOARCH) ./cmd/podman/

# Build linux/amd64 binary for cf-demo container (no CGo, no lwext4/sqlite)
cf-demo-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags "nosqlite nolwext4" -o cf-demo/bin/loophole ./cmd/loophole

# Build linux/amd64 binary for Fly test machine
fly-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags "nosqlite nolwext4" -o bin/loophole-linux-amd64 ./cmd/loophole

# Download third-party dependencies
deps:
	./download-deps.sh

# Run all tests: unit tests + all e2e variants
e2e: test e2e-fuse e2e-nbd e2e-testnbdtcp

# Run e2e tests (FUSE + losetup + kernel ext4)
# Usage: make e2e-fuse [RUN=TestName]
e2e-fuse: liblwext4
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (NBD + kernel ext4)
# Usage: make e2e-nbd [RUN=TestName]
e2e-nbd: liblwext4
	LOOPHOLE_MODE=nbd go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (NBD over TCP + kernel ext4)
# Usage: make e2e-testnbdtcp [RUN=TestName]
e2e-testnbdtcp: liblwext4
	LOOPHOLE_MODE=testnbdtcp go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (in-process lwext4, no FUSE/NBD/root required)
# Usage: make e2e-inprocess [RUN=TestName]
e2e-inprocess: liblwext4
	LOOPHOLE_MODE=inprocess go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (lwext4 + FUSE, no root required)
# Usage: make e2e-lwext4fuse [RUN=TestName]
e2e-lwext4fuse: liblwext4
	LOOPHOLE_MODE=fusefs go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run SQLite e2e tests (pure Go, no root/FUSE/NBD required)
# Usage: make e2e-sqlite [RUN=TestName]
e2e-sqlite: liblwext4
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') -run 'TestE2E_SQLite' ./e2e/

# Run FUSE benchmarks (ext4 by default, override with LOOPHOLE_DEFAULT_FS=xfs)
# Usage: make bench-fuse
#        make bench-fuse LOOPHOLE_DEFAULT_FS=xfs
#        make bench-fuse COUNT=5   (for benchstat: run 5 times)
bench-fuse: liblwext4
	LOG_LEVEL=error go test -tags "$(BUILDTAGS)" -bench=. -run=^$$ -benchmem -count=$(or $(COUNT),1) -timeout 600s ./e2e/

# --- lwext4 for wasm32 (TinyGo CGO) ---
WASM_CC      ?= /opt/homebrew/opt/llvm/bin/clang
WASM_AR      ?= /opt/homebrew/opt/llvm/bin/llvm-ar
WASM_SYSROOT ?= /opt/homebrew/share/wasi-sysroot
WASM_LWEXT4_BUILDDIR := build/js-wasm/lwext4
WASM_LWEXT4_OBJDIR   := $(WASM_LWEXT4_BUILDDIR)/obj
WASM_LWEXT4_LIB      := $(WASM_LWEXT4_BUILDDIR)/liblwext4.a

WASM_LWEXT4_OBJS := $(patsubst $(LWEXT4_SRCDIR)/%.c,$(WASM_LWEXT4_OBJDIR)/%.o,$(LWEXT4_SRCS))

liblwext4-wasm: $(WASM_LWEXT4_LIB)

$(WASM_LWEXT4_LIB): $(WASM_LWEXT4_OBJS)
	$(WASM_AR) rcs $@ $^

$(WASM_LWEXT4_OBJDIR)/%.o: $(LWEXT4_SRCDIR)/%.c | $(WASM_LWEXT4_OBJDIR)
	$(WASM_CC) --target=wasm32-wasi --sysroot=$(WASM_SYSROOT) $(LWEXT4_CFLAGS) -c $< -o $@

$(WASM_LWEXT4_OBJDIR):
	mkdir -p $(WASM_LWEXT4_OBJDIR)

# Build WASM binary with TinyGo.
# TinyGo CGO doesn't support ${SRCDIR}, so we generate a file with absolute paths.
GOROOT_TINYGO ?= $(HOME)/sdk/go1.25.2
wasm: liblwext4-wasm
	@printf '//go:build tinygo\n\npackage lwext4\n\n// #cgo CFLAGS: -I$(CURDIR)/third_party/lwext4/include\n// #cgo LDFLAGS: -L$(CURDIR)/build/js-wasm/lwext4\nimport "C"\n' > lwext4/cgo_generated.go
	PATH=$(GOROOT_TINYGO)/bin:$(PATH) GOROOT=$(GOROOT_TINYGO) tinygo build -target=wasm -tags "$(BUILDTAGS) nos3 tos" -o $(BINDIR)/loophole.wasm ./cmd/wasm/
	@rm -f lwext4/cgo_generated.go

# Run containerstorage integration tests (FUSE mode)
test-containerstorage: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/

# Run containerstorage integration tests (NBD mode)
test-containerstorage-nbd: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	LOOPHOLE_MODE=nbd go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/
