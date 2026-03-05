.PHONY: build install check fmt test test-lwext4-c podman deps test-containerstorage test-containerstorage-nbd e2e e2e-fuse e2e-nbd e2e-testnbdtcp e2e-lwext4fuse e2e-juicefs e2e-juicefsfuse e2e-sqlite liblwext4 clean-lwext4

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

# JuiceFS build tags: exclude all backends except sqlite (meta) and s3 (object storage)
JUICEFS_NOTAGS := noredis nomysql nopg notikv noetcd nobadger noazure nob2 nobos nocifs nocos nodragonfly nogs nohdfs noibmcos nonfs noobs nooss noqingstore noqiniu nosftp noswift noufile nowebdav
BUILDTAGS := $(JUICEFS_NOTAGS)

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

# Run e2e tests (lwext4 + FUSE, no root required)
# Usage: make e2e-lwext4fuse [RUN=TestName]
e2e-lwext4fuse: liblwext4
	LOOPHOLE_MODE=lwext4fuse go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (JuiceFS in-process, no root/FUSE/NBD required)
# Usage: make e2e-juicefs [RUN=TestName]
e2e-juicefs: liblwext4
	LOOPHOLE_MODE=juicefs go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (JuiceFS + FUSE, requires root and FUSE)
# Usage: make e2e-juicefsfuse [RUN=TestName]
e2e-juicefsfuse: liblwext4
	LOOPHOLE_MODE=juicefsfuse go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run SQLite e2e tests (pure Go, no root/FUSE/NBD required)
# Usage: make e2e-sqlite [RUN=TestName]
e2e-sqlite: liblwext4
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') -run 'TestE2E_SQLite' ./e2e/

# Run containerstorage integration tests (FUSE mode)
test-containerstorage: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/

# Run containerstorage integration tests (NBD mode)
test-containerstorage-nbd: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	LOOPHOLE_MODE=nbd go test -tags "$(BUILDTAGS)" -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/
