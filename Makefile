.PHONY: build install check fmt test test-lwext4 test-lwext4-c podman deps test-containerstorage test-containerstorage-nbd e2e e2e-fuse e2e-nbd e2e-testnbdtcp e2e-lwext4fuse

# Build loophole library and CLI
build:
	go build ./...

# Install loophole CLI into $GOPATH/bin
install:
	go install ./cmd/loophole

# Static checks: lint + vet + build
check:
	golangci-lint run ./...

# Format all Go source files
fmt:
	gofmt -w -s $$(find . -name '*.go' -not -path './third_party/*' -not -path './old/*')

# Run unit tests
test:
	go test ./...

# Run lwext4 unit tests
# Usage: make test-lwext4 [RUN=TestName]
test-lwext4:
	go test -v -count=1 $(if $(RUN),-run '$(RUN)') ./lwext4/

# Build and run lwext4 C tests (client-server model)
# Requires: cmake, make, e2fsprogs (mkfs.ext4)
test-lwext4-c:
	@echo "=== Building lwext4 C test tools ==="
	mkdir -p third_party/lwext4/build_test
	cd third_party/lwext4/build_test && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$$(nproc)
	@echo "=== Creating 128MB ext4 test image ==="
	mkdir -p third_party/lwext4/build_test/ext_images
	dd if=/dev/zero of=third_party/lwext4/build_test/ext_images/ext4 bs=1M count=128 2>/dev/null
	mkfs.ext4 -q third_party/lwext4/build_test/ext_images/ext4
	@echo "=== Running lwext4-generic tests ==="
	cd third_party/lwext4/build_test && \
		./fs_test/lwext4-generic -i ext_images/ext4 -s 1048576 -c 10 -d 100 -l -b -t

# Build podman with loophole storage driver
podman:
	cd third_party/podman && \
	go build -tags "exclude_graphdriver_btrfs exclude_graphdriver_devicemapper containers_image_openpgp" \
	-o /app/bin/podman ./cmd/podman/

# Download third-party dependencies
deps:
	./download-deps.sh

# Run all tests: unit tests + all e2e variants
e2e: test e2e-fuse e2e-nbd e2e-testnbdtcp

# Run e2e tests (FUSE + losetup + kernel ext4)
# Usage: make e2e-fuse [RUN=TestName]
e2e-fuse:
	go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (NBD + kernel ext4)
# Usage: make e2e-nbd [RUN=TestName]
e2e-nbd:
	LOOPHOLE_MODE=nbd go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (NBD over TCP + kernel ext4)
# Usage: make e2e-testnbdtcp [RUN=TestName]
e2e-testnbdtcp:
	LOOPHOLE_MODE=testnbdtcp go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (lwext4 + FUSE, no root required)
# Usage: make e2e-lwext4fuse [RUN=TestName]
e2e-lwext4fuse:
	LOOPHOLE_MODE=lwext4fuse go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run containerstorage integration tests (FUSE mode)
test-containerstorage: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	go test -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/

# Run containerstorage integration tests (NBD mode)
test-containerstorage-nbd: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	LOOPHOLE_MODE=nbd go test -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/
