.PHONY: build install test podman deps test-containerstorage test-containerstorage-nbd e2e e2e-nbd

# Build loophole library and CLI
build:
	go build ./...

# Install loophole CLI into $GOPATH/bin
install:
	go install ./cmd/loophole

# Run unit tests
test:
	go test ./...

# Build podman with loophole storage driver
podman:
	cd third_party/podman && \
	go build -tags "exclude_graphdriver_btrfs exclude_graphdriver_devicemapper containers_image_openpgp" \
	-o /app/bin/podman ./cmd/podman/

# Download third-party dependencies
deps:
	./download-deps.sh

# Run e2e tests (FUSE + losetup + kernel ext4)
# Usage: make e2e [RUN=TestName]
e2e:
	go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run e2e tests (NBD + kernel ext4)
# Usage: make e2e-nbd [RUN=TestName]
e2e-nbd:
	LOOPHOLE_MODE=nbd go test -v -count=1 -timeout 300s $(if $(RUN),-run '$(RUN)') ./e2e/

# Run containerstorage integration tests (FUSE mode)
test-containerstorage: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	go test -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/

# Run containerstorage integration tests (NBD mode)
test-containerstorage-nbd: install
	mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
	LOOPHOLE_MODE=nbd go test -v -count=1 -timeout 600s -run TestLoophole ./containerstorage/
