FROM golang:1.25-bookworm AS fsx-builder

RUN apt-get update \
    && apt-get install -y libaio-dev libattr1-dev libacl1-dev xfslibs-dev \
       autoconf automake libtool pkg-config make \
    && rm -rf /var/lib/apt/lists/* \
    && git clone --depth=1 https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git /tmp/xfstests \
    && cd /tmp/xfstests \
    && make configure \
    && ./configure \
    && make -j$(nproc) \
    && cp ltp/fsx /usr/local/bin/fsx \
    && rm -rf /tmp/xfstests

FROM golang:1.25-bookworm

RUN apt-get update && apt-get install -y \
    fuse \
    e2fsprogs \
    util-linux \
    conmon \
    nbd-client \
    nbd-server \
    strace \
    fio \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fsx-builder /usr/local/bin/fsx /usr/local/bin/fsx

# Install crun from GitHub (bookworm's version is too old for podman v6)
RUN GOARCH=$(go env GOARCH) && \
    curl -fsSL "https://github.com/containers/crun/releases/download/1.26/crun-1.26-linux-${GOARCH}" \
    -o /usr/bin/crun && chmod +x /usr/bin/crun

WORKDIR /app

CMD ["go", "test", "-v", "-run", "TestFuse", "./..."]
