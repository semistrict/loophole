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

ENV PATH="/usr/local/go/bin:${PATH}"

RUN apt-get update && apt-get install -y \
    fuse3 \
    libfuse3-dev \
    e2fsprogs \
    util-linux \
    conmon \
    nbd-client \
    nbd-server \
    strace \
    fio \
    xfsprogs \
    cmake \
    sysstat \
    procps \
    iproute2 \
    curl \
    busybox-static \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/fusermount3 /usr/bin/fusermount \
    && printf '%s\n' 'case ":$PATH:" in' \
       '  *:/usr/local/go/bin:*) ;;' \
       '  *) export PATH="/usr/local/go/bin:$PATH" ;;' \
       'esac' > /etc/profile.d/go-path.sh

COPY --from=fsx-builder /usr/local/bin/fsx /usr/local/bin/fsx

# Install crun from GitHub (bookworm's version is too old for podman v6)
RUN GOARCH=$(go env GOARCH) && \
    curl -fsSL "https://github.com/containers/crun/releases/download/1.26/crun-1.26-linux-${GOARCH}" \
    -o /usr/bin/crun && chmod +x /usr/bin/crun

# Install golangci-lint
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b /usr/local/bin

WORKDIR /app

CMD ["go", "test", "-v", "-run", "TestFuse", "./..."]
