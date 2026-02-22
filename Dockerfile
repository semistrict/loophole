FROM rust:1.93 AS fsx-builder

# Build fsx from xfstests source in a stage that does not depend on Rust src/.
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

FROM rust:1.93 AS builder

RUN apt-get update && apt-get install -y \
    libfuse-dev \
    fuse \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY sqlx-macros.db ./sqlx-macros.db

# sqlx::query! needs a compile-time DATABASE_URL in container builds.
ENV DATABASE_URL=sqlite:///app/sqlx-macros.db

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && cp target/release/loophole /usr/local/bin/loophole

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y \
    fuse \
    e2fsprogs \
    mount \
    curl \
    ca-certificates \
    coreutils \
    util-linux \
    s3cmd \
    python3 \
    git \
    ripgrep \
    bpftrace \
    perf-tools-unstable \
    strace \
    sysstat \
    iproute2 \
    iputils-ping \
    net-tools \
    tcpdump \
    iftop \
    fio \
    xfsprogs \
    python3-pytest \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/loophole /usr/local/bin/loophole
COPY --from=fsx-builder /usr/local/bin/fsx /usr/local/bin/fsx
COPY tests/ /tests/
ENTRYPOINT ["loophole"]
