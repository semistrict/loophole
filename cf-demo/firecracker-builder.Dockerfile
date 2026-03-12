# Toolchain image for building the Firecracker binary.
# Used as a long-lived container with bind-mounted source code.
# Build: docker build -t loophole-firecracker-builder -f cf-demo/firecracker-builder.Dockerfile .
# Run:   docker run -d --name loophole-fc-builder -v $PWD:/src/loophole -w /src/loophole loophole-firecracker-builder sleep infinity
# Exec:  docker exec loophole-fc-builder make libloophole.a && docker exec loophole-fc-builder cargo build ...
FROM golang:1.25.2-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential clang libclang-dev libseccomp-dev pkg-config make curl ca-certificates git python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable

ENV PATH=/root/.cargo/bin:$PATH
