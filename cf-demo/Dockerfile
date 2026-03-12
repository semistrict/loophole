FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    iptables \
    iproute2 \
    python3 \
    rsync \
    squashfs-tools \
    tmux \
    && rm -rf /var/lib/apt/lists/*

COPY bin/container-control /usr/local/bin/container-control

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/container-control"]
