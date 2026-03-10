FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    containerd \
    runc \
    e2fsprogs \
    fuse3 \
    nbd-client \
    nbd-server \
    util-linux \
    curl \
    iproute2 \
    procps \
    strace \
    && rm -rf /var/lib/apt/lists/*

# containerd config with loophole proxy snapshotter
COPY containerd-config.toml /etc/containerd/config.toml

# Startup script
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
