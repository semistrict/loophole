FROM ubuntu:24.04

RUN dpkg --add-architecture arm64 \
    && sed -i 's/^Types: deb$/Types: deb\nArchitectures: amd64/' /etc/apt/sources.list.d/ubuntu.sources \
    && printf '%s\n' \
    'Types: deb' \
    'URIs: http://ports.ubuntu.com/ubuntu-ports/' \
    'Suites: noble noble-updates noble-backports' \
    'Components: main universe restricted multiverse' \
    'Architectures: arm64' \
    'Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg' \
    '' \
    'Types: deb' \
    'URIs: http://ports.ubuntu.com/ubuntu-ports/' \
    'Suites: noble-security' \
    'Components: main universe restricted multiverse' \
    'Architectures: arm64' \
    'Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg' \
    >/etc/apt/sources.list.d/ubuntu-arm64.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    iptables \
    iproute2 \
    python3 \
    rsync \
    strace \
    squashfs-tools \
    tmux \
    && rm -rf /var/lib/apt/lists/*

COPY bin/loophole /usr/local/bin/loophole
COPY bin/loophole-sandboxd /usr/local/bin/loophole-sandboxd
COPY bin/runsc /usr/local/bin/runsc
COPY bin/container-control /usr/local/bin/container-control
COPY entrypoint.sh /usr/local/bin/cf-demo-entrypoint

RUN chmod +x /usr/local/bin/loophole /usr/local/bin/loophole-sandboxd /usr/local/bin/runsc /usr/local/bin/container-control /usr/local/bin/cf-demo-entrypoint

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/cf-demo-entrypoint"]
