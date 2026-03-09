FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils fuse3 e2fsprogs ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY bin/loophole /usr/local/bin/loophole
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
