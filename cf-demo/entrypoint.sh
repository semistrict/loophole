#!/bin/sh
set -e

mkdir -p /root/.loophole

MODE="fuse"

if [ -n "$R2_ENDPOINT" ]; then
  cat > /root/.loophole/config.toml <<EOF
default_profile = "r2"

[profiles.r2]
endpoint = "${R2_ENDPOINT}"
bucket = "${R2_BUCKET}"
access_key = "${R2_ACCESS_KEY}"
secret_key = "${R2_SECRET_KEY}"
region = "auto"
mode = "${MODE}"
log_level = "debug"
EOF
else
  cat > /root/.loophole/config.toml <<EOF
default_profile = "local"

[profiles.local]
local_dir = "/data"
mode = "${MODE}"
log_level = "debug"
EOF
  mkdir -p /data
fi

exec loophole start -f --listen tcp://0.0.0.0:8080
