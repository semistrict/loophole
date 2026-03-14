#!/bin/sh
set -e

log() {
  echo "[entrypoint] $*"
}

mkdir -p /root/.loophole

MODE="fuse"
SANDBOX_MODE="${SANDBOX_MODE:-chroot}"

log "starting"
log "sandbox_mode=${SANDBOX_MODE}"
log "uname=$(uname -a)"
log "whoami=$(id)"
log "pwd=$(pwd)"
log "PATH=${PATH}"

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
sandbox_mode = "${SANDBOX_MODE}"
EOF
else
  cat > /root/.loophole/config.toml <<EOF
default_profile = "local"

[profiles.local]
local_dir = "/data"
mode = "${MODE}"
log_level = "debug"
sandbox_mode = "${SANDBOX_MODE}"
EOF
  mkdir -p /data
fi

if [ "$SANDBOX_MODE" = "firecracker" ]; then
  log "firecracker_bin=${LOOPHOLE_FIRECRACKER_BIN:-unset}"
  log "firecracker_kernel=${LOOPHOLE_FIRECRACKER_KERNEL:-unset}"
  if [ -n "${LOOPHOLE_FIRECRACKER_BIN:-}" ] && [ -e "$LOOPHOLE_FIRECRACKER_BIN" ]; then
    ls -l "$LOOPHOLE_FIRECRACKER_BIN"
    "$LOOPHOLE_FIRECRACKER_BIN" --version || true
  else
    log "firecracker binary missing"
  fi
  if [ -n "${LOOPHOLE_FIRECRACKER_KERNEL:-}" ] && [ -e "$LOOPHOLE_FIRECRACKER_KERNEL" ]; then
    ls -l "$LOOPHOLE_FIRECRACKER_KERNEL"
  else
    log "firecracker kernel missing"
  fi
  if [ -e /dev/kvm ]; then
    ls -l /dev/kvm
  else
    log "/dev/kvm missing"
  fi
  if [ -e /dev/vhost-vsock ]; then
    ls -l /dev/vhost-vsock
  else
    log "/dev/vhost-vsock missing"
  fi
fi

PROFILE=$(awk -F'"' '/default_profile/{print $2}' /root/.loophole/config.toml)
DAEMON_SOCK="/root/.loophole/${PROFILE}.sock"
SANDBOXD_SOCK="/root/.loophole/sandboxd.sock"

log "starting loophole daemon"
loophole -p "${PROFILE}" serve --socket-path "${DAEMON_SOCK}" &
LOOPHOLE_PID=$!
log "loophole pid=${LOOPHOLE_PID}"

# Wait for daemon socket
log "waiting for daemon socket ${DAEMON_SOCK}"
for i in $(seq 1 30); do
  [ -S "$DAEMON_SOCK" ] && break
  [ "$i" = "30" ] && { log "ERROR: daemon socket not found"; exit 1; }
  sleep 1
done

log "starting sandboxd"
export LOOPHOLE_SANDBOXD_RUNSC_DEBUG="${LOOPHOLE_SANDBOXD_RUNSC_DEBUG:-true}"
export LOOPHOLE_SANDBOXD_RUNSC_PLATFORM="${LOOPHOLE_SANDBOXD_RUNSC_PLATFORM:-systrap}"
loophole-sandboxd -p "${PROFILE}" --socket-path "${SANDBOXD_SOCK}" --loophole-bin /usr/local/bin/loophole --runsc-bin /usr/local/bin/runsc &
SANDBOXD_PID=$!
log "sandboxd pid=${SANDBOXD_PID}"

log "waiting for sandboxd socket ${SANDBOXD_SOCK}"
for i in $(seq 1 30); do
  [ -S "$SANDBOXD_SOCK" ] && break
  [ "$i" = "30" ] && { log "ERROR: sandboxd socket not found"; exit 1; }
  sleep 1
done

export CONTROL_LISTEN_ADDR=:8080
export CONTROL_PROXY_TARGET=unix://${DAEMON_SOCK}
export CONTROL_SANDBOXD_TARGET=unix://${SANDBOXD_SOCK}
export CONTROL_CONTAINER_ID="${CONTAINER_DO_ID:-unknown}"
export LOOPHOLE_DEFAULT_ZYGOTE="${LOOPHOLE_DEFAULT_ZYGOTE:-ubuntu-2404-v4}"

exec /usr/local/bin/container-control
