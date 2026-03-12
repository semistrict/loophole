#!/bin/bash
# cf-boot-firecracker.sh — Prepare a Firecracker-backed sandbox volume in a
# Cloudflare container using the real loophole daemon path.
#
# Runs locally; sends commands to CF container via control exec endpoint.
# Requires: cf-stage-firecracker.sh to have been run first.
#
# Flow:
#   1. Preflight — verify staged binaries + /dev/kvm
#   2. Download + convert rootfs (squashfs → ext4 with guest agent)
#   3. Start loophole daemon with sandbox_mode=firecracker
#   4. Import rootfs ext4 into a loophole volume
#   5. Test sandbox/exec via the daemon (triggers VM boot + netns)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

BASE_URL="${BASE_URL:-https://cf-demo-4.ramon3525.workers.dev}"
CONTAINER_ID="fc-firecracker-1"

STAGE_DIR="/tmp/loophole-stage"
WORK_DIR="/tmp/loophole-fc"
STORE_DIR="${WORK_DIR}/store"
CONFIG_DIR="/root/.loophole"
PROFILE="r2"
VOLUME_NAME="rootfs"

FC_BIN="${STAGE_DIR}/bin/firecracker"
LOOPHOLE_BIN="${STAGE_DIR}/bin/loophole"
GUEST_AGENT_BIN="${STAGE_DIR}/bin/loophole-guest-agent"
KERNEL="${STAGE_DIR}/assets/vmlinux-6.1.155"
DAEMON_SOCK="${CONFIG_DIR}/${PROFILE}.sock"

ROOTFS_URL="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/ubuntu-24.04.squashfs"

SKIP_ROOTFS=false
SKIP_IMPORT=false

usage() {
  cat <<EOF
Usage: $0 [options]

Prepare a Firecracker sandbox volume in a Cloudflare container.
Requires cf-stage-firecracker.sh to have been run first.

Options:
  --url URL              Worker base URL (default: ${BASE_URL})
  --skip-rootfs          Skip rootfs download/conversion (reuse existing)
  --skip-import          Skip loophole volume import (reuse existing)
  -h, --help             Show this help
EOF
}

while (($#)); do
  case "$1" in
    --url)          BASE_URL="$2"; shift 2 ;;
    --skip-rootfs)  SKIP_ROOTFS=true; shift ;;
    --skip-import)  SKIP_IMPORT=true; shift ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

log() { echo "=== $* ===" >&2; }

# bg_exec argv... — run a command in the container via bg-exec, poll until done,
# print output. Returns the remote exit code.
bg_exec() {
  local argv_json
  argv_json=$(python3 -c "import json,sys; print(json.dumps(sys.argv[1:]))" "$@")

  local start_resp
  start_resp=$(curl -fsS -X POST -G \
    --data-urlencode "argv=${argv_json}" \
    "${BASE_URL}/debug/control/${CONTAINER_ID}/bg-exec")
  local job_id
  job_id=$(echo "$start_resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

  local poll_url="${BASE_URL}/debug/control/${CONTAINER_ID}/bg-exec/${job_id}"
  while true; do
    sleep 2
    local poll_resp
    poll_resp=$(curl -fsS "${poll_url}") || continue
    local done
    done=$(echo "$poll_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('done',False))")
    if [ "$done" = "True" ]; then
      local output exit_code
      output=$(echo "$poll_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('output',''))")
      exit_code=$(echo "$poll_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('exitCode',1))")
      [ -n "$output" ] && echo "$output"
      return "$exit_code"
    fi
  done
}

# Helper: run a command via the daemon's Unix socket inside the container.
daemon_curl() {
  local method="$1"
  local path="$2"
  shift 2
  local extra_args=""
  [ $# -gt 0 ] && extra_args="$*"
  bg_exec curl -sS -X "${method}" --unix-socket "${DAEMON_SOCK}" "http://localhost${path}" ${extra_args}
}

# --- Preflight ---

log "Preflight checks"
bg_exec test -x "${FC_BIN}" -a -x "${LOOPHOLE_BIN}" -a -x "${GUEST_AGENT_BIN}" -a -f "${KERNEL}" >/dev/null
echo "  staged binaries: ok"

bg_exec test -e /dev/kvm >/dev/null
echo "  /dev/kvm: ok"

# --- Prepare rootfs ---

if [ "$SKIP_ROOTFS" = false ]; then
  log "Downloading rootfs squashfs"
  bg_exec sh -c "mkdir -p ${WORK_DIR} && curl -fL --continue-at - -o ${WORK_DIR}/ubuntu.squashfs '${ROOTFS_URL}'"
  echo "  downloaded"

  log "Converting squashfs to ext4 with guest agent"
  bg_exec sh -c "$(cat <<'CONVERT_SCRIPT'
set -e
WORK_DIR=/tmp/loophole-fc
STAGE_DIR=/tmp/loophole-stage
SQUASH_TMP="${WORK_DIR}/squashfs-root"
rm -rf "$SQUASH_TMP"

command -v unsquashfs >/dev/null 2>&1 || {
  DEBIAN_FRONTEND=noninteractive apt-get update -qq
  apt-get install -y -qq squashfs-tools e2fsprogs
}

unsquashfs -d "$SQUASH_TMP" "${WORK_DIR}/ubuntu.squashfs"

# Inject guest agent
cp "${STAGE_DIR}/bin/loophole-guest-agent" "${SQUASH_TMP}/sbin/loophole-guest-agent"
chmod 755 "${SQUASH_TMP}/sbin/loophole-guest-agent"

# Static resolv.conf (guest agent configures networking, but DNS needs this file)
rm -f "${SQUASH_TMP}/etc/resolv.conf"
echo "nameserver 8.8.8.8" > "${SQUASH_TMP}/etc/resolv.conf"

# Create ext4 image
rm -f "${WORK_DIR}/ubuntu.ext4"
truncate -s 1G "${WORK_DIR}/ubuntu.ext4"
mkfs.ext4 -d "$SQUASH_TMP" -F "${WORK_DIR}/ubuntu.ext4"
rm -rf "$SQUASH_TMP"
CONVERT_SCRIPT
  )"
  echo "  converted"
fi

# --- Set up loophole config and start daemon ---

log "Setting up loophole config"
bg_exec sh -c "mkdir -p ${CONFIG_DIR} ${STORE_DIR} && printf '%s\n' 'default_profile = \"${PROFILE}\"' '' '[profiles.${PROFILE}]' 'local_dir = \"${STORE_DIR}\"' 'mode = \"inprocess\"' 'log_level = \"debug\"' 'sandbox_mode = \"firecracker\"' > ${CONFIG_DIR}/config.toml" >/dev/null

log "Stopping any existing daemon"
bg_exec sh -c "${LOOPHOLE_BIN} stop 2>/dev/null || true" >/dev/null

# --- Import volume ---

if [ "$SKIP_IMPORT" = false ]; then
  log "Importing rootfs into loophole volume '${VOLUME_NAME}'"
  bg_exec sh -c "rm -rf ${STORE_DIR} && mkdir -p ${STORE_DIR} && LOOPHOLE_FIRECRACKER_BIN=${FC_BIN} LOOPHOLE_FIRECRACKER_KERNEL=${KERNEL} ${LOOPHOLE_BIN} device dd if=${WORK_DIR}/ubuntu.ext4 of=${VOLUME_NAME}:"
  echo "  imported"
fi

# --- Start daemon ---

log "Starting loophole daemon (firecracker sandbox mode)"
bg_exec sh -c "$(cat <<'DAEMON_START'
set -e
WORK_DIR=/tmp/loophole-fc
STAGE_DIR=/tmp/loophole-stage

export LOOPHOLE_FIRECRACKER_BIN="${STAGE_DIR}/bin/firecracker"
export LOOPHOLE_FIRECRACKER_KERNEL="${STAGE_DIR}/assets/vmlinux-6.1.155"
export LOOPHOLE_FIRECRACKER_BOOT_TIMEOUT=30s
export LOOPHOLE_FIRECRACKER_MEM_MIB=512
export LOOPHOLE_FIRECRACKER_VCPUS=2

nohup ${STAGE_DIR}/bin/loophole start --foreground > ${WORK_DIR}/daemon.log 2>&1 &
echo "daemon pid: $!"
DAEMON_START
)"

# Wait for daemon socket
log "Waiting for daemon socket"
for i in $(seq 1 30); do
  if bg_exec test -S "${DAEMON_SOCK}" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" = "30" ]; then
    echo "ERROR: daemon socket not found after 30s" >&2
    bg_exec sh -c "tail -20 ${WORK_DIR}/daemon.log" >&2 || true
    exit 1
  fi
  sleep 1
done

# Configure container-control to proxy to the daemon socket.
log "Configuring proxy"
bg_exec sh -c "curl -sS -X POST 'http://localhost:8080/control/set-proxy?target=unix://${DAEMON_SOCK}' -H \"X-Control-Secret: \$CONTROL_SECRET\""

# Verify daemon status
log "Daemon status"
daemon_curl GET /status

# --- Test sandbox exec (triggers VM boot + netns) ---

log "Testing sandbox exec (this boots the VM)"
echo "  Running: echo hello"
RESULT=$(bg_exec curl -sS -X POST --unix-socket "${DAEMON_SOCK}" "http://localhost/sandbox/exec?cmd=echo+hello&volume=${VOLUME_NAME}")
echo "  $RESULT"

echo ""
echo "  Running: cat /etc/os-release"
RESULT=$(bg_exec curl -sS -X POST --unix-socket "${DAEMON_SOCK}" "http://localhost/sandbox/exec?cmd=cat+/etc/os-release&volume=${VOLUME_NAME}")
echo "  $RESULT"

echo ""
echo "  Running: ip addr show"
RESULT=$(bg_exec curl -sS -X POST --unix-socket "${DAEMON_SOCK}" "http://localhost/sandbox/exec?cmd=ip+addr+show&volume=${VOLUME_NAME}")
echo "  $RESULT"

log "Done"
