#!/bin/bash
# gce-boot.sh — Prepare rootfs and boot Firecracker on a GCE VM.
#
# Run on the GCE instance after gce-deploy.sh has copied artifacts.
# Must be run as root (for KVM, TAP networking, netns).
#
# Usage:
#   sudo /tmp/loophole-deploy/scripts/gce-boot.sh              # full run
#   sudo /tmp/loophole-deploy/scripts/gce-boot.sh --skip-rootfs # reuse existing rootfs
#   sudo /tmp/loophole-deploy/scripts/gce-boot.sh --shell       # drop to serial console
#   sudo /tmp/loophole-deploy/scripts/gce-boot.sh --daemon      # use daemon sandbox mode

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

WORK_DIR="${WORK_DIR:-/tmp/loophole-fc}"
STORE_DIR="${WORK_DIR}/store"
CONFIG_DIR="${WORK_DIR}/config"

FC_BIN="${DEPLOY_DIR}/bin/firecracker"
LOOPHOLE_BIN="${DEPLOY_DIR}/bin/loophole"
GUEST_AGENT_BIN="${DEPLOY_DIR}/bin/loophole-guest-agent"
KERNEL="${DEPLOY_DIR}/assets/vmlinux-6.1.155"

ROOTFS_URL="${ROOTFS_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/ubuntu-24.04.squashfs}"
VOLUME_NAME="rootfs"
PROFILE="gce"

TAP_DEV="tap0"
TAP_IP="172.16.0.1"
GUEST_IP="172.16.0.2"
GUEST_MAC="06:00:c0:a8:00:02"
MASK_CIDR="24"

SKIP_ROOTFS=false
SHELL_MODE=false
DAEMON_MODE=false

log() { echo "=== $* ===" >&2; }

usage() {
  cat <<EOF
Usage: $0 [options]

Boot Firecracker VM on GCE with loophole-backed rootfs.
Requires artifacts from gce-deploy.sh.

Options:
  --skip-rootfs    Reuse existing rootfs (skip download/conversion)
  --shell          Boot VM with serial console (no daemon, no guest agent)
  --daemon         Use loophole daemon with sandbox_mode=firecracker
  --work-dir DIR   Working directory (default: ${WORK_DIR})
  -h, --help       Show this help
EOF
}

while (($#)); do
  case "$1" in
    --skip-rootfs) SKIP_ROOTFS=true; shift ;;
    --shell)       SHELL_MODE=true; shift ;;
    --daemon)      DAEMON_MODE=true; shift ;;
    --work-dir)    WORK_DIR="$2"; shift 2 ;;
    -h|--help)     usage; exit 0 ;;
    *)             echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# --- Preflight ---

log "Preflight checks"
for bin in "${FC_BIN}" "${LOOPHOLE_BIN}" "${GUEST_AGENT_BIN}"; do
  if [ ! -x "${bin}" ]; then
    echo "ERROR: missing executable: ${bin}" >&2
    echo "Run gce-deploy.sh first." >&2
    exit 1
  fi
done
echo "  binaries: ok"

if [ ! -f "${KERNEL}" ]; then
  echo "ERROR: missing kernel: ${KERNEL}" >&2
  exit 1
fi
echo "  kernel: ok"

if [ ! -e /dev/kvm ]; then
  echo "ERROR: /dev/kvm not found — nested virtualization not enabled" >&2
  exit 1
fi
echo "  /dev/kvm: ok"

# --- Install deps if needed ---

install_if_missing() {
  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      log "Installing dependencies"
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq squashfs-tools e2fsprogs iproute2 iptables curl
      return
    fi
  done
}
install_if_missing unsquashfs mkfs.ext4 ip iptables

# --- Prepare rootfs ---

mkdir -p "${WORK_DIR}"

if [ "${SKIP_ROOTFS}" = false ]; then
  SQUASHFS="${WORK_DIR}/ubuntu.squashfs"
  EXT4="${WORK_DIR}/ubuntu.ext4"

  if [ ! -f "${SQUASHFS}" ]; then
    log "Downloading rootfs squashfs"
    curl -fL --progress-bar -o "${SQUASHFS}" "${ROOTFS_URL}"
  else
    echo "  squashfs: already downloaded"
  fi

  log "Converting squashfs to ext4"
  SQUASH_TMP="${WORK_DIR}/squashfs-root"
  rm -rf "${SQUASH_TMP}"
  unsquashfs -d "${SQUASH_TMP}" "${SQUASHFS}"

  # Inject guest agent
  cp "${GUEST_AGENT_BIN}" "${SQUASH_TMP}/sbin/loophole-guest-agent"
  chmod 755 "${SQUASH_TMP}/sbin/loophole-guest-agent"

  # Static resolv.conf
  rm -f "${SQUASH_TMP}/etc/resolv.conf"
  echo "nameserver 8.8.8.8" > "${SQUASH_TMP}/etc/resolv.conf"

  # Create ext4 image
  rm -f "${EXT4}"
  truncate -s 1G "${EXT4}"
  mkfs.ext4 -d "${SQUASH_TMP}" -F "${EXT4}"
  rm -rf "${SQUASH_TMP}"
  echo "  -> ${EXT4}"
fi

EXT4="${WORK_DIR}/ubuntu.ext4"

# --- Set up loophole config and import volume ---

log "Setting up loophole config"
mkdir -p "${CONFIG_DIR}" "${STORE_DIR}"

cat > "${CONFIG_DIR}/config.toml" <<TOML
default_profile = "${PROFILE}"

[profiles.${PROFILE}]
local_dir = "${STORE_DIR}"
mode = "inprocess"
log_level = "debug"
TOML

export LOOPHOLE_HOME="${CONFIG_DIR}"

# Stop any existing daemon
"${LOOPHOLE_BIN}" stop >/dev/null 2>&1 || true

log "Importing rootfs into loophole volume '${VOLUME_NAME}'"
rm -rf "${STORE_DIR}"
mkdir -p "${STORE_DIR}"
"${LOOPHOLE_BIN}" device dd if="${EXT4}" of="${VOLUME_NAME}:"
echo "  imported"

# --- Networking ---

log "Setting up host networking"
HOST_IFACE=$(ip -j route show default | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['dev'])")
echo "  host iface: ${HOST_IFACE}"

if ! ip link show "${TAP_DEV}" &>/dev/null; then
  ip tuntap add dev "${TAP_DEV}" mode tap
fi
ip addr replace "${TAP_IP}/${MASK_CIDR}" dev "${TAP_DEV}"
ip link set dev "${TAP_DEV}" up
echo "  tap: ${TAP_DEV} (${TAP_IP}/${MASK_CIDR})"

sysctl -w net.ipv4.ip_forward=1 >/dev/null

if ! iptables -t nat -C POSTROUTING -o "${HOST_IFACE}" -j MASQUERADE 2>/dev/null; then
  iptables -t nat -A POSTROUTING -o "${HOST_IFACE}" -j MASQUERADE
fi
if ! iptables -C FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null; then
  iptables -A FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
fi
if ! iptables -C FORWARD -i "${TAP_DEV}" -o "${HOST_IFACE}" -j ACCEPT 2>/dev/null; then
  iptables -A FORWARD -i "${TAP_DEV}" -o "${HOST_IFACE}" -j ACCEPT
fi
echo "  NAT masquerade via ${HOST_IFACE}"

# --- Daemon mode ---

if [ "${DAEMON_MODE}" = true ]; then
  log "Starting in daemon sandbox mode"

  # Update config with firecracker sandbox mode
  cat > "${CONFIG_DIR}/config.toml" <<TOML
default_profile = "${PROFILE}"

[profiles.${PROFILE}]
local_dir = "${STORE_DIR}"
mode = "inprocess"
log_level = "debug"
sandbox_mode = "firecracker"
TOML

  export LOOPHOLE_FIRECRACKER_BIN="${FC_BIN}"
  export LOOPHOLE_FIRECRACKER_KERNEL="${KERNEL}"
  export LOOPHOLE_FIRECRACKER_BOOT_TIMEOUT=30s
  export LOOPHOLE_FIRECRACKER_MEM_MIB=512
  export LOOPHOLE_FIRECRACKER_VCPUS=2

  log "Starting loophole daemon"
  "${LOOPHOLE_BIN}" start --foreground &
  DAEMON_PID=$!

  DAEMON_SOCK="${CONFIG_DIR}/${PROFILE}.sock"
  log "Waiting for daemon socket"
  for i in $(seq 1 30); do
    [ -S "${DAEMON_SOCK}" ] && break
    if [ "$i" = "30" ]; then
      echo "ERROR: daemon socket not found after 30s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "  daemon PID: ${DAEMON_PID}"

  log "Testing sandbox exec (this boots the VM)"
  echo "  Running: echo hello"
  curl -sS -X POST --unix-socket "${DAEMON_SOCK}" \
    "http://localhost/sandbox/exec?cmd=echo+hello&volume=${VOLUME_NAME}"
  echo ""
  echo "  Running: cat /etc/os-release"
  curl -sS -X POST --unix-socket "${DAEMON_SOCK}" \
    "http://localhost/sandbox/exec?cmd=cat+/etc/os-release&volume=${VOLUME_NAME}"
  echo ""
  echo "  Running: ip addr show"
  curl -sS -X POST --unix-socket "${DAEMON_SOCK}" \
    "http://localhost/sandbox/exec?cmd=ip+addr+show&volume=${VOLUME_NAME}"
  echo ""

  log "Daemon running (PID ${DAEMON_PID}). Exec via:"
  echo "  curl -sS -X POST --unix-socket ${DAEMON_SOCK} 'http://localhost/sandbox/exec?cmd=COMMAND&volume=${VOLUME_NAME}'"
  echo ""
  echo "Press Ctrl+C to stop."
  wait "${DAEMON_PID}"
  exit $?
fi

# --- Direct Firecracker boot ---

FC_SOCK="${WORK_DIR}/fc.sock"
FC_CONFIG="${WORK_DIR}/fc-config.json"

BOOT_ARGS="console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw net.ifnames=0 ip=${GUEST_IP}::${TAP_IP}:255.255.255.0::eth0:off"
if [ "${SHELL_MODE}" = false ]; then
  BOOT_ARGS="${BOOT_ARGS} init=/sbin/loophole-guest-agent"
fi

cat > "${FC_CONFIG}" <<EOF
{
  "boot-source": {
    "kernel_image_path": "${KERNEL}",
    "boot_args": "${BOOT_ARGS}"
  },
  "drives": [
    {
      "drive_id": "rootfs",
      "path_on_host": "${VOLUME_NAME}",
      "is_root_device": true,
      "is_read_only": false,
      "io_engine": "Loophole"
    }
  ],
  "network-interfaces": [
    {
      "iface_id": "eth0",
      "host_dev_name": "${TAP_DEV}",
      "guest_mac": "${GUEST_MAC}"
    }
  ],
  "machine-config": {
    "vcpu_count": 2,
    "mem_size_mib": 512
  }
}
EOF

log "Booting Firecracker VM"
echo "  kernel:  ${KERNEL}"
echo "  rootfs:  loophole volume '${VOLUME_NAME}'"
echo "  network: ${GUEST_IP} via ${TAP_DEV} (NAT to ${HOST_IFACE})"
echo "  config:  ${FC_CONFIG}"
if [ "${SHELL_MODE}" = true ]; then
  echo "  mode:    serial console (login: root)"
else
  echo "  mode:    guest agent (vsock exec)"
fi

export LOOPHOLE_INIT=1
export LOOPHOLE_CONFIG_DIR="${CONFIG_DIR}"
export LOOPHOLE_PROFILE="${PROFILE}"
rm -f "${FC_SOCK}"
exec "${FC_BIN}" --api-sock "${FC_SOCK}" --no-seccomp --config-file "${FC_CONFIG}"
