#!/bin/bash
set -e

usage() {
  cat <<EOF
Usage: $0 <command> -p <profile> -v <volume> [options]

Commands:
  populate   Build rootfs, extract into volume, validate
  run        Boot the VM via QEMU + NBD
  validate   Mount and check expected files/dirs

Options:
  -p PROFILE   Profile name from ~/.loophole/config.toml (required)
  -v VOLUME    Volume name (required)
  -c CPUS      Number of CPUs for QEMU (default: 4)
  -m MEMORY    Memory in MB for QEMU (default: 4096)
  -s SIZE      Volume size for populate (default: 5GB)
EOF
  exit 1
}

COMMAND="${1:-}"
[ -z "$COMMAND" ] && usage
shift

PROFILE=""
VOLUME=""
CPUS=4
MEMORY=4096
SIZE="5GB"

while getopts "p:v:c:m:s:" opt; do
  case "$opt" in
    p) PROFILE="$OPTARG" ;;
    v) VOLUME="$OPTARG" ;;
    c) CPUS="$OPTARG" ;;
    m) MEMORY="$OPTARG" ;;
    s) SIZE="$OPTARG" ;;
    *) usage ;;
  esac
done

[ -z "$PROFILE" ] && { echo "error: -p <profile> is required"; exit 1; }
[ -z "$VOLUME" ] && { echo "error: -v <volume> is required"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINDIR="${SCRIPT_DIR}/bin"
LOOPHOLE="${BINDIR}/loophole-$(go env GOOS)-$(go env GOARCH)"
LP="$LOOPHOLE -p $PROFILE"

cmd_populate() {
  set -x

  mkdir -p "$BINDIR"
  make -C "$SCRIPT_DIR" loophole

  # Build the VM rootfs image and create a temporary container.
  docker build -t loophole-vm -f Dockerfile.vm "$SCRIPT_DIR"
  CID=$(docker create loophole-vm)
  trap 'docker rm "$CID" >/dev/null' EXIT

  # Copy out kernel and initrd into bin/.
  docker cp "$CID:/boot/vmlinuz-6.1.0-43-arm64" "$BINDIR/vmlinuz"
  docker cp "$CID:/boot/initrd.img-6.1.0-43-arm64" "$BINDIR/initrd.img"

  # Start daemon with profile (in-process lwext4, no FUSE/root needed).
  LOOPHOLE_MODE=inprocess $LP start -d

  $LP delete -y "$VOLUME" 2>/dev/null || true
  $LP create --size "$SIZE" "$VOLUME"
  $LP mount "$VOLUME"

  # Extract the rootfs directly into the volume via lwext4.
  docker export "$CID" | $LP file tar -xv -C "$VOLUME:/"

  $LP unmount "$VOLUME"

  # Validate after populating.
  cmd_validate
}

cmd_run() {
  KERNEL="${BINDIR}/vmlinuz"
  INITRD="${BINDIR}/initrd.img"

  [ -f "$KERNEL" ] || { echo "error: kernel not found at $KERNEL (run populate first)"; exit 1; }
  [ -f "$INITRD" ] || { echo "error: initrd not found at $INITRD (run populate first)"; exit 1; }

  # Ensure daemon is running.
  $LP start -d

  # Start NBD server inside the daemon; prints socket path.
  NBD_SOCK=$($LP serve-nbd)

  set -x
  qemu-system-aarch64 \
      -machine virt -accel hvf -cpu host \
      -m "$MEMORY" -smp "$CPUS" \
      -kernel "$KERNEL" -initrd "$INITRD" \
      -append "console=ttyAMA0 root=/dev/vda rw" \
      -drive "file=nbd+unix:///${VOLUME}?socket=${NBD_SOCK},format=raw,if=virtio" \
      -netdev user,id=net0 -device virtio-net-pci,netdev=net0 \
      -nographic
}

cmd_validate() {
  echo "=== Validating volume $VOLUME ==="
  $LP mount "$VOLUME"

  FAIL=0
  check_dir() {
    if $LP file ls "$VOLUME:$1" >/dev/null 2>&1; then
      echo "  OK  $1"
    else
      echo "  MISSING  $1"
      FAIL=1
    fi
  }

  check_file() {
    if $LP file cat "$VOLUME:$1" >/dev/null 2>&1; then
      echo "  OK  $1"
    else
      echo "  MISSING  $1"
      FAIL=1
    fi
  }

  check_dir /bin
  check_dir /etc
  check_dir /lib
  check_dir /usr
  check_dir /var
  check_dir /sbin
  check_dir /boot
  check_file /bin/sh
  check_file /etc/passwd
  check_file /etc/os-release
  check_file /sbin/init

  $LP unmount "$VOLUME"

  if [ "$FAIL" -ne 0 ]; then
    echo "FAIL: volume $VOLUME is missing expected files"
    exit 1
  fi
  echo "OK: volume $VOLUME validated"
}

case "$COMMAND" in
  populate) cmd_populate ;;
  run)      cmd_run ;;
  validate) cmd_validate ;;
  *)        echo "error: unknown command '$COMMAND'"; usage ;;
esac
