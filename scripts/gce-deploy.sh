#!/bin/bash
# gce-deploy.sh — Build Firecracker artifacts locally in Docker and deploy to GCE.
#
# Builds:
#   - loophole CLI (Go cross-compile, no CGo)
#   - loophole-guest-agent (Go cross-compile)
#   - libloophole.a (Go c-archive via Docker, linux/amd64)
#   - firecracker binary (Rust via Docker, linux/amd64, with loophole feature)
#
# Then copies binaries + kernel + boot script to GCE via scp.
#
# Usage:
#   ./scripts/gce-deploy.sh                  # full build + deploy
#   ./scripts/gce-deploy.sh --skip-build     # deploy existing artifacts only
#   ./scripts/gce-deploy.sh --skip-deploy    # build only, don't copy to GCE

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

GCE_INSTANCE="${GCE_INSTANCE:-loophole-fc-1}"
GCE_ZONE="${GCE_ZONE:-us-west1-b}"
GCE_USER="${GCE_USER:-$(whoami)}"
REMOTE_DIR="${REMOTE_DIR:-/tmp/loophole-deploy}"
KERNEL_URL="${KERNEL_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155}"
ROOTFS_URL="${ROOTFS_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/ubuntu-24.04.squashfs}"

STAGE_DIR="${REPO_ROOT}/build/gce-stage"

BUILDER_IMAGE="loophole-firecracker-builder"
BUILDER_IMAGE_WARM="${BUILDER_IMAGE}:warm"
BUILDER_CONTAINER="loophole-fc-builder"
CARGO_TARGET="/src/loophole/build/linux-amd64/cargo-target"

SKIP_BUILD=false
SKIP_DEPLOY=false

log() { echo "=== $* ===" >&2; }

usage() {
  cat <<EOF
Usage: $0 [options]

Build Firecracker artifacts locally in Docker and deploy to GCE.

Options:
  --instance NAME    GCE instance name (default: ${GCE_INSTANCE})
  --zone ZONE        GCE zone (default: ${GCE_ZONE})
  --skip-build       Reuse existing build artifacts
  --skip-deploy      Build only, don't copy to GCE
  -h, --help         Show this help
EOF
}

while (($#)); do
  case "$1" in
    --instance)    GCE_INSTANCE="$2"; shift 2 ;;
    --zone)        GCE_ZONE="$2"; shift 2 ;;
    --skip-build)  SKIP_BUILD=true; shift ;;
    --skip-deploy) SKIP_DEPLOY=true; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

ensure_builder() {
  # Build base image if missing
  if ! docker image inspect "${BUILDER_IMAGE}" >/dev/null 2>&1; then
    log "Building ${BUILDER_IMAGE} Docker image"
    docker build --platform linux/amd64 -t "${BUILDER_IMAGE}" \
      -f "${REPO_ROOT}/cf-demo/firecracker-builder.Dockerfile" \
      "${REPO_ROOT}"
  fi

  # Start container if not running
  if ! docker ps -q -f "name=^${BUILDER_CONTAINER}$" | grep -q .; then
    docker rm -f "${BUILDER_CONTAINER}" 2>/dev/null || true
    local image="${BUILDER_IMAGE}"
    if docker image inspect "${BUILDER_IMAGE_WARM}" >/dev/null 2>&1; then
      image="${BUILDER_IMAGE_WARM}"
    fi
    log "Starting ${BUILDER_CONTAINER} container (from ${image})"
    docker run -d \
      --name "${BUILDER_CONTAINER}" \
      --platform linux/amd64 \
      -v "${REPO_ROOT}:/src/loophole" \
      -w /src/loophole \
      "${image}" \
      sleep infinity
  fi
}

build_artifacts() {
  mkdir -p "${STAGE_DIR}/bin" "${STAGE_DIR}/assets"

  log "Building Go binaries (host cross-compile for linux/amd64)"
  (cd "${REPO_ROOT}" && make cf-demo-bin cf-demo-guest-agent-bin)
  cp "${REPO_ROOT}/cf-demo/bin/loophole" "${STAGE_DIR}/bin/loophole"
  cp "${REPO_ROOT}/cf-demo/bin/loophole-guest-agent" "${STAGE_DIR}/bin/loophole-guest-agent"
  echo "  loophole: $(wc -c < "${STAGE_DIR}/bin/loophole" | tr -d ' ') bytes"
  echo "  guest-agent: $(wc -c < "${STAGE_DIR}/bin/loophole-guest-agent" | tr -d ' ') bytes"

  log "Building Firecracker binary (Docker, linux/amd64)"
  ensure_builder

  # Clear Go build cache to avoid stale builds from VirtioFS mtime caching.
  docker exec "${BUILDER_CONTAINER}" go clean -cache
  docker exec "${BUILDER_CONTAINER}" make libloophole.a
  docker exec "${BUILDER_CONTAINER}" bash -c "
    export CARGO_TARGET_DIR=${CARGO_TARGET}
    export LOOPHOLE_LIB_DIR=/src/loophole/build/linux-amd64
    touch third_party/firecracker/src/vmm/build.rs
    cargo build -j2 \
      --manifest-path third_party/firecracker/Cargo.toml \
      -p firecracker \
      --features vmm/loophole
  "

  cp "${REPO_ROOT}/build/linux-amd64/cargo-target/debug/firecracker" \
     "${STAGE_DIR}/bin/firecracker"
  echo "  firecracker: $(wc -c < "${STAGE_DIR}/bin/firecracker" | tr -d ' ') bytes"

  # Snapshot container so restarts don't redo dependency downloads
  log "Committing warm builder image"
  docker commit "${BUILDER_CONTAINER}" "${BUILDER_IMAGE_WARM}" >/dev/null

  log "Downloading kernel"
  local kernel_name
  kernel_name=$(basename "${KERNEL_URL}")
  if [ ! -f "${STAGE_DIR}/assets/${kernel_name}" ]; then
    curl -fL -o "${STAGE_DIR}/assets/${kernel_name}" "${KERNEL_URL}"
  else
    echo "  ${kernel_name}: already downloaded"
  fi
}

deploy_to_gce() {
  local kernel_name
  kernel_name=$(basename "${KERNEL_URL}")

  # Create tarball — binaries share a lot of Go runtime text so they
  # compress much better together than individually.
  local tarball="${STAGE_DIR}/loophole-deploy.tar.gz"
  log "Creating deployment tarball"
  tar -czf "${tarball}" -C "${STAGE_DIR}" \
    bin/loophole \
    bin/loophole-guest-agent \
    bin/firecracker \
    "assets/${kernel_name}" \
    -C "${REPO_ROOT}" \
    scripts/gce-boot.sh
  echo "  $(du -h "${tarball}" | cut -f1) compressed"

  log "Uploading to ${GCE_INSTANCE}:${REMOTE_DIR}"
  gcloud compute ssh "${GCE_INSTANCE}" --zone "${GCE_ZONE}" \
    --command "mkdir -p ${REMOTE_DIR}"
  gcloud compute scp --zone "${GCE_ZONE}" \
    "${tarball}" "${GCE_INSTANCE}:${REMOTE_DIR}/loophole-deploy.tar.gz"

  log "Extracting on ${GCE_INSTANCE}"
  gcloud compute ssh "${GCE_INSTANCE}" --zone "${GCE_ZONE}" \
    --command "cd ${REMOTE_DIR} && tar -xzf loophole-deploy.tar.gz && chmod +x bin/* scripts/*"

  log "Deployed to ${GCE_INSTANCE}:${REMOTE_DIR}"
  echo ""
  echo "Files on ${GCE_INSTANCE}:"
  gcloud compute ssh "${GCE_INSTANCE}" --zone "${GCE_ZONE}" \
    --command "ls -lh ${REMOTE_DIR}/bin/ ${REMOTE_DIR}/assets/ ${REMOTE_DIR}/scripts/"
  echo ""
  echo "Next steps:"
  echo "  gcloud compute ssh ${GCE_INSTANCE} --zone ${GCE_ZONE}"
  echo "  sudo ${REMOTE_DIR}/scripts/gce-boot.sh"
  echo ""
  echo "Or from here:"
  echo "  gcloud compute ssh ${GCE_INSTANCE} --zone ${GCE_ZONE} -- sudo ${REMOTE_DIR}/scripts/gce-boot.sh"
}

main() {
  require_cmd docker
  require_cmd gcloud
  require_cmd go

  if [[ "${SKIP_BUILD}" == false ]]; then
    build_artifacts
  fi

  if [[ "${SKIP_DEPLOY}" == false ]]; then
    deploy_to_gce
  fi
}

main "$@"
