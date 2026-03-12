#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

BASE_URL="${BASE_URL:-https://cf-demo-4.ramon3525.workers.dev}"
CONTAINER_ID="fc-firecracker-1"
REMOTE_DIR="${REMOTE_DIR:-/tmp/loophole-stage}"
KERNEL_URL="${KERNEL_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155}"
ROOTFS_URL="${ROOTFS_URL:-}"
RAMDISK_URL="${RAMDISK_URL:-}"
SKIP_BUILD=false
SKIP_SYNC=false

BUILDER_IMAGE="loophole-firecracker-builder"
BUILDER_IMAGE_WARM="${BUILDER_IMAGE}:warm"
BUILDER_CONTAINER="loophole-fc-builder"
CARGO_TARGET="/src/loophole/build/linux-amd64/cargo-target"

HOST_GOOS=$(go env GOOS)
HOST_GOARCH=$(go env GOARCH)
HOST_LOOPHOLE_BIN="${REPO_ROOT}/bin/loophole-${HOST_GOOS}-${HOST_GOARCH}"

usage() {
  cat <<EOF
Usage: $0 [options]

Build Firecracker artifacts and stage them into a Cloudflare container.

Options:
  --url URL              Worker base URL (default: ${BASE_URL})
  --remote-dir DIR       Remote staging directory (default: ${REMOTE_DIR})
  --kernel-url URL       Remote kernel URL
  --rootfs-url URL       Optional remote rootfs URL
  --ramdisk-url URL      Optional remote ramdisk URL
  --skip-build           Reuse existing local build artifacts
  --skip-sync            Build locally but do not rsync or fetch remote assets
  -h, --help             Show this help
EOF
}

log() {
  echo "=== $* ===" >&2
}

while (($#)); do
  case "$1" in
    --url)          BASE_URL="$2"; shift 2 ;;
    --remote-dir)   REMOTE_DIR="$2"; shift 2 ;;
    --kernel-url)   KERNEL_URL="$2"; shift 2 ;;
    --rootfs-url)   ROOTFS_URL="$2"; shift 2 ;;
    --ramdisk-url)  RAMDISK_URL="$2"; shift 2 ;;
    --skip-build)   SKIP_BUILD=true; shift ;;
    --skip-sync)    SKIP_SYNC=true; shift ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "Unknown arg: $1" >&2; usage >&2; exit 1 ;;
  esac
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

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
    # Remove stopped container if it exists
    docker rm -f "${BUILDER_CONTAINER}" 2>/dev/null || true
    # Prefer warm image (has cached Go modules + Cargo registry)
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

commit_warm_image() {
  log "Committing warm builder image as ${BUILDER_IMAGE_WARM}"
  docker commit "${BUILDER_CONTAINER}" "${BUILDER_IMAGE_WARM}" >/dev/null
}

build_local_artifacts() {
  log "Building Go binaries (host cross-compile)"
  (cd "${REPO_ROOT}" && make loophole cf-demo-bin cf-demo-guest-agent-bin)

  log "Building Firecracker binary (docker exec)"
  ensure_builder

  # Clear Go build cache to avoid stale builds from VirtioFS mtime caching.
  # The warm image preserves GOCACHE; VirtioFS may serve stale mtimes for
  # host-modified files, causing Go to skip recompilation.
  docker exec "${BUILDER_CONTAINER}" go clean -cache
  docker exec "${BUILDER_CONTAINER}" make libloophole.a
  docker exec "${BUILDER_CONTAINER}" bash -c "
    export CARGO_TARGET_DIR=${CARGO_TARGET}
    export LOOPHOLE_LIB_DIR=/src/loophole/build/linux-amd64
    # Touch build.rs to force Cargo to re-link against the fresh libloophole.a
    touch third_party/firecracker/src/vmm/build.rs
    cargo build -j2 \
      --manifest-path third_party/firecracker/Cargo.toml \
      -p firecracker \
      --features vmm/loophole
  "

  # Copy firecracker binary to staging location
  mkdir -p "${REPO_ROOT}/cf-demo/bin"
  cp "${REPO_ROOT}/build/linux-amd64/cargo-target/debug/firecracker" \
     "${REPO_ROOT}/cf-demo/bin/firecracker"

  # Snapshot container so restarts don't redo dependency downloads
  commit_warm_image
}

upload_via_r2() {
  # Source R2 credentials from .dev.vars
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/cf-demo/.dev.vars"
  set +a

  export AWS_ACCESS_KEY_ID="${R2_ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${R2_SECRET_KEY}"
  export AWS_DEFAULT_REGION="auto"

  log "Ensuring remote staging directories and curl exist"
  bg_exec mkdir -p "${REMOTE_DIR}/bin" "${REMOTE_DIR}/assets" >/dev/null
  bg_exec sh -c "command -v curl >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq curl)" >/dev/null

  local r2_prefix="stage"
  local bins=(
    "${REPO_ROOT}/cf-demo/bin/loophole"
    "${REPO_ROOT}/cf-demo/bin/loophole-guest-agent"
    "${REPO_ROOT}/cf-demo/bin/firecracker"
  )

  for bin in "${bins[@]}"; do
    local name
    name=$(basename "${bin}")
    local r2_key="${r2_prefix}/${name}.gz"

    local gz_tmp
    gz_tmp=$(mktemp)
    gzip -c "${bin}" > "${gz_tmp}"
    local gz_size
    gz_size=$(wc -c < "${gz_tmp}" | tr -d ' ')

    # Check if R2 already has the same file (compare MD5/ETag)
    local local_md5
    local_md5=$(md5 -q "${gz_tmp}" 2>/dev/null || md5sum "${gz_tmp}" | cut -d' ' -f1)
    local remote_etag
    remote_etag=$(aws s3api head-object --bucket "${R2_BUCKET}" --key "${r2_key}" \
      --endpoint-url "${R2_ENDPOINT}" --query ETag --output text 2>/dev/null | tr -d '"' || echo "")

    if [[ "${local_md5}" == "${remote_etag}" ]]; then
      log "Skipping upload for ${name} (unchanged)"
      rm -f "${gz_tmp}"
    else
      log "Uploading ${name} to R2 (${r2_key}, ${gz_size} bytes)"
      aws s3 cp "${gz_tmp}" "s3://${R2_BUCKET}/${r2_key}" \
        --endpoint-url "${R2_ENDPOINT}" \
        --content-type "application/gzip"
      rm -f "${gz_tmp}"
    fi

    log "Generating presigned URL for ${name}"
    local presigned_url
    presigned_url=$(aws s3 presign "s3://${R2_BUCKET}/${r2_key}" \
      --endpoint-url "${R2_ENDPOINT}" \
      --expires-in 300)

    log "Pulling ${name} from R2 in container"
    bg_exec sh -c "curl -fSL '${presigned_url}' | gunzip > ${REMOTE_DIR}/bin/${name}.tmp && chmod +x ${REMOTE_DIR}/bin/${name}.tmp && mv ${REMOTE_DIR}/bin/${name}.tmp ${REMOTE_DIR}/bin/${name}"
    log "${name}: ok"
  done
}

remote_fetch_asset() {
  local url="$1"
  local dst="$2"
  [[ -z "${url}" ]] && return 0

  bg_exec sh -c "
set -e
if ! command -v curl >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update >/dev/null
  apt-get install -y --no-install-recommends curl ca-certificates >/dev/null
fi
mkdir -p \"\$(dirname '${dst}')\"
curl -fL --continue-at - -o '${dst}' '${url}'
" >/dev/null
}

print_ready_summary() {
  cat <<EOF
Ready in container ${CONTAINER_ID}:
  ${REMOTE_DIR}/bin/loophole
  ${REMOTE_DIR}/bin/loophole-guest-agent
  ${REMOTE_DIR}/bin/firecracker
EOF
  [[ -n "${KERNEL_URL}" ]] && echo "  ${REMOTE_DIR}/assets/$(basename "${KERNEL_URL}")"
  [[ -n "${ROOTFS_URL}" ]] && echo "  ${REMOTE_DIR}/assets/$(basename "${ROOTFS_URL}")"
  [[ -n "${RAMDISK_URL}" ]] && echo "  ${REMOTE_DIR}/assets/$(basename "${RAMDISK_URL}")"
}

main() {
  require_cmd curl
  require_cmd docker
  require_cmd aws
  require_cmd go

  if [[ "${SKIP_BUILD}" == false ]]; then
    build_local_artifacts
  fi

  if [[ "${SKIP_SYNC}" == true ]]; then
    print_ready_summary
    return 0
  fi

  upload_via_r2

  if [[ -n "${KERNEL_URL}" ]]; then
    log "Fetching kernel remotely"
    remote_fetch_asset "${KERNEL_URL}" "${REMOTE_DIR}/assets/$(basename "${KERNEL_URL}")"
  fi
  if [[ -n "${ROOTFS_URL}" ]]; then
    log "Fetching rootfs remotely"
    remote_fetch_asset "${ROOTFS_URL}" "${REMOTE_DIR}/assets/$(basename "${ROOTFS_URL}")"
  fi
  if [[ -n "${RAMDISK_URL}" ]]; then
    log "Fetching ramdisk remotely"
    remote_fetch_asset "${RAMDISK_URL}" "${REMOTE_DIR}/assets/$(basename "${RAMDISK_URL}")"
  fi

  print_ready_summary
}

main "$@"
