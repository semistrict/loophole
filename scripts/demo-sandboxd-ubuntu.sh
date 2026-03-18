#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

UBUNTU_IMAGE="${UBUNTU_IMAGE:-ubuntu:24.04}"
DOCKER_ARCH="${DOCKER_ARCH:-$(docker version --format '{{.Server.Arch}}' 2>/dev/null || uname -m)}"
case "$DOCKER_ARCH" in
  arm64|aarch64)
    DEFAULT_PLATFORM="linux/arm64"
    DEFAULT_TAR="ubuntu-2404-arm64.tar"
    ;;
  amd64|x86_64)
    DEFAULT_PLATFORM="linux/amd64"
    DEFAULT_TAR="ubuntu-2404-amd64.tar"
    ;;
  *)
    echo "unsupported Docker architecture: $DOCKER_ARCH" >&2
    exit 1
    ;;
esac

PLATFORM="${PLATFORM:-$DEFAULT_PLATFORM}"
ROOTFS_TAR="${ROOTFS_TAR:-$DEFAULT_TAR}"
DEMO_SUFFIX="${DEMO_SUFFIX:-$(date +%s)}"
ROOTFS_VOLUME="${ROOTFS_VOLUME:-ubuntu-rootfs-$DEMO_SUFFIX}"
ZYGOTE_NAME="${ZYGOTE_NAME:-ubuntu-2404-$DEMO_SUFFIX}"
SANDBOX_NAME="${SANDBOX_NAME:-demo-$DEMO_SUFFIX}"
DEMO_ATTACH="${DEMO_ATTACH:-1}"
DEMO_RUNSC_DEBUG="${DEMO_RUNSC_DEBUG:-1}"

if [[ ! -f "$ROOTFS_TAR" ]]; then
  echo "exporting $UBUNTU_IMAGE to $ROOTFS_TAR"
  cid="$(docker create --platform "$PLATFORM" "$UBUNTU_IMAGE" /bin/true)"
  trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
  docker export "$cid" > "$ROOTFS_TAR"
  docker rm "$cid" >/dev/null
  trap - EXIT
fi

docker compose up -d s3
docker compose build go

docker compose run --rm --service-ports \
  -e DEMO_ROOTFS_TAR="$ROOTFS_TAR" \
  -e DEMO_ROOTFS_VOLUME="$ROOTFS_VOLUME" \
  -e DEMO_ZYGOTE_NAME="$ZYGOTE_NAME" \
  -e DEMO_SANDBOX_NAME="$SANDBOX_NAME" \
  -e DEMO_ATTACH="$DEMO_ATTACH" \
  -e DEMO_RUNSC_DEBUG="$DEMO_RUNSC_DEBUG" \
  go bash -lc '
set -euo pipefail

export PATH="/usr/local/go/bin:$PATH"
DEMO_DIR="/tmp/demo-${DEMO_SANDBOX_NAME}"
mkdir -p "${DEMO_DIR}"
export LOOPHOLE_HOME="${DEMO_DIR}/home"
export LOOPHOLE_SANDBOXD_RUNSC_DEBUG="${DEMO_RUNSC_DEBUG}"
SANDBOXD_SOCKET="$LOOPHOLE_HOME/sandboxd.sock"
mkdir -p "$LOOPHOLE_HOME"
cp /root/.loophole/config.toml "$LOOPHOLE_HOME/config.toml"
make loophole loophole-sandboxd runsc

GOARCH="$(go env GOARCH)"
LOOPHOLE_BIN="/app/bin/loophole-linux-${GOARCH}"
SANDBOXD_BIN="/app/bin/loophole-sandboxd-linux-${GOARCH}"

ROOTFS_TAR_IN_CONTAINER="/app/${DEMO_ROOTFS_TAR}"
ROOTFS_MOUNT="${DEMO_DIR}/rootfs"
mkdir -p "${ROOTFS_MOUNT}"

echo "creating and mounting ${DEMO_ROOTFS_VOLUME}"
"${LOOPHOLE_BIN}" -p default create "${DEMO_ROOTFS_VOLUME}" -m "${ROOTFS_MOUNT}" >/tmp/demo-rootfs.log 2>&1 &
OWNER_PID=$!

for _ in $(seq 1 100); do
  if mountpoint -q "${ROOTFS_MOUNT}"; then
    break
  fi
  sleep 0.1
done

if ! mountpoint -q "${ROOTFS_MOUNT}"; then
  echo "rootfs mount did not become ready at ${ROOTFS_MOUNT}" >&2
  tail -n 100 /tmp/demo-rootfs.log >&2 || true
  exit 1
fi

echo "extracting ${ROOTFS_TAR_IN_CONTAINER}"
tar xf "${ROOTFS_TAR_IN_CONTAINER}" -C "${ROOTFS_MOUNT}"

echo "checkpointing ${DEMO_ROOTFS_VOLUME}"
DEMO_ROOTFS_CHECKPOINT="$("${LOOPHOLE_BIN}" -p default checkpoint "${ROOTFS_MOUNT}" | awk '/^checkpoint / { print $2 }')"
if [[ -z "${DEMO_ROOTFS_CHECKPOINT}" ]]; then
  echo "failed to create checkpoint for ${DEMO_ROOTFS_VOLUME}" >&2
  exit 1
fi
echo "shutting down owner for ${DEMO_ROOTFS_VOLUME}"
if ! "${LOOPHOLE_BIN}" -p default shutdown "${DEMO_ROOTFS_VOLUME}"; then
  echo "owner already stopped after checkpoint; continuing"
fi
wait "${OWNER_PID}" || true

echo "starting sandboxd"
"${SANDBOXD_BIN}" -p default >/tmp/sandboxd-launch.log 2>&1 &
SANDBOXD_PID=$!

for _ in $(seq 1 200); do
  if [[ -S "${SANDBOXD_SOCKET}" ]]; then
    if curl --silent --fail --unix-socket "${SANDBOXD_SOCKET}" http://localhost/v1/zygotes >/dev/null 2>&1; then
      break
    fi
  fi
  sleep 0.1
done

echo "registering zygote ${DEMO_ZYGOTE_NAME}"
curl --silent --show-error --unix-socket "${SANDBOXD_SOCKET}" \
  -H "content-type: application/json" \
  -d "{\"name\":\"${DEMO_ZYGOTE_NAME}\",\"volume\":\"${DEMO_ROOTFS_VOLUME}\",\"checkpoint\":\"${DEMO_ROOTFS_CHECKPOINT}\"}" \
  http://localhost/v1/zygotes >/tmp/demo-zygote.json

echo "creating sandbox ${DEMO_SANDBOX_NAME}"
curl --silent --show-error --unix-socket "${SANDBOXD_SOCKET}" \
  -H "content-type: application/json" \
  -d "{\"name\":\"${DEMO_SANDBOX_NAME}\",\"source\":{\"kind\":\"zygote\",\"zygote\":\"${DEMO_ZYGOTE_NAME}\"}}" \
  http://localhost/v1/sandboxes >/tmp/demo-sandbox.json

SBX_ID="$(sed -n '\''s/.*"id":"\([^"]*\)".*/\1/p'\'' /tmp/demo-sandbox.json | head -n1)"
if [[ -z "${SBX_ID}" ]]; then
  echo "failed to parse sandbox id" >&2
  cat /tmp/demo-sandbox.json >&2
  exit 1
fi

sandbox_api() {
  curl --unix-socket "${SANDBOXD_SOCKET}" "$@"
}

cleanup_demo() {
  set +e
  if [[ -n "${SBX_ID:-}" ]]; then
    sandbox_api -X DELETE "http://localhost/v1/sandboxes/${SBX_ID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${DEMO_ROOTFS_VOLUME:-}" ]]; then
    "${LOOPHOLE_BIN}" -p default shutdown "${DEMO_ROOTFS_VOLUME}" >/dev/null 2>&1 || true
  fi
  kill "${SANDBOXD_PID}" >/dev/null 2>&1 || true
}

export DEMO_ROOTFS_VOLUME DEMO_ZYGOTE_NAME DEMO_SANDBOX_NAME SBX_ID SANDBOXD_PID SANDBOXD_SOCKET LOOPHOLE_HOME
export -f sandbox_api cleanup_demo
trap cleanup_demo EXIT

if [[ "${DEMO_ATTACH}" == "1" ]]; then
  echo
  echo "Attaching to sandbox shell. Exit the shell to return to the host container."
  "${SANDBOXD_BIN}" shell --socket-path "${SANDBOXD_SOCKET}" "${SBX_ID}" /bin/bash -i || true
  stty sane 2>/dev/null || true
  printf "\033[?2004l"
  echo
fi

cat <<EOF

Demo is ready.

Sandbox id: ${SBX_ID}
Volume:     ${DEMO_ROOTFS_VOLUME}
Zygote:     ${DEMO_ZYGOTE_NAME}

Try:
  sandbox_api -H "content-type: application/json" \\
    -d '"'"'{"argv":["/bin/cat","/etc/os-release"]}'"'"' \\
    http://localhost/v1/sandboxes/${SBX_ID}/processes

  sandbox_api -H "content-type: application/json" \\
    -d '"'"'{"argv":["/bin/bash","-lc","id && uname -a"]}'"'"' \\
    http://localhost/v1/sandboxes/${SBX_ID}/processes

  printf "hello from fs api\n" | \\
    sandbox_api -X PUT --data-binary @- \\
    "http://localhost/v1/sandboxes/${SBX_ID}/fs/write?path=/tmp/hello.txt"

  sandbox_api "http://localhost/v1/sandboxes/${SBX_ID}/fs/read?path=/tmp/hello.txt"

Logs:
  tail -f ${LOOPHOLE_HOME}/sandboxd.log
  tail -f /tmp/demo-rootfs.log
  ls -1 ${LOOPHOLE_HOME}/sandboxd/sandboxes/${SBX_ID}/runsc-debug
  tail -f ${LOOPHOLE_HOME}/sandboxd/sandboxes/${SBX_ID}/runsc-debug/*.txt

Cleanup:
  cleanup_demo

EOF

exec bash -i
'
