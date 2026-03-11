#!/bin/bash
# loophole-clone.sh — Snapshot a running Firecracker VM and restore the clone.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOOPHOLE_SRC=$(cd "${SCRIPT_DIR}/.." && pwd)

WORK_DIR="${WORK_DIR:-/tmp/loophole-demo}"
CLONE_ID="${CLONE_ID:-1}"
FC_BIN="${FC_BIN:-${WORK_DIR}/firecracker/build/cargo_target/debug/firecracker}"
FC_SOCK="${FC_SOCK:-${WORK_DIR}/fc.sock}"
CLONE_SOCK="${CLONE_SOCK:-${WORK_DIR}/fc-clone-${CLONE_ID}.sock}"
SNAP_PATH="${SNAP_PATH:-${WORK_DIR}/snap-${CLONE_ID}}"
MEM_PATH="${MEM_PATH:-${WORK_DIR}/mem-${CLONE_ID}}"
MEM_VOL_NAME="${MEM_VOL_NAME:-fc-mem}"
NETNS="${NETNS:-fc-clone-${CLONE_ID}}"
SNAP_TYPE="${SNAP_TYPE:-}"

TAP_IP="172.16.0.1"
MASK_CIDR="24"
VETH_HOST_IP="10.0.${CLONE_ID}.1"
VETH_NS_IP="10.0.${CLONE_ID}.2"

SNAP_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --snap-only) SNAP_ONLY=true ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

log() { echo "=== $* ===" >&2; }

kill_stale_clone() {
    sudo pkill -f -- "$FC_BIN --api-sock $CLONE_SOCK" 2>/dev/null || true
}

api() {
    local sock="$1" method="$2" path="$3"
    shift 3
    local output
    output=$(sudo curl -s --unix-socket "$sock" -X "$method" "http://localhost${path}" \
        -H 'Content-Type: application/json' -w '\n%{http_code}' "$@")
    local http_code
    http_code=$(echo "$output" | tail -1)
    local body
    body=$(echo "$output" | head -n -1)
    if [ "$http_code" -ge 400 ] 2>/dev/null; then
        echo "ERROR: HTTP $http_code: $body" >&2
        return 1
    fi
    [ -n "$body" ] && echo "$body"
    return 0
}

cleanup() {
    log "Cleaning up clone"
    kill_stale_clone
    sudo ip netns del "$NETNS" 2>/dev/null || true
    sudo ip link del veth-h${CLONE_ID} 2>/dev/null || true
    rm -f "$CLONE_SOCK"
}
trap cleanup EXIT

log "Pausing source VM"
api "$FC_SOCK" PATCH /vm -d '{"state": "Paused"}'

if [ -z "$SNAP_TYPE" ]; then
    if [ -f "${WORK_DIR}/.mem-base-created" ]; then
        SNAP_TYPE="Diff"
    else
        SNAP_TYPE="Full"
    fi
fi

log "Creating $SNAP_TYPE snapshot (memory -> loophole volume '${MEM_VOL_NAME}')"
api "$FC_SOCK" PUT /snapshot/create -d "{
    \"snapshot_type\": \"${SNAP_TYPE}\",
    \"snapshot_path\": \"${SNAP_PATH}\",
    \"mem_file_path\": \"${MEM_PATH}\",
    \"mem_volume_name\": \"${MEM_VOL_NAME}\"
}"

if [ "$SNAP_TYPE" = "Full" ] && [ "$MEM_VOL_NAME" = "fc-mem" ]; then
    touch "${WORK_DIR}/.mem-base-created"
fi

echo "  type:       $SNAP_TYPE"
echo "  snapshot:   $SNAP_PATH ($(stat -c%s "$SNAP_PATH") bytes)"
echo "  mem volume: $MEM_VOL_NAME (loophole, cloned for snapshot)"

log "Resuming source VM"
api "$FC_SOCK" PATCH /vm -d '{"state": "Resumed"}'

if [ "$SNAP_ONLY" = true ]; then
    log "Done (--snap-only)"
    exit 0
fi

log "Setting up network namespace '$NETNS'"
kill_stale_clone
sudo ip netns del "$NETNS" 2>/dev/null || true
sudo ip link del veth-h${CLONE_ID} 2>/dev/null || true

sudo ip netns add "$NETNS"
sudo ip netns exec "$NETNS" ip link set dev lo up
sudo ip netns exec "$NETNS" ip tuntap add dev tap0 mode tap
sudo ip netns exec "$NETNS" ip addr add "${TAP_IP}/${MASK_CIDR}" dev tap0
sudo ip netns exec "$NETNS" ip link set dev tap0 up
echo "  tap0 ${TAP_IP}/${MASK_CIDR} in netns $NETNS"

sudo ip link add veth-h${CLONE_ID} type veth peer name veth-n${CLONE_ID}
sudo ip link set veth-n${CLONE_ID} netns "$NETNS"
sudo ip addr add "${VETH_HOST_IP}/24" dev veth-h${CLONE_ID}
sudo ip link set veth-h${CLONE_ID} up
sudo ip netns exec "$NETNS" ip addr add "${VETH_NS_IP}/24" dev veth-n${CLONE_ID}
sudo ip netns exec "$NETNS" ip link set veth-n${CLONE_ID} up
sudo ip netns exec "$NETNS" ip route add default via "$VETH_HOST_IP"
echo "  veth bridge: host ${VETH_HOST_IP} <-> ns ${VETH_NS_IP}"

HOST_IFACE=$(ip -j route show default | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['dev'])")
if ! sudo iptables -t nat -C POSTROUTING -s "${VETH_HOST_IP%.*}.0/24" -o "$HOST_IFACE" -j MASQUERADE 2>/dev/null; then
    sudo iptables -t nat -A POSTROUTING -s "${VETH_HOST_IP%.*}.0/24" -o "$HOST_IFACE" -j MASQUERADE
fi
echo "  NAT via $HOST_IFACE"

sudo sysctl -w vm.unprivileged_userfaultfd=1 >/dev/null

log "Starting clone Firecracker in netns '$NETNS'"
rm -f "$CLONE_SOCK"

MEM_CLONE_SIDECAR="${SNAP_PATH}.mem-clone"
if [ ! -f "$MEM_CLONE_SIDECAR" ]; then
    echo "ERROR: memory clone sidecar not found: $MEM_CLONE_SIDECAR" >&2
    exit 1
fi
MEM_CLONE_VOL=$(cat "$MEM_CLONE_SIDECAR")
echo "  mem clone:  $MEM_CLONE_VOL"

(
    for i in $(seq 1 50); do
        [ -S "$CLONE_SOCK" ] && break
        sleep 0.1
    done
    if [ ! -S "$CLONE_SOCK" ]; then
        echo "ERROR: clone API socket did not appear" >&2
        exit 1
    fi

    log "Loading snapshot into clone (memory from loophole volume)"
    api "$CLONE_SOCK" PUT /snapshot/load -d "{
        \"snapshot_path\": \"${SNAP_PATH}\",
        \"mem_backend\": {
            \"backend_type\": \"Loophole\",
            \"backend_path\": \"${MEM_CLONE_VOL}\"
        },
        \"enable_diff_snapshots\": true,
        \"resume_vm\": true
    }"

    log "Clone VM restored and running"
) &
LOAD_HELPER_PID=$!

sudo ip netns exec "$NETNS" env \
    LOOPHOLE_INIT=1 \
    LOOPHOLE_CONFIG_DIR="${WORK_DIR}/loophole-config" \
    LOOPHOLE_PROFILE=demo \
    HOME="$HOME" \
    "$FC_BIN" --api-sock "$CLONE_SOCK" --no-seccomp
FC_STATUS=$?

wait "$LOAD_HELPER_PID" || true
exit "$FC_STATUS"
