#!/usr/bin/env bash
set -euo pipefail

# Repeatedly run storage2 TestSimulation until a failure appears.

RUN_TEST="${RUN_TEST:-TestSimulation}"
SIM_PAGES_FIXED="${SIM_PAGES_FIXED:-256}"

LOG_DIR="${SIM_FUZZ_LOG_DIR:-/tmp/sim-fuzz-storage2}"
START_RUN="${SIM_FUZZ_START_RUN:-1}"
MAX_RUNS="${SIM_FUZZ_MAX_RUNS:-0}" # 0 means run forever
PARALLELISM="${SIM_FUZZ_PARALLELISM:-4}"

NODES_STR="${SIM_FUZZ_NODES_VALUES:-3 5 7 9}"
CRASH_STR="${SIM_FUZZ_CRASH_VALUES:-0.01 0.02 0.05 0.10 0.15}"
FAULT_STR="${SIM_FUZZ_FAULT_VALUES:-0.01 0.02 0.05 0.10 0.15 0.20}"
OPS_STR="${SIM_FUZZ_OPS_VALUES:-500 1000 2000 3000 5000}"
TIMELINE_STR="${SIM_FUZZ_TIMELINE_VALUES:-20 40 80 120}"

read -r -a NODES_VALUES <<< "$NODES_STR"
read -r -a CRASH_VALUES <<< "$CRASH_STR"
read -r -a FAULT_VALUES <<< "$FAULT_STR"
read -r -a OPS_VALUES <<< "$OPS_STR"
read -r -a TIMELINE_VALUES <<< "$TIMELINE_STR"

pick() {
  local arr=("$@")
  local idx=$((RANDOM % ${#arr[@]}))
  printf '%s' "${arr[$idx]}"
}

rand_u64() {
  od -An -N8 -tu8 /dev/urandom | tr -d ' '
}

mkdir -p "$LOG_DIR"

STATE_DIR="$(mktemp -d "$LOG_DIR/state.XXXXXX")"
COUNTER_FILE="$STATE_DIR/counter"
LOCK_DIR="$STATE_DIR/lock"
FAIL_FLAG="$STATE_DIR/fail"
FAIL_SUMMARY="$STATE_DIR/fail-summary.txt"
echo "$START_RUN" > "$COUNTER_FILE"

cleanup() {
  rm -rf "$STATE_DIR"
}
trap cleanup EXIT

next_run() {
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    sleep 0.05
  done

  local run current
  run="$(cat "$COUNTER_FILE")"
  if [[ "$MAX_RUNS" -ne 0 && "$run" -ge $((START_RUN + MAX_RUNS)) ]]; then
    rmdir "$LOCK_DIR"
    return 1
  fi

  current="$run"
  run=$((run + 1))
  echo "$run" > "$COUNTER_FILE"
  rmdir "$LOCK_DIR"
  echo "$current"
}

worker_loop() {
  local worker="$1"
  local run seed nodes crash fault timelines ops log
  while true; do
    if [[ -f "$FAIL_FLAG" ]]; then
      return 0
    fi

    run="$(next_run)" || return 0
    seed="$(rand_u64)"
    nodes="$(pick "${NODES_VALUES[@]}")"
    crash="$(pick "${CRASH_VALUES[@]}")"
    fault="$(pick "${FAULT_VALUES[@]}")"
    timelines="$(pick "${TIMELINE_VALUES[@]}")"
    ops="$(pick "${OPS_VALUES[@]}")"
    log="$LOG_DIR/run-${run}.w${worker}.log"

    echo "RUN $run worker=$worker seed=$seed ops=$ops nodes=$nodes crash=$crash fault=$fault timelines=$timelines"

    sim_env=(
      SIM_SEED="$seed"
      SIM_OPS="$ops"
      SIM_PAGES="$SIM_PAGES_FIXED"
      SIM_NODES="$nodes"
      SIM_CRASH_RATE="$crash"
      SIM_FAULT_RATE="$fault"
      SIM_TIMELINES="$timelines"
    )

    if env "${sim_env[@]}" go test -tags "" -run "$RUN_TEST" -timeout 600s ./storage2/ >"$log" 2>&1; then
      echo "PASS $run worker=$worker"

    else
      if [[ ! -f "$FAIL_FLAG" ]]; then
        {
          echo "FAIL $run worker=$worker"
          echo "Log: $log"
          echo
          echo "Repro:"
          echo "SIM_SEED=$seed SIM_OPS=$ops SIM_PAGES=$SIM_PAGES_FIXED SIM_NODES=$nodes SIM_CRASH_RATE=$crash SIM_FAULT_RATE=$fault SIM_TIMELINES=$timelines go test -tags '' -run $RUN_TEST -timeout 60s ./storage2/"
        } > "$FAIL_SUMMARY"
        touch "$FAIL_FLAG"
      fi
      return 1
    fi
  done
}

echo "Starting $PARALLELISM workers in parallel..."
pids=()
for worker in $(seq 1 "$PARALLELISM"); do
  worker_loop "$worker" &
  pids+=("$!")
done

failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  cat "$FAIL_SUMMARY"
  echo
  rg -n "^--- FAIL:|=== MISMATCH|seed:|panic:|FAIL$" "$(awk '/^Log:/ {print $2}' "$FAIL_SUMMARY")" || true
  exit 1
fi

echo "Reached SIM_FUZZ_MAX_RUNS=$MAX_RUNS with no failures."
