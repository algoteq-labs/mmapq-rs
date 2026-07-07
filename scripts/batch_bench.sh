#!/usr/bin/env bash
# scripts/batch_bench.sh — standard latency benchmark suite for the batch queue
#
# Runs three configurations in sequence:
#   1. Internal server  — client and server in the same process
#   2. External server  — separate processes, async region mapping
#   3. External server  — separate processes, sync region mapping
#
# Prerequisites:
#   - Kernel boot parameter: isolcpus=5,7,9 (or adjust CORES below)
#   - Run from the project root directory
#
# Usage:
#   ./scripts/batch_bench.sh [options]
#
# Environment variables (override defaults):
#   BENCH_DIR           queue directory                    (default: /tmp)
#   BENCH_ITER          measurement iterations             (default: 20000000)
#   BENCH_WARMUP        warmup iterations                  (default: 100000)
#   BENCH_MSG           message size in bytes              (default: 64)
#   BENCH_CLIENT        client thread CPU core             (default: 5)
#   BENCH_SERVER        server thread CPU core             (default: 7)
#   BENCH_MAPPER        mapper thread CPU core             (default: 9)
#   BENCH_ECHO_MAPPER   echo server mapper thread CPU core (default: -1)

set -euo pipefail

DIR=${BENCH_DIR:-/tmp}
ITER=${BENCH_ITER:-20000000}
WARMUP=${BENCH_WARMUP:-100000}
MSG=${BENCH_MSG:-64}
CLIENT=${BENCH_CLIENT:-5}
SERVER=${BENCH_SERVER:-7}
MAPPER=${BENCH_MAPPER:-9}
ECHO_MAPPER=${BENCH_ECHO_MAPPER:--1}

BENCH="cargo run --release --example batch_latency_bench --"
ECHO="cargo run --release --example batch_echo_server --"

echo "========================================================"
echo "  mmapq-rs batch queue latency benchmark"
echo "  $(date)"
echo "  Cores:   client=$CLIENT  server=$SERVER  mapper=$MAPPER  echo_mapper=$ECHO_MAPPER"
echo "  Config:  iter=$ITER  warmup=$WARMUP  msg=${MSG}B"
echo "  Dir:     $DIR"
echo "========================================================"

# ── 1. Internal server ────────────────────────────────────────────────────────
echo ""
echo "--- INTERNAL SERVER ---"
rm -rf "$DIR/batch_req" "$DIR/batch_res"
sleep 2
$BENCH $ITER $WARMUP $MSG $CLIENT $SERVER $MAPPER

# ── 2. External server — async ────────────────────────────────────────────────
echo ""
echo "--- EXTERNAL SERVER (async) ---"
rm -rf "$DIR/batch_req" "$DIR/batch_res"
sleep 2
$ECHO "$DIR" batch_req batch_res $SERVER $ECHO_MAPPER &
ECHO_PID=$!
while [ ! -d "$DIR/batch_res" ]; do sleep 0.01; done
$BENCH $ITER $WARMUP $MSG $CLIENT $SERVER $MAPPER --external
wait $ECHO_PID 2>/dev/null || true

# ── 3. External server — sync ─────────────────────────────────────────────────
echo ""
echo "--- EXTERNAL SERVER (sync) ---"
rm -rf "$DIR/batch_req" "$DIR/batch_res"
sleep 2
$ECHO "$DIR" batch_req batch_res $SERVER -1 --sync &
ECHO_PID=$!
while [ ! -d "$DIR/batch_res" ]; do sleep 0.01; done
$BENCH $ITER $WARMUP $MSG $CLIENT $SERVER $MAPPER --external --sync
wait $ECHO_PID 2>/dev/null || true

echo ""
echo "========================================================"
echo "  Done."
echo "========================================================"
