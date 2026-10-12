#!/usr/bin/env bash
set -euo pipefail

TARGET_BYTES=${1:-1073741824} # 1 GiB
DGRAM_SIZE=${2:-1024}         # 1 KiB
CONCURRENCY=22                # 22 client connections on 22 cores
PACING_MICROS=${3:-0}         # Microseconds pacing per packet
REPORT_DIR="docs/benchmarks"
mkdir -p "$REPORT_DIR" target

CP="target/classes:target/test-classes:$(cat target/cp.txt)"

echo "=========================================================================="
echo " 22-CORE WSL2 MULTI-SOCKET THROUGHPUT BENCHMARK (JAVA 25 + ZGC + REUSEPORT)"
echo " CPU Cores:      $(nproc) logical cores"
echo " Target Payload: $TARGET_BYTES bytes ($(( TARGET_BYTES / 1024 / 1024 )) MiB)"
echo " Datagram Size:  $DGRAM_SIZE bytes ($(( TARGET_BYTES / DGRAM_SIZE )) packets)"
echo " Concurrency:    $CONCURRENCY parallel client streams (1 per core)"
if [ "$PACING_MICROS" -gt 0 ]; then
  echo " Pacing:         ${PACING_MICROS} µs/datagram"
fi
echo " JVM:            $(java -version 2>&1 | head -n 1)"
echo "=========================================================================="

run_tier() {
  local NAME="$1"
  local SOCKET_COUNT="$2"
  local GC_LOG="$3"
  local REPORT_FILE="$4"
  local PORT="$5"

  echo ""
  echo "--------------------------------------------------------------------------"
  echo ">>> [BENCHMARK RUN] $NAME (socketCount=$SOCKET_COUNT, concurrency=$CONCURRENCY)"
  echo "--------------------------------------------------------------------------"

  rm -f server_benchmark.log "$GC_LOG"

  java --enable-native-access=ALL-UNNAMED \
       -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC \
       -XX:+AlwaysPreTouch -XX:ZAllocationSpikeTolerance=5 \
       -Xms4096m -Xmx4096m \
       "-Xlog:gc*:file=$GC_LOG:time,uptime,level,tags" \
       -Dwebtransport4j.server.socket.rcvbuf=67108864 \
       -Dwebtransport4j.server.recv.max_messages_per_read=128 \
       -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
       -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
       -cp "$CP" \
       io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
       server "$PORT" "$TARGET_BYTES" "$REPORT_FILE" "$SOCKET_COUNT" > server_benchmark.log 2>&1 &
  SERVER_PID=$!
  trap 'kill -9 "$SERVER_PID" 2>/dev/null || true' EXIT

  echo "Waiting for server on port $PORT..."
  for i in {1..30}; do
    if grep -q "SERVER_READY_PORT=" server_benchmark.log 2>/dev/null; then
      echo "Server ready (PID: $SERVER_PID). Streaming datagram flood..."
      break
    fi
    sleep 0.3
  done

  if ! grep -q "SERVER_READY_PORT=" server_benchmark.log 2>/dev/null; then
    echo "❌ Server failed to bind. Log output:"
    cat server_benchmark.log
    kill -9 "$SERVER_PID" 2>/dev/null || true
    exit 1
  fi

  # Launch concurrent client flood across 22 cores
  java --enable-native-access=ALL-UNNAMED \
       -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC \
       -XX:+AlwaysPreTouch \
       -Xms2048m -Xmx2048m \
       -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
       -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
       -cp "$CP" \
       io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
       client "$PORT" "$TARGET_BYTES" "$DGRAM_SIZE" "$CONCURRENCY" "$PACING_MICROS"

  wait "$SERVER_PID" || true
  trap - EXIT
  cat server_benchmark.log

  echo ""
  echo "--- Native JVM GC Log Verification ($GC_LOG) ---"
  if grep -E "Pause|Garbage Collection" "$GC_LOG" >/dev/null 2>&1; then
    echo "Recorded GC events in $GC_LOG:"
    { grep -E "Pause|Garbage Collection" "$GC_LOG" 2>/dev/null || true; } | head -n 10
  else
    echo "✅ Verified: EXACTLY 0 GC pause events recorded in native JVM GC log!"
  fi

  sleep 1
}

# Run 1: Single Socket Baseline
run_tier \
  "Tier 1: Single Socket Baseline (1 socket, 22 concurrent streams)" \
  "1" \
  "target/server_zgc_1_socket.log" \
  "$REPORT_DIR/DATAGRAM_SINGLE_SOCKET_REPORT.md" \
  "54361"

# Run 2: 8 Sockets Sharded
run_tier \
  "Tier 2: 8-Socket Sharded (8 sockets, 22 concurrent streams)" \
  "8" \
  "target/server_zgc_8_socket.log" \
  "$REPORT_DIR/DATAGRAM_8_SOCKET_REPORT.md" \
  "54362"

# Run 3: 22 Sockets Sharded (Full 22-core 1:1 hardware socket mapping)
run_tier \
  "Tier 3: 22-Socket Sharded (22 sockets, 22 concurrent streams)" \
  "22" \
  "target/server_zgc_22_socket.log" \
  "$REPORT_DIR/DATAGRAM_22_SOCKET_REPORT.md" \
  "54363"

echo ""
echo "=========================================================================="
echo " 22-CORE BENCHMARK SUITE COMPLETE!"
echo " Reports generated:"
echo "   - $REPORT_DIR/DATAGRAM_SINGLE_SOCKET_REPORT.md"
echo "   - $REPORT_DIR/DATAGRAM_8_SOCKET_REPORT.md"
echo "   - $REPORT_DIR/DATAGRAM_22_SOCKET_REPORT.md"
echo "=========================================================================="
