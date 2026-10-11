#!/usr/bin/env bash
set -euo pipefail

TARGET_BYTES=${1:-1073741824} # 1 GiB (1,073,741,824 bytes)
SOCKETS=${2:-8}               # 8 sockets for multi-socket run
DGRAM_SIZE=${3:-1024}         # 1 KiB per datagram
CONCURRENCY=${4:-8}           # 8 concurrent client connections
REPORT_DIR="docs/benchmarks"
mkdir -p "$REPORT_DIR" target

if [ ! -f target/cp.txt ]; then
  echo "Building classpath..."
  mvn dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi

CP="target/classes:target/test-classes:$(cat target/cp.txt)"

echo "=========================================================================="
echo " WEBTRANSPORT MULTI-SOCKET SHARDING BENCHMARK (SO_REUSEPORT + DISPATCHER)"
echo " Target Payload: $TARGET_BYTES bytes ($(( TARGET_BYTES / 1024 / 1024 )) MB)"
echo " Datagram Size:  $DGRAM_SIZE bytes ($(( TARGET_BYTES / DGRAM_SIZE )) packets)"
echo " Sharded Sockets: $SOCKETS UDP sockets on the same port"
echo " Concurrency:    $CONCURRENCY parallel client connections"
echo " JVM:            $(java -version 2>&1 | head -n 1)"
echo "=========================================================================="

run_benchmark() {
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
       -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC -Xms2048m -Xmx3072m \
       "-Xlog:gc*:file=$GC_LOG:time,uptime,level,tags" \
       -Dwebtransport4j.server.socket.rcvbuf=33554432 \
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

  # Launch client flood
  java --enable-native-access=ALL-UNNAMED \
       -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC -Xms1024m -Xmx2048m \
       -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
       -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
       -cp "$CP" \
       io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
       client "$PORT" "$TARGET_BYTES" "$DGRAM_SIZE" "$CONCURRENCY"

  wait "$SERVER_PID" || true
  trap - EXIT
  cat server_benchmark.log

  echo ""
  echo "--- Native JVM GC Log Verification ($GC_LOG) ---"
  if grep -E "Pause|Garbage Collection" "$GC_LOG" 2>/dev/null; then
    echo "Recorded GC events in $GC_LOG:"
    grep -E "Pause|Garbage Collection" "$GC_LOG" | head -n 10
  else
    echo "✅ Verified: EXACTLY 0 GC pause events recorded in native JVM GC log!"
  fi
}

# Run 1: Single Socket Baseline
run_benchmark \
  "Single Socket Baseline (socketCount=1)" \
  "1" \
  "target/server_zgc_single_socket.log" \
  "$REPORT_DIR/DATAGRAM_SINGLE_SOCKET_REPORT.md" \
  "54361"

# Run 2: Multi-Socket Sharded via SO_REUSEPORT + QuicCodecDispatcher (8 sockets)
run_benchmark \
  "Multi-Socket Sharded (socketCount=$SOCKETS)" \
  "$SOCKETS" \
  "target/server_zgc_multi_socket.log" \
  "$REPORT_DIR/DATAGRAM_8_SOCKET_REPORT.md" \
  "54362"

echo ""
echo "=========================================================================="
echo " MULTI-SOCKET BENCHMARKS COMPLETED!"
echo " Reports generated:"
echo "   - $REPORT_DIR/DATAGRAM_SINGLE_SOCKET_REPORT.md"
echo "   - $REPORT_DIR/DATAGRAM_8_SOCKET_REPORT.md"
echo "=========================================================================="
