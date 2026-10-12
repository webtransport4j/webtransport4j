#!/usr/bin/env bash
set -euo pipefail

TARGET_BYTES=${1:-17179869184} # 16 GiB (17,179,869,184 bytes)
DGRAM_SIZE=${2:-1024}          # 1 KiB per datagram
REPORT_DIR="docs/benchmarks"
mkdir -p "$REPORT_DIR" target

CP="target/classes:target/test-classes:$(cat target/cp.txt)"

echo "=========================================================================="
echo " 16GB WEBTRANSPORT DATAGRAM BENCHMARK (TUNED: 2GB HEAP + RECYCLER + SPIKE TOLERANCE)"
echo " Target Payload: $TARGET_BYTES bytes (16.00 GiB)"
echo " Datagram Size:  $DGRAM_SIZE bytes ($(( TARGET_BYTES / DGRAM_SIZE )) packets)"
echo " Heap Config:    -Xms1024m -Xmx2048m (Tuned Heap with ZAllocationSpikeTolerance=5)"
echo " Netty Config:   -Dio.netty.recycler.maxCapacityPerThread=8192 -Dio.netty.allocator.type=pooled"
echo " JVM:            $(java -version 2>&1 | head -n 1)"
echo "=========================================================================="

run_benchmark() {
  local NAME="$1"
  local VM_FLAGS="$2"
  local GC_LOG="$3"
  local REPORT_FILE="$4"
  local PORT=54330

  echo ""
  echo "--------------------------------------------------------------------------"
  echo ">>> [BENCHMARK RUN] $NAME"
  echo "    JVM Flags: $VM_FLAGS"
  echo "--------------------------------------------------------------------------"

  rm -f server_benchmark.log "$GC_LOG"

  java --enable-native-access=ALL-UNNAMED \
       $VM_FLAGS \
       "-Xlog:gc*:file=$GC_LOG:time,uptime,level,tags" \
       -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
       -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
       -Dio.netty.recycler.maxCapacityPerThread=8192 \
       -Dio.netty.allocator.type=pooled \
       -cp "$CP" \
       io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
       server "$PORT" "$TARGET_BYTES" "$REPORT_FILE" > server_benchmark.log 2>&1 &
  SERVER_PID=$!
  trap 'kill -9 "$SERVER_PID" 2>/dev/null || true' EXIT

  echo "Waiting for server on port $PORT..."
  for i in {1..30}; do
    if grep -q "SERVER_READY_PORT=" server_benchmark.log 2>/dev/null; then
      echo "Server ready (PID: $SERVER_PID). Streaming 16GB datagram flood..."
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
       -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
       -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
       -Dio.netty.recycler.maxCapacityPerThread=8192 \
       -Dio.netty.allocator.type=pooled \
       -cp "$CP" \
       io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
       client "$PORT" "$TARGET_BYTES" "$DGRAM_SIZE"

  local SERVER_STATUS=0
  wait "$SERVER_PID" || SERVER_STATUS=$?
  cat server_benchmark.log
  if (( SERVER_STATUS != 0 )); then
    echo "❌ Benchmark server failed with status $SERVER_STATUS." >&2
    exit "$SERVER_STATUS"
  fi
  trap - EXIT

  echo ""
  echo "--- Native JVM GC Log Verification ($GC_LOG) ---"
  if [[ ! -f "$GC_LOG" || ! -r "$GC_LOG" ]]; then
    echo "❌ GC log is missing or unreadable: $GC_LOG; pause verification unavailable." >&2
    return 1
  elif grep -E "Pause|Garbage Collection" "$GC_LOG"; then
    echo "Recorded GC events in $GC_LOG (showing first 15):"
    grep -E "Pause|Garbage Collection" "$GC_LOG" | sed -n '1,15p'
  else
    echo "✅ Verified: EXACTLY 0 GC pause events recorded in native JVM GC log!"
  fi
}

run_benchmark \
  "Java 25 + ZGC + Compact Headers + 2GB Heap + SpikeTolerance=5 + Recycler" \
  "-XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC -Xms1024m -Xmx2048m -XX:ZAllocationSpikeTolerance=5" \
  "target/server_zgc_optimized_16gb.log" \
  "$REPORT_DIR/DATAGRAM_16GB_ZGC_OPTIMIZED_REPORT.md"

echo ""
echo "=========================================================================="
echo " 16GB OPTIMIZED DATAGRAM BENCHMARK COMPLETED!"
echo " Report generated: $REPORT_DIR/DATAGRAM_16GB_ZGC_OPTIMIZED_REPORT.md"
echo "=========================================================================="
