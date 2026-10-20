#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [ -z "${JAVA_HOME:-}" ] && [ -d "$HOME/.tools/jdk-25" ]; then
  export JAVA_HOME="$HOME/.tools/jdk-25"
  export PATH="$JAVA_HOME/bin:${PATH}"
fi

TARGET_BYTES=${1:-10737418240} # 10 GiB (10,737,418,240 bytes)
DGRAM_SIZE=${2:-1024}          # 1 KiB per datagram
CONCURRENCY=22                 # 22 client connections on 22 cores
SOCKET_COUNT=22                # 22 sharded UDP sockets
PACING_MICROS=${3:-1}          # 1 µs pacing for 0-loss rate matching
TRANSPORT="auto"               # Auto: prefers Epoll on Linux with GRO/GSO
PORT=54390
REPORT_DIR="docs/benchmarks"
REPORT_FILE="$REPORT_DIR/DATAGRAM_10GB_22_CHANNEL_REPORT.md"
SERVER_LOG="target/server_10gb_benchmark.log"
GC_LOG="target/server_10gb_gc.log"
SERVER_READY_TIMEOUT_SECONDS=${SERVER_READY_TIMEOUT_SECONDS:-120}

mkdir -p "$REPORT_DIR" target

mvn test-compile -DskipTests

if [ ! -f target/cp.txt ]; then
  mvn dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi

CP="target/classes:target/test-classes:$(cat target/cp.txt)"

echo "=========================================================================="
echo " 10GB 22-CHANNEL WEBTRANSPORT BENCHMARK (JAVA 25 + ZGC + AUTO EPOLL+GRO/GSO)"
echo " CPU Cores:      $(nproc) logical cores"
echo " Target Payload: $TARGET_BYTES bytes ($(( TARGET_BYTES / 1024 / 1024 / 1024 )) GiB)"
echo " Datagram Size:  $DGRAM_SIZE bytes ($(( TARGET_BYTES / DGRAM_SIZE )) packets)"
echo " Concurrency:    $CONCURRENCY parallel client channels (1 per core)"
echo " Server Sockets: $SOCKET_COUNT sharded sockets (SO_REUSEPORT)"
echo " Pacing:         ${PACING_MICROS} µs/datagram (zero-loss rate matching)"
echo " Transport:      $TRANSPORT (Epoll with GRO/GSO)"
echo " JVM:            $(java -version 2>&1 | head -n 1)"
echo "=========================================================================="

rm -f "$SERVER_LOG" "$GC_LOG" "$REPORT_FILE"

java --enable-native-access=ALL-UNNAMED \
     -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC \
     -XX:+AlwaysPreTouch -XX:ZAllocationSpikeTolerance=5 \
     -Xms6144m -Xmx6144m \
     "-Xlog:gc*:file=$GC_LOG:time,uptime,level,tags" \
     -Dwebtransport4j.server.transport="$TRANSPORT" \
     -Dwebtransport4j.epoll.udpgro=true \
     -Dwebtransport4j.epoll.udpgso=true \
     -Dwebtransport4j.server.socket.rcvbuf=67108864 \
     -Dwebtransport4j.server.recv.max_messages_per_read=128 \
     -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
     -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
     -cp "$CP" \
     io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
     server "$PORT" "$TARGET_BYTES" "$REPORT_FILE" "$SOCKET_COUNT" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
trap 'kill -9 "$SERVER_PID" 2>/dev/null || true' EXIT

echo "Waiting for server on port $PORT..."
READY_DEADLINE=$((SECONDS + SERVER_READY_TIMEOUT_SECONDS))
while (( SECONDS < READY_DEADLINE )); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "❌ Server exited before becoming ready. Log output:"
    cat "$SERVER_LOG"
    exit 1
  fi
  if grep -q "SERVER_READY_PORT=" "$SERVER_LOG" 2>/dev/null; then
    echo "Server ready (PID: $SERVER_PID). Streaming 10 GB datagram flood across 22 channels..."
    break
  fi
  sleep 0.3
done

if ! grep -q "SERVER_READY_PORT=" "$SERVER_LOG" 2>/dev/null; then
  echo "❌ Server failed to bind. Log output:"
  cat "$SERVER_LOG"
  kill -9 "$SERVER_PID" 2>/dev/null || true
  exit 1
fi

# Launch 22-channel client flood
java --enable-native-access=ALL-UNNAMED \
     -XX:+UnlockExperimentalVMOptions -XX:+UseCompactObjectHeaders -XX:+UseZGC \
     -XX:+AlwaysPreTouch \
     -Xms3072m -Xmx3072m \
     -Dwebtransport4j.quic.datagram.recv.queue.len=65536 \
     -Dwebtransport4j.quic.datagram.send.queue.len=65536 \
     -cp "$CP" \
     io.github.webtransport4j.benchmark.Datagram1GbZgcBenchmark \
     client "$PORT" "$TARGET_BYTES" "$DGRAM_SIZE" "$CONCURRENCY" "$PACING_MICROS"

SERVER_STATUS=0
wait "$SERVER_PID" || SERVER_STATUS=$?
trap - EXIT

cat "$SERVER_LOG"

if (( SERVER_STATUS != 0 )); then
  echo "❌ Benchmark server failed (exit status $SERVER_STATUS)."
  exit "$SERVER_STATUS"
fi

if ! grep -Fxq '| **Run Status** | **COMPLETE** |' "$REPORT_FILE" \
    || ! grep -Fxq '| **Completed On Time (latch)** | true |' "$REPORT_FILE" \
    || ! grep -Fxq '| **Target Reached** | true |' "$REPORT_FILE"; then
  echo "❌ Benchmark report does not confirm the target payload was ingested on time."
  exit 1
fi

echo ""
echo "--- Native JVM GC Log Verification ($GC_LOG) ---"
if grep -E "Pause" "$GC_LOG" >/dev/null 2>&1; then
  echo "Recorded GC events in $GC_LOG:"
  { grep -E "Pause" "$GC_LOG" 2>/dev/null || true; } | head -n 10
else
  echo "✅ Verified: EXACTLY 0 GC pause events recorded in native JVM GC log!"
fi

echo ""
echo "=========================================================================="
echo " 10GB 22-CHANNEL BENCHMARK COMPLETE!"
echo " Report saved to: $REPORT_FILE"
echo "=========================================================================="
