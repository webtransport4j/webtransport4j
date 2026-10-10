#!/usr/bin/env bash
set -euo pipefail

PORT=54321
OPS=${1:-20000}
CP="target/classes:$(mvn dependency:build-classpath | grep -v '\[INFO\]' | tr '\n' ':')"

echo "=========================================================================="
echo " MULTI-PRIMITIVE GC OBSERVATION (ISOLATED DEDICATED SERVER JVM)"
echo " Communication Primitives: Datagrams, Bidirectional & Unidirectional Streams"
echo " Server Mode: Experimental Pipeline + Owned Callback Buffers"
echo " External Client: Separate Dedicated JVM Process"
echo " Operations Target: $OPS total messages"
echo "=========================================================================="

if [ "$OPS" -ge 100000 ]; then
  HOTSPOT_HEAP_ARGS="-Xms4096m -Xmx4096m -XX:NewSize=2048m -XX:MaxNewSize=2048m -XX:MetaspaceSize=128m"
  EPSILON_HEAP_ARGS="-Xms4096m -Xmx4096m"
else
  HOTSPOT_HEAP_ARGS="-XX:NewSize=512m -XX:MaxNewSize=512m -XX:MetaspaceSize=128m"
  EPSILON_HEAP_ARGS="-Xms1024m -Xmx1024m"
fi

mkdir -p docs/benchmarks

# Finite GC observation only: neither run proves zero allocation or production readiness.
echo ""
echo "--> [TEST 1/2] Standard Production HotSpot GC ($HOTSPOT_HEAP_ARGS)..."
rm -f server_output.log server_gc_hotspot.log
java $HOTSPOT_HEAP_ARGS \
     "-Xlog:gc*:file=server_gc_hotspot.log:time,uptime,level,tags" \
     -cp "$CP" \
     io.github.webtransport4j.example.benchmark.ZeroGcServerMain "$PORT" "docs/benchmarks/SERVER_ZERO_GC_HOTSPOT_REPORT.md" > server_output.log 2>&1 &
SERVER_PID=$!

echo "Waiting for server to bind on port $PORT..."
for i in {1..30}; do
  if grep -q "SERVER_READY_PORT=" server_output.log 2>/dev/null; then
    echo "Server is ready! (PID: $SERVER_PID)"
    break
  fi
  sleep 0.5
done

if ! grep -q "SERVER_READY_PORT=" server_output.log 2>/dev/null; then
  echo "Server failed to start. Logs:"
  cat server_output.log
  kill -9 $SERVER_PID 2>/dev/null || true
  exit 1
fi

echo "Launching Separate External Client Process (Flooding Datagrams, Bi-Streams, Uni-Streams)..."
java -cp "$CP" \
     io.github.webtransport4j.example.benchmark.ZeroGcClientMain "$PORT" "$OPS"

wait "$SERVER_PID"
cat server_output.log

echo ""
echo "--- Native JVM GC Log Verification (server_gc_hotspot.log) ---"
if grep -E "Pause|Garbage Collection" server_gc_hotspot.log 2>/dev/null; then
  echo "⚠️ GC pauses recorded in log:"
  grep -E "Pause|Garbage Collection" server_gc_hotspot.log
else
  echo "✅ Verified: EXACTLY 0 GC pause events recorded in native JVM GC log!"
fi

# Test 2: finite Epsilon capacity check; successful completion is not an allocation proof.
echo ""
echo "--> [TEST 2/2] Epsilon GC Proof Engine (-XX:+UseEpsilonGC $EPSILON_HEAP_ARGS)..."
PORT=54322
rm -f server_output.log server_gc_epsilon.log
java -XX:+UnlockExperimentalVMOptions \
     -XX:+UseEpsilonGC \
     $EPSILON_HEAP_ARGS \
     "-Xlog:gc*:file=server_gc_epsilon.log:time,uptime,level,tags" \
     -cp "$CP" \
     io.github.webtransport4j.example.benchmark.ZeroGcServerMain "$PORT" "docs/benchmarks/SERVER_ZERO_GC_EPSILON_REPORT.md" > server_output.log 2>&1 &
SERVER_PID=$!

echo "Waiting for server to bind on port $PORT..."
for i in {1..30}; do
  if grep -q "SERVER_READY_PORT=" server_output.log 2>/dev/null; then
    echo "Server is ready! (PID: $SERVER_PID)"
    break
  fi
  sleep 0.5
done

if ! grep -q "SERVER_READY_PORT=" server_output.log 2>/dev/null; then
  echo "Server failed to start. Logs:"
  cat server_output.log
  kill -9 $SERVER_PID 2>/dev/null || true
  exit 1
fi

echo "Launching Separate External Client Process..."
java -cp "$CP" \
     io.github.webtransport4j.example.benchmark.ZeroGcClientMain "$PORT" "$OPS"

wait "$SERVER_PID"
cat server_output.log

echo ""
echo "--- Native JVM GC Log Verification (server_gc_epsilon.log) ---"
if grep -E "OutOfMemoryError|Out of memory" server_gc_epsilon.log server_output.log 2>/dev/null; then
  echo "❌ OOM detected in Epsilon GC log!"
  cat server_gc_epsilon.log
  exit 1
else
  echo "Epsilon completed this finite workload without reported memory exhaustion. Allocation was not measured."
fi

# Produce unified markdown report
cp docs/benchmarks/SERVER_ZERO_GC_HOTSPOT_REPORT.md docs/benchmarks/SERVER_ZERO_GC_REPORT.md

echo ""
echo "=========================================================================="
echo " FINITE GC OBSERVATION COMPLETED; NOT A ZERO-ALLOCATION OR PRODUCTION CERTIFICATION"
echo "=========================================================================="
