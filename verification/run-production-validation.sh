#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-true}
duration=${2:-300}
repeats=${3:-3}
recovery=${4:-80}
case "$mode" in true|false) ;; *) exit 2 ;; esac
[[ "$duration" =~ ^[0-9]+$ && "$repeats" =~ ^[0-9]+$ && "$recovery" =~ ^[0-9]+$ ]]
report_dir="target/production-validation/${mode}-${duration}-$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "$report_dir"
cp_value="target/test-classes:target/classes:$(cat target/validation-classpath.txt)"
server_seconds=$((duration + recovery))
java --enable-native-access=ALL-UNNAMED -Xms256m -Xmx1024m -XX:MaxDirectMemorySize=256m \
  -Dio.netty.eventLoopThreads=4 -Dio.netty.leakDetection.level=advanced \
  "-XX:StartFlightRecording=filename=${report_dir}/server.jfr,settings=profile,dumponexit=true,maxsize=128m" \
  "-Xlog:gc*:file=${report_dir}/gc.log:time,uptime,level,tags" \
  -cp "$cp_value" io.github.webtransport4j.server.ProductionValidationMain \
  server "$mode" "$server_seconds" "$report_dir/server.csv" > "$report_dir/server.log" 2>&1 &
server_pid=$!
trap 'kill -TERM "$server_pid" 2>/dev/null || true' EXIT
port=""
for ((i=0; i<60; i++)); do
  port=$(sed -n 's/^VALIDATION_PORT=//p' "$report_dir/server.log")
  [[ -n "$port" ]] && break
  kill -0 "$server_pid"
  sleep 0.5
done
[[ "$port" =~ ^[0-9]+$ ]]
echo "SERVER pid=$server_pid port=$port mode=$mode duration=$duration reports=$report_dir"
client=(java --enable-native-access=ALL-UNNAMED -Xmx512m -XX:MaxDirectMemorySize=128m
  -Dio.netty.eventLoopThreads=2 -cp "$cp_value" io.github.webtransport4j.server.ProductionValidationMain)
"${client[@]}" soak "$port" "$duration" > "$report_dir/soak.log" 2>&1
echo "SOAK complete"
"${client[@]}" attack "$port" 1000 "$repeats" > "$report_dir/control.log" 2>&1
echo "QUOTA CONTROL complete"
"${client[@]}" attack "$port" 9000 "$repeats" > "$report_dir/attack.log" 2>&1
echo "ATTACK REPLAY complete; waiting for idle recovery"
wait "$server_pid"
trap - EXIT
if rg -n 'OutOfMemoryError|LEAK:|AssertionError' "$report_dir"/*.log; then exit 1; fi
tail -n 2 "$report_dir/server.csv"
echo "VALIDATION COMPLETE $report_dir"
