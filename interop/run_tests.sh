#!/usr/bin/env bash
# ==============================================================================
# WebTransport Interoperability Test Runner
# Usage: ./run_tests.sh [python]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:-python}"
MAIN_CLASS="io.github.webtransport4j.example.ServerSample"
SERVER_LOG="${SCRIPT_DIR}/interop-server.log"
SERVER_PID=""

case "${TARGET}" in
    python) ;;
    *)
        echo "❌ Unknown target: ${TARGET}"
        echo "Valid targets: python"
        exit 1
        ;;
esac

for command in mvn java python3 lsof pgrep; do
    command -v "${command}" >/dev/null 2>&1 || {
        echo "❌ Required command not found: ${command}"
        exit 1
    }
done

stop_process() {
    local pid="$1"

    kill -TERM "${pid}" 2>/dev/null || return 0

    for ((i = 0; i < 10; i++)); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done

    kill -KILL "${pid}" 2>/dev/null || true
}

cleanup() {
    if [[ -n "${SERVER_PID}" ]]; then
        echo "🛑 Stopping test server..."
        stop_process "${SERVER_PID}"
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "=================================================================="
echo "🌐 WebTransport Interoperability Test Suite"
echo "=================================================================="

# Match only Java processes associated with this sample server.
# Do not kill unrelated Java applications.
SERVER_PATTERN='java.*io[.]github[.]webtransport4j[.]example[.]ServerSample'

while IFS= read -r pid; do
    [[ -n "${pid}" ]] || continue
    echo "🛑 Stopping existing ServerSample instance: ${pid}"
    stop_process "${pid}"
done < <(pgrep -f "${SERVER_PATTERN}" || true)

echo "🚀 Starting WebTransport server..."
echo "📝 Server log: ${SERVER_LOG}"

# Run Maven from the directory containing the project's pom.xml,
# or pass MAVEN_PROJECT_DIR explicitly.
(
    cd "${MAVEN_PROJECT_DIR:-${SCRIPT_DIR}/..}"
    exec mvn exec:java \
        -Dexec.mainClass="${MAIN_CLASS}" \
        -Dwebtransport4j.dev_mode=true \
        -Dwebtransport4j.quic.idle.timeout.seconds=30
) >"${SERVER_LOG}" 2>&1 &

SERVER_PID=$!

echo "⏳ Waiting for the server's UDP socket..."

ready=false
for ((i = 0; i < 120; i++)); do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "❌ Server exited during startup."
        tail -n 80 "${SERVER_LOG}"
        exit 1
    fi

    # exec:java runs the sample inside Maven's JVM.
    if lsof -nP -a -p "${SERVER_PID}" -iUDP \
        >/dev/null 2>&1; then
        ready=true
        break
    fi

    sleep 1
done

if [[ "${ready}" != true ]]; then
    echo "❌ Server did not bind a UDP socket within 120 seconds."
    tail -n 80 "${SERVER_LOG}"
    exit 1
fi

echo "✅ Server UDP socket is bound."
echo "🐍 Running Python IETF Draft-16 Interop Suite..."

export PYTHONPATH="${SCRIPT_DIR}:${SCRIPT_DIR}/python${PYTHONPATH:+:${PYTHONPATH}}"
python3 "${SCRIPT_DIR}/python/interop_test_suite.py"