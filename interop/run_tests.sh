#!/usr/bin/env bash
# ==============================================================================
# WebTransport Interoperability Test Runner
# ==============================================================================
# Usage:
#   ./run_tests.sh          # Runs Python Draft-16 Interop Test Suite (default)
#   ./run_tests.sh python   # Runs Python Draft-16 Interop Test Suite
#   ./run_tests.sh go       # Runs Go WebTransport Client Test
#   ./run_tests.sh all      # Runs all available automated suites
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:-python}"

echo "=================================================================="
echo "🌐 WebTransport Interoperability Test Suite"
echo "=================================================================="

run_python_suite() {
    echo "🐍 Running Python IETF Draft-16 Interop Suite (30 Tests)..."
    export PYTHONPATH="${SCRIPT_DIR}:${SCRIPT_DIR}/python:${PYTHONPATH:-}"
    python3 "${SCRIPT_DIR}/python/interop_test_suite.py"
}

run_go_suite() {
    echo "🐹 Running Go WebTransport Client..."
    cd "${SCRIPT_DIR}/go"
    go run client.go -url https://127.0.0.1:4433/test -insecure
}

case "${TARGET}" in
    python)
        run_python_suite
        ;;
    go)
        run_go_suite
        ;;
    all)
        run_python_suite
        echo ""
        run_go_suite
        ;;
    *)
        echo "❌ Unknown target: ${TARGET}"
        echo "Valid targets: python, go, all"
        exit 1
        ;;
esac
