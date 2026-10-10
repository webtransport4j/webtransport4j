#!/usr/bin/env bash
# ==============================================================================
# Lichess Transport Benchmark: lila-ws (WebSocket) vs WebTransport4J (QUIC)
# ==============================================================================
# Usage:
#   ./scripts/benchmark-lichess.sh [options]
#
# Options:
#   -i, --iterations <N>  Number of iterations per scenario (default: 500)
#   -w, --warmup <N>      Number of warmup iterations (default: 50)
#   -o, --output <file>   Markdown report destination (default: lila-ws-benchmark-results.md)
#   -h, --help            Show this help message
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ITERATIONS=500
WARMUP=50
OUTPUT_FILE="lila-ws-benchmark-results.md"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    -w|--warmup)
      WARMUP="$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    -h|--help)
      grep '^# ' "$0" | cut -c 3-
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

echo "================================================================================"
echo "  Building WebTransport4J & Lichess Benchmark Suite..."
echo "================================================================================"

cd "${ROOT_DIR}"
mvn test-compile -DskipTests -q

echo "================================================================================"
echo "  Running Production-Grade Lichess Benchmark (${ITERATIONS} iterations, ${WARMUP} warmup)..."
echo "================================================================================"

mvn exec:java \
  -Dexec.mainClass="io.github.webtransport4j.example.lichess.benchmark.LilaBenchmarkRunner" \
  -Dexec.args="${ITERATIONS} ${WARMUP}" \
  -Dexec.classpathScope="test" \
  -q

if [[ -f "${OUTPUT_FILE}" ]]; then
  echo ""
  echo "--------------------------------------------------------------------------------"
  echo "  Detailed Markdown Report written to: ${OUTPUT_FILE}"
  echo "--------------------------------------------------------------------------------"
fi
