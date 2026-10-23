#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

PACKETS=${1:-50000}

echo "=========================================================================="
echo "⚡ RUNNING 100% REAL NETWORK CARD & IPv6 INTERNET ROUTE VERIFICATION ⚡"
echo " Datagrams:  $PACKETS"
echo " Mode:       $(if [ "$EUID" -eq 0 ]; then echo "ROOT/PRIVILEGED (Direct Driver XDP Hook)"; else echo "STANDARD USER (Checks Kernel Capabilities & Fallback)"; fi)"
echo "=========================================================================="

mvn -q test-compile exec:java \
  -Dexec.mainClass="io.github.webtransport4j.afxdp.RealNetworkCardIpv6Check" \
  -Dexec.classpathScope="test" \
  -Dexec.args="$PACKETS"
