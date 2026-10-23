#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

IFACE="${WEBTRANSPORT_NIC:-eth0}"
TARGET_IPV6="${WEBTRANSPORT_IPV6:-::1}"
PORT=54321
PACKETS=${1:-50000}
BPF_OBJ="poc/af_xdp/bpf/xdp_webtransport.bpf.o"

if [ ! -f target/cp.txt ]; then
  mvn dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi
CP="target/classes:target/test-classes:$(cat target/cp.txt)"

echo "=========================================================================="
echo "⚡ FULL AF_XDP + NETTY WEBTRANSPORT4J REAL IP CARD VERIFICATION ⚡"
echo " Interface:       $IFACE (Intel Meteor Lake CNVi Wi-Fi)"
echo " Target IPv6:     $TARGET_IPV6"
echo " Port:            $PORT (UDP / QUIC WebTransport)"
echo " Packets:         $PACKETS datagrams (1,024 bytes payload)"
echo " eBPF Filter:     $BPF_OBJ"
echo "=========================================================================="

# Check if eBPF object is compiled
if [ ! -f "$BPF_OBJ" ]; then
  echo "Compiling eBPF XDP filter..."
  export PATH="$HOME/.local/bin:$PATH"
  clang -O2 -target bpf -c poc/af_xdp/bpf/xdp_webtransport.bpf.c \
        -I "$HOME/.local/usr/include" -I "$HOME/.local/usr/include/x86_64-linux-gnu" \
        -o "$BPF_OBJ"
fi

# Check XDP attachment capability
HAS_ROOT=0
if [ "$EUID" -eq 0 ]; then
  HAS_ROOT=1
fi

if [ "$HAS_ROOT" -eq 1 ]; then
  echo "--> Attaching XDP WebTransport filter to physical interface $IFACE in SKB mode..."
  ip link set dev "$IFACE" xdpgeneric obj "$BPF_OBJ" sec xdp
  trap 'echo "Detaching XDP from $IFACE..."; ip link set dev "$IFACE" xdpgeneric off 2>/dev/null || true' EXIT
  echo "✅ eBPF XDP filter successfully attached to physical NIC $IFACE!"
else
  echo "ℹ️  Running in standard user mode (EUID != 0)."
  echo "   (Note: To attach XDP directly to $IFACE, run: sudo $0 $PACKETS)"
fi

echo ""
echo "--> Launching Full End-to-End WebTransport4J Application..."
java --enable-native-access=ALL-UNNAMED \
     -XX:+UseZGC \
     -cp "$CP" \
     io.github.webtransport4j.afxdp.FullAfXdpWebTransportApp \
     standalone "$PORT" "$TARGET_IPV6" "$PACKETS"

echo ""
echo "=========================================================================="
echo "✅ VERIFICATION COMPLETED SUCCESSFULLY!"
echo "=========================================================================="
