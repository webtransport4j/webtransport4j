# WebTransport Interoperability Test Suite

Independent cross-language client implementations and protocol verification suites for [webtransport4j](https://github.com/webtransport4j/webtransport4j).

---

## Directory Structure

```text
interop/
├── README.md                      # Documentation & usage guide
├── run_tests.sh                   # Unified test runner script
├── interop_test_suite.py          # Root entrypoint (forwarder)
├── sitecustomize.py               # Runtime patch & hook forwarder
│
├── python/                        # Python WebTransport implementations
│   ├── interop_test_suite.py      # Official IETF Draft-16 30-Test Suite
│   ├── sitecustomize.py           # Stream event buffering & async shims
│   ├── client.py                  # Interactive CLI client
│   ├── benchmarks/                # Throughput & performance benchmarks
│   │   └── throughput_benchmark.py
│   ├── examples/                  # Real-world application demos
│   │   ├── chat_client.py         # Multi-stream chat client with room commands
│   │   ├── chat_simple.py         # Minimal chat protocol client
│   │   ├── voice_client.py        # Unidirectional voice streaming client
│   │   └── proxy_migration.py     # Connection migration / proxy test
│   └── diagnostics/               # Diagnostic & component verification scripts
│       ├── test_all_streams.py    # Unidirectional & bidirectional stream scanner
│       ├── cli_ini_streams.py     # Client-initiated stream checks
│       ├── server_ini_streams.py  # Server-initiated stream checks
│       ├── dg_test.py             # Datagram echo test
│       ├── test_ping.py           # Low-latency ping test
│       ├── test_idle.py           # Idle timeout probe
│       ├── test_idle_all.py       # Multi-connection idle scan
│       ├── test_kill.py           # Abrupt teardown check
│       ├── test_length_codec.py   # Varint length encoder check
│       ├── test_large_file.py     # Bulk file transfer test
│       └── quick_test.py          # Minimal 10-line connectivity sanity test
│
├── go/                            # Go WebTransport client (quic-go / webtransport-go)
│   ├── client.go                  # Go interop client
│   ├── go.mod                     # Go module definitions
│   ├── go.sum                     # Go checksums
│   └── throughput/                # Go high-throughput benchmark client
│       ├── main.go
│       ├── go.mod
│       └── go.sum
│
└── browser/                       # In-browser WebTransport testing
    └── native-wt-test.html        # Interactive HTML5 WebTransport test console
```

---

## Quick Start

### 1. Start the Java Server
From repository root:
```bash
mvn exec:java -Dexec.mainClass="io.github.webtransport4j.example.ServerSample" \
  -Dwebtransport4j.dev_mode=true \
  -Dwebtransport4j.quic.idle.timeout.seconds=30
```

### 2. Run the Full Test Suite
```bash
# Using the unified runner:
./interop/run_tests.sh

# Or directly with Python:
PYTHONPATH=interop python3 interop/python/interop_test_suite.py
```

### 3. Run the Go Client
```bash
cd interop/go
go run client.go -url https://127.0.0.1:4433/test -insecure
```

### 4. Run Browser Tests
Open `interop/browser/native-wt-test.html` in Chrome or Edge (with `--origin-to-force-quic-on=localhost:4433 --ignore-certificate-errors-spki-list=...`).

---

## Test Matrix (30 / 30 Passed)

The Python test suite (`interop/python/interop_test_suite.py`) verifies both **positive and negative** cases across all sections of **IETF Draft-16** (`draft-ietf-webtrans-http3-16`):

| Section | Scope | Covered Tests |
|---|---|---|
| **§ 3.2** | Session Establishment & URL Path Routing | Primary `/test` session, `/chat` and `/echo` routes, scheme rejection |
| **§ 4.2** | Unidirectional Streams | Client Uni stream, Server Uni stream greeting, unknown session ID drop |
| **§ 4.3** | Bidirectional Streams | Bidi echo, Server Bidi greeting, 250KB payload integrity, 8 concurrent streams, Head-of-line blocking elimination, unknown session ID drop |
| **§ 4.4 & 9.5** | Error Remapping & Stream Abort | Draft-16 error code remapping ($N \leftrightarrow H$), reserved codepoint exclusion, `RESET_STREAM`, `STOP_SENDING` |
| **§ 4.5** | WebTransport Datagrams | Quarter Stream ID encoding & Uni ACK, unknown session ID datagram drop |
| **§ 4.7 & 9.6** | Capsule Protocol (Drain) | Valid `WT_DRAIN_SESSION` (`len=0`), malformed drain rejection (`len>0`) |
| **§ 5.4** | Capsule Protocol (Prohibited) | Prohibited `WT_MAX_STREAM_DATA` (`0x190b4d3e`) & `WT_STREAM_DATA_BLOCKED` (`0x190b4d42`) rejection |
| **§ 6.1 & 9.6** | Capsule Protocol (Close) | Truncated close capsule, oversized reason (>1024B), invalid UTF-8 close, graceful close (`0x2843`) |
| **§ 5.1 & 5.3** | Flow Control & Stream Limits | Concurrency exhaustion boundary & permit recovery |
| **§ 6.0** | Inactivity / Heartbeat Timeout | 15s idle survival (< 30s timeout), 36s inactivity drop (> 30s timeout) |
