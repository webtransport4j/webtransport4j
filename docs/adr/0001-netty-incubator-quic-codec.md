# ADR 0001: Adoption of Netty Incubator QUIC & Quiche Native Engine

## Status
Accepted

## Context
WebTransport requires underlying HTTP/3 and QUIC transports (RFC 9000, RFC 9114, draft-ietf-webtrans-http3). The Java platform lacks native standard QUIC protocol support in the JDK socket layer. We evaluated three architectural approaches:
1. Pure Java user-space QUIC implementation (e.g. Kwik).
2. Binding to Cloudflare Quiche via `netty-incubator-codec-quic` and native BoringSSL/quictls libraries.
3. Custom JNI bindings to msquic or lsquic.

## Decision
We adopted `netty-incubator-codec-quic` (based on Cloudflare Quiche and BoringSSL) integrated directly into the Netty event loop architecture.

## Consequences

### Positive
- **High Throughput & Hardware Acceleration**: BoringSSL and Quiche provide AVX-512 / AES-NI assembly-accelerated cryptographic handshakes and packet encryption.
- **Battle-Tested Event Loop Integration**: Native integration with Netty's `Channel`, `ChannelPipeline`, and event loop threading model without impedance mismatch.
- **Connection Migration & Datagrams**: Built-in native support for QUIC unreliable datagrams (`RFC 9221`) and path validation.

### Negative / Trade-offs
- Multi-architecture native binaries (`.so`, `.dylib`, `.dll`) must be packaged in the JAR for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64).
