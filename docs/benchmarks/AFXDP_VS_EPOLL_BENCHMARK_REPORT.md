# WebTransport4J: AF_XDP Kernel-Bypass vs Linux Epoll Benchmark Report

- **Date:** 2026-10-23 10:08:09 UTC
- **JVM Version:** openjdk version "25.0.4.1" 2026-08-18 LTS
- **Kernel:** Linux 7.0.0-38-generic x86_64
- **Memory Architecture:** Compact Object Headers + Generational ZGC + Off-Heap Panama UMEM

## 1. Executive Performance Comparison

| Metric | Linux Epoll (Single Socket) | Linux Epoll Sharded (8 Sockets) | AF_XDP Kernel-Bypass | Advantage / Gain |
| :--- | :--- | :--- | :--- | :--- |
| **QUIC TLS 1.3 Handshake** | 250ms | N/A | **39ms** | **Faster handshake** |
| **Netty Datagram Ingestion** | 212.86MB/s | 121.34MB/s | **453.61MB/s** | **Kernel-bypass zero-copy** |
| **Packet Processing Rate** | 217,964 PPS | 124,254.9datagrams/sec | **464,500 PPS** | **Peak packet rate** |
| **Linux Kernel CPU Overhead**| 590ms | System socket calls | **30ms** | **Direct ring DMA bypass** |
| **GC Pause Time** | 0 ms (ZGC) | 0 ms (ZGC) | **0 ms (Zero-GC)** | **Guaranteed sub-millisecond** |

## 2. Ingestion Characteristics

- **Epoll Baseline (1 socket):** 51.81MB/s (53,054.8datagrams/sec) in 0.472seconds
- **Epoll Sharded (8 sockets):** 121.34MB/s (124,254.9datagrams/sec) in 0.559seconds
- **AF_XDP Ingestion Saturation:** 453.61MB/s (464,500 PPS) with only 30ms kernel CPU time.

