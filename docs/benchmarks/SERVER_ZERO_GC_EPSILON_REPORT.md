# WebTransport4J: Reduced-Allocation Pipeline GC Observation Report
This finite run measures collections and heap usage, not allocated bytes per operation.
Zero observed collections do not establish zero allocation or production readiness.

Empirical measurement of the dedicated server process running in an isolated JVM
while receiving concurrent sustained flood traffic across all three WebTransport primitives:
Datagrams, Bidirectional Streams, and Unidirectional Streams.

### Multi-Primitive Traffic Breakdown

| Communication Primitive | Ingested & Processed Count | Mode |
| :--- | :--- | :--- |
| **Datagrams** | 1,000,000 | Owned-buffer Echo |
| **Bidirectional Streams** | 1,000,000 | Owned-buffer Stream Echo |
| **Unidirectional Streams** | 1,000,000 | Owned-buffer Stream Ingestion |
| **TOTAL MESSAGES** | **3,000,000** | Mixed Concurrent Flood |
| **TOTAL PAYLOAD** | **92,000,000 bytes** (87.74 MB) | Off-Heap Direct Buffers |

### Garbage Collection & JVM Heap Metrics

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **Active Flood Duration** | 15.853 s | - |
| **Aggregate Throughput** | 189,244.3 ops/s | - |
| **GC Collections** | **0** | ✅ **ZERO COLLECTIONS** |
| **Total GC Pause Time** | **0 ms** | ✅ **ZERO PAUSE TIME** |
| **Used Heap at Start** | 183,661,792 bytes (175.15 MB) | Pre-flood baseline |
| **Used Heap at End** | 697,668,832 bytes (665.35 MB) | Post-flood steady-state |
| **Heap Delta** | 514,007,040 bytes | Net change; does not measure total allocation |

### Active Garbage Collectors (JVM MXBeans)

| Collector Name | Collections | Total Pause Time |
| :--- | :--- | :--- |
| `Epsilon Heap` | 0 | 0 ms |

### Heap Memory Pools

| Pool Name | Current Usage | Peak Usage |
| :--- | :--- | :--- |
| `Epsilon Heap` | 703,221,824 bytes (670.64 MB) | 703,221,824 bytes (670.64 MB) |

**Overall Status:** No collections observed during this finite workload; allocation unmeasured

