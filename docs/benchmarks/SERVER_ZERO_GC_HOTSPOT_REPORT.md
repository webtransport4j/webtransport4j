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
| **Active Flood Duration** | 12.705 s | - |
| **Aggregate Throughput** | 236,127.4 ops/s | - |
| **GC Collections** | **0** | ✅ **ZERO COLLECTIONS** |
| **Total GC Pause Time** | **0 ms** | ✅ **ZERO PAUSE TIME** |
| **Used Heap at Start** | 190,970,032 bytes (182.12 MB) | Pre-flood baseline |
| **Used Heap at End** | 711,063,728 bytes (678.12 MB) | Post-flood steady-state |
| **Heap Delta** | 520,093,696 bytes | Net change; does not measure total allocation |

### Active Garbage Collectors (JVM MXBeans)

| Collector Name | Collections | Total Pause Time |
| :--- | :--- | :--- |
| `G1 Young Generation` | 0 | 0 ms |
| `G1 Concurrent GC` | 0 | 0 ms |
| `G1 Old Generation` | 0 | 0 ms |

### Heap Memory Pools

| Pool Name | Current Usage | Peak Usage |
| :--- | :--- | :--- |
| `G1 Eden Space` | 702,545,920 bytes (670.00 MB) | 702,545,920 bytes (670.00 MB) |
| `G1 Old Gen` | 9,566,384 bytes (9.12 MB) | 9,566,384 bytes (9.12 MB) |
| `G1 Survivor Space` | 0 bytes (0.00 MB) | 0 bytes (0.00 MB) |

**Overall Status:** No collections observed during this finite workload; allocation unmeasured

