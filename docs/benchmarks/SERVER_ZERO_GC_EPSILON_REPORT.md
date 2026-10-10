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
| **Active Flood Duration** | 10.565 s | - |
| **Aggregate Throughput** | 283,963.3 ops/s | - |
| **GC Collections** | **0** | ✅ **ZERO COLLECTIONS** |
| **Total GC Pause Time** | **0 ms** | ✅ **ZERO PAUSE TIME** |
| **Used Heap at Start** | 187,486,040 bytes (178.80 MB) | Pre-flood baseline |
| **Used Heap at End** | 467,219,144 bytes (445.57 MB) | Post-flood steady-state |
| **Heap Delta** | 279,733,104 bytes | Net change; does not measure total allocation |

### Active Garbage Collectors (JVM MXBeans)

| Collector Name | Collections | Total Pause Time |
| :--- | :--- | :--- |
| `Epsilon Heap` | 0 | 0 ms |

### Heap Memory Pools

| Pool Name | Current Usage | Peak Usage |
| :--- | :--- | :--- |
| `Epsilon Heap` | 471,939,144 bytes (450.08 MB) | 471,939,144 bytes (450.08 MB) |

**Overall Status:** No collections observed during this finite workload; allocation unmeasured

