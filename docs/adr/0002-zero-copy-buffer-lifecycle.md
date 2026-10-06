# ADR 0002: Zero-Copy Buffer Lifecycle & Reference Counting

## Status
Accepted

## Context
High-rate streaming and datagram applications processing 100k+ packets per second create severe garbage collection pressure if buffers are copied into JVM heap arrays. Netty manages off-heap direct memory via reference counting (`ByteBuf.retain()` and `ByteBuf.release()`), which requires strict ownership discipline to prevent memory leaks and use-after-free faults.

## Decision
We enforce a strict single-ownership transfer model across the framework:
1. **Inbound Streams & Datagrams**:
   - Inbound payloads preserve the allocation type of their underlying `ByteBuf`; direct buffers keep payload contents off heap.
   - For datagrams, `DatagramMailbox` increments the reference count (`retain()`) upon acceptance into the queue and releases it (`release()`) after dispatch to the worker thread.
   - A datagram rejected before enqueue remains caller-owned. If a high-priority arrival evicts an already-enqueued normal-priority frame, the mailbox releases only the reference it retained for that queued frame; any caller that still holds the original reference remains responsible for releasing it.
2. **Buffer Interface Abstraction**:
   - Application handlers receive `WebTransportBuffer`, wrapping underlying `ByteBuf` or JDK 22+ `MemorySegment` (via Multi-Release JAR).
   - Calling `buffer.release()` decrements the underlying count and returns memory to the Netty pooled allocator.

## Consequences

### Positive
- **Zero Heap Copying**: Direct off-heap buffers pass straight from network socket to application logic.
- **Reduced Heap-Allocation Pressure**: Direct buffers can avoid heap copies, but wrappers, queue nodes and application processing may still allocate heap objects and trigger GC.
- **Auditable via Leak Detection**: Fully compatible with `-Dio.netty.leakDetection.level=PARANOID`.

### Negative / Trade-offs
- Handler implementations must be careful to consume or release buffers, or delegate them to a reactive sink.
