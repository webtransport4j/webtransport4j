# ADR 0002: Zero-Copy Buffer Lifecycle & Reference Counting

## Status
Accepted

## Context
High-rate streaming and datagram applications processing 100k+ packets per second create severe garbage collection pressure if buffers are copied into JVM heap arrays. Netty manages off-heap direct memory via reference counting (`ByteBuf.retain()` and `ByteBuf.release()`), which requires strict ownership discipline to prevent memory leaks and use-after-free faults.

## Decision
We enforce a strict single-ownership transfer model across the framework:
1. **Inbound Streams & Datagrams**:
   - Inbound payloads preserve the allocation type of their underlying `ByteBuf`; direct buffers keep payload contents off heap.
   - Inbound payloads are dispatched directly to the handler on Netty's EventLoop without mailbox queue allocations. The dispatcher wraps a retained slice in `DefaultNettyWebTransportBuffer` and releases its reference upon handler callback completion. Applications that offload processing retain ownership (`buffer.retain()`) and release when finished.
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
