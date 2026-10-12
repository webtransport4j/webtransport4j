# Reduced-Allocation Flyweight Dispatch

`DefaultMessageDispatcher` dispatches inbound stream and datagram payloads through thread-confined
flyweight buffers unconditionally by default. Frames, owned callback buffers, retained slices,
queues, connections, streams, NIO views and application operations can allocate.

Inbound dispatch uses `DefaultMessageDispatcher` for Session lookup, handler selection, error handling,
and metrics. Stream admission and byte accounting remain upstream. Dispatch occurs directly
on Netty's `EventLoop`. Callbacks run on the network event loop and must not block; applications
requiring heavy or blocking computations offload work in their `WebTransportHandler` by retaining
the buffer (`buffer.retain()`) and executing tasks on their own worker executor or virtual threads.

Callbacks receive independent reference-counted buffers. `buffer.retain()` returns the same
object. Retain before transferring it to asynchronous work and release that reference on every
completion, cancellation and failure path. The dispatcher releases its reference after the
callback. Borrowed NIO views must not outlive an owned reference. Nested dispatch uses independent
callback objects. The dispatcher does not recycle callback identities.

`FlyweightWebTransportBuffer` remains a compatibility utility. An attached borrowed view may be
promoted by `retain()` into the same independently owned object. Promotion prevents recycling;
detach does not release retained ownership. Prefer `DefaultNettyWebTransportBuffer` for owned
buffers. Borrowed utility instances remain thread-confined.

Before enabling this pipeline in production:

- Compare both modes with identical JVM, payload, executor and traffic settings. Measure allocated
  bytes per operation with an allocation profiler, alongside throughput and p99 latency.
- Soak test sustained mixed traffic and connection/stream churn with constrained heap and direct
  memory. Include slow consumers, rejected executor work, queue saturation and abrupt disconnects.
- Verify payload correctness, bounded queues, recovered memory after traffic stops, no Netty leak
  reports and independent health Session availability throughout the run.
- Repeat the reported over-quota attack against the fixed server; confirm children are rejected
  and Session cleanup releases admitted children.

The benchmark script reports finite-window GC observations. Heap delta is not allocation rate;
Epsilon completion only establishes that the measured workload fit the available memory. These
checks do not replace allocation profiling or long-duration validation.

## Inbound Data Dispatching: Standard vs. Raw Netty Consumers

`webtransport4j` offers two complementary programming models for incoming stream chunks and datagrams:

### 1. Standard API (`WebTransportBuffer`)
* **Streams**: `stream.onData(Consumer<WebTransportBuffer>)`
* **Datagrams**: `WebTransportHandler#onDatagramReceived(WebTransportSession, WebTransportBuffer)`
* **Description**: Transport-neutral abstraction. Decouples application logic from Netty internals. Safe, zero-copy readable views backed by Netty `ByteBuf`.

### 2. Power-User Raw API (`ByteBuf`)
* **Streams**: `((NettyWebTransportStream) stream).onRawByteBuf(Consumer<ByteBuf>)`
* **Datagrams**: `((NettyWebTransportSession) session).onRawDatagram(Consumer<ByteBuf>)`
* **Description**: Direct access to Netty's raw direct `ByteBuf` on the event loop with **zero wrapper object allocation**. Designed for ultra-low-latency workloads (e.g. game servers, media relays).

### Mutual Exclusivity Design

Each stream or datagram channel permits **either** a standard consumer or a raw consumer, but **never both**. Registering both on the same stream throws `IllegalStateException`.

This is an intentional design choice for several key reasons:

1. **Inbound Buffer Mutation & Reader Index Integrity**:
   Netty allocates a single direct buffer for each incoming network frame. When a consumer reads data (e.g. `readByte()`, decoding payload), it mutates the buffer's `readerIndex`. Calling two consumers in sequence would cause the second consumer to receive an empty or partially consumed buffer (`readableBytes() == 0`).
2. **Deterministic Ownership & Leak Prevention**:
   The dispatcher automatically returns the pooled `ByteBuf` to Netty's allocator once the callback returns:
   ```java
   try {
     consumer.accept(content);
   } finally {
     content.release(); // refCnt drops to 0, recycled to pool
   }
   ```
   Single-consumer ownership guarantees deterministic deallocation without reference-counting races or leaks.
3. **Preserving Zero-GC Invariants**:
   If the dispatcher simultaneously fed standard consumers, it would be forced to allocate `WebTransportBuffer` wrappers for every packet, undermining the zero-allocation guarantee of the raw API.

### Fanning Out Multiple Listeners

If an application requires multiple operations (e.g., auditing, metrics inspection, and message decoding), handle fan-out inside a single consumer:

```java
nettyStream.onRawByteBuf(buf -> {
    // 1. Inspect metrics without modifying readerIndex
    recordThroughput(buf.readableBytes());

    // 2. Decode or dispatch business payload
    processDirect(buf);
});
```

## Rejected stream retention

Rejected children receive a protocol shutdown and an explicit local close. Netty 4.2.16 can
retain remotely created stream entries in its QUIC connection registry until peer FIN/RESET,
even after local close. A peer ignoring rejection could otherwise accumulate these entries.
`webtransport4j.webtransport.max.rejected.streams.per.connection` therefore provides a cumulative
rejection budget (default 64, minimum 1). Reaching it closes the QUIC connection with
`H3_EXCESSIVE_LOAD`, releasing its transport registry and all Sessions on that connection.
Ordinary valid streams do not consume this budget. This policy intentionally limits repeated
invalid streams; it is not a process-wide connection or memory limit.
