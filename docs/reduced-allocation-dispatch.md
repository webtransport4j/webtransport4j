# Reduced-allocation dispatch (experimental)

`enableZeroGc(true)` retains its historical API name for compatibility. It selects an
experimental pipeline, not a guarantee of zero allocation or zero collections. It remains off
by default. Frames, owned callback buffers, retained slices, queues, connections, streams, NIO
views and application operations can allocate.

Both modes use `DefaultMessageDispatcher` for Session lookup, handler selection, error handling,
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

## Rejected stream retention

Rejected children receive a protocol shutdown and an explicit local close. Netty 4.2.16 can
retain remotely created stream entries in its QUIC connection registry until peer FIN/RESET,
even after local close. A peer ignoring rejection could otherwise accumulate these entries.
`webtransport4j.webtransport.max.rejected.streams.per.connection` therefore provides a cumulative
rejection budget (default 64, minimum 1). Reaching it closes the QUIC connection with
`H3_EXCESSIVE_LOAD`, releasing its transport registry and all Sessions on that connection.
Ordinary valid streams do not consume this budget. This policy intentionally limits repeated
invalid streams; it is not a process-wide connection or memory limit.
