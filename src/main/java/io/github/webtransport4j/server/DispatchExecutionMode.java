package io.github.webtransport4j.server;

public enum DispatchExecutionMode {
    /**
     * Execute callback methods directly on the Netty event loop thread.
     * ✅ Best performance (no thread switching)
     * ✅ Simple to reason about thread-safety
     * ❌ Handler must not block (must be very fast)
     * ❌ No CPU-intensive operations
     */
    NETTY_EVENT_LOOP,

    /**
     * Use a fixed-size thread pool for all handlers.
     * ✅ Predictable resource usage
     * ✅ Prevents thread explosion
     * ❌ Handlers wait if pool is full
     * ❌ Can cause latency spikes
     * ❌ Requires tuning pool size
     */
    FIXED_THREAD_POOL,

    /**
     * Use virtual threads (Java 21+).
     * ✅ Nearly as fast as event loop
     * ✅ Handles blocking operations without blocking OS threads
     * ✅ Low resource usage
     * ✅ Simple programming model
     * ❌ Requires Java 21+
     * ❌ Might have slightly higher overhead than event loop for very short tasks
     */
    VIRTUAL_THREADS,

    /**
     * Use Netty's DefaultEventExecutorGroup for handlers.
     * ✅ Strict sequential ordering of events per connection
     * ✅ Uses FastThreadLocalThread for optimized memory allocation
     * ✅ Shields native QUIC event loop from blocking logic
     * ❌ Fixed thread pool size can lead to starvation if tasks block
     */
    NETTY_EVENT_EXECUTOR_GROUP
}