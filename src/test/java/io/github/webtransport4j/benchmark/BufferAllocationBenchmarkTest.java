package io.github.webtransport4j.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark comparing standalone wrapper allocation vs. FastThreadLocal mutable buffer reuse.
 * Evaluates throughput, JIT escape analysis, multi-threaded scaling, and async data safety.
 */
public class BufferAllocationBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(BufferAllocationBenchmarkTest.class);

  private static final class MutableTestBuffer implements WebTransportBuffer {
    private ByteBuf delegate;

    MutableTestBuffer(ByteBuf delegate) {
      this.delegate = delegate;
    }

    MutableTestBuffer wrap(ByteBuf delegate) {
      this.delegate = delegate;
      return this;
    }

    @Override
    public int readableBytes() {
      return delegate.readableBytes();
    }

    @Override
    public java.nio.ByteBuffer nioBuffer() {
      return delegate.nioBuffer();
    }

    @Override
    public java.nio.ByteBuffer skipBytes(int bytes) {
      return delegate.skipBytes(bytes).nioBuffer();
    }

    @Override
    public byte[] readBytes() {
      byte[] bytes = new byte[delegate.readableBytes()];
      delegate.readBytes(bytes);
      return bytes;
    }

    @Override
    public WebTransportBuffer retain() {
      delegate.retain();
      return this;
    }

    @Override
    public void release() {
      if (delegate != null && delegate.refCnt() > 0) {
        delegate.release();
      }
    }

    @Override
    public void close() {
      release();
    }
  }

  private static final FastThreadLocal<MutableTestBuffer> REUSABLE_BUFFER =
      new FastThreadLocal<MutableTestBuffer>() {
        @Override
        protected MutableTestBuffer initialValue() {
          return new MutableTestBuffer(Unpooled.EMPTY_BUFFER);
        }
      };

  private static final int WARMUP_ITERATIONS = 200_000;
  private static final int BENCHMARK_ITERATIONS = 5_000_000;
  private static final int CONCURRENT_THREADS = 8;
  private static final int CONCURRENT_ITERATIONS_PER_THREAD = 500_000;

  /**
   * Benchmarks single-threaded throughput and nanoseconds per operation after JIT warmup.
   */
  @Test
  public void testSingleThreadedThroughput() {
    byte[] payload = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    ByteBuf buf = Unpooled.wrappedBuffer(payload);

    try {
      // 1. Warm up JIT for both paths
      warmup(buf);

      // 2. Measure FastThreadLocal.wrap()
      final long ftlStart = System.nanoTime();
      final long ftlBlackhole = runFastThreadLocalBenchmark(buf, BENCHMARK_ITERATIONS);
      final long ftlDurationNs = System.nanoTime() - ftlStart;
      assertTrue(ftlBlackhole > 0);

      // 3. Measure new DefaultNettyWebTransportBuffer()
      final long newStart = System.nanoTime();
      final long newBlackhole = runNewAllocationBenchmark(buf, BENCHMARK_ITERATIONS);
      final long newDurationNs = System.nanoTime() - newStart;
      assertTrue(newBlackhole > 0);

      final double ftlNsPerOp = (double) ftlDurationNs / BENCHMARK_ITERATIONS;
      final double newNsPerOp = (double) newDurationNs / BENCHMARK_ITERATIONS;
      final double ftlOpsPerSec = (BENCHMARK_ITERATIONS / (ftlDurationNs / 1_000_000_000.0));
      final double newOpsPerSec = (BENCHMARK_ITERATIONS / (newDurationNs / 1_000_000_000.0));

      logger.info("================================================================================");
      logger.info("⚡ SINGLE-THREADED BUFFER BENCHMARK ({} iterations)", BENCHMARK_ITERATIONS);
      logger.info("================================================================================");
      logger.info(
          "FastThreadLocal.wrap():     {} ns/op | {} ops/sec",
          String.format("%.2f", ftlNsPerOp),
          String.format("%,.0f", ftlOpsPerSec));
      logger.info(
          "new DefaultNettyBuffer():   {} ns/op | {} ops/sec",
          String.format("%.2f", newNsPerOp),
          String.format("%,.0f", newOpsPerSec));
      logger.info("================================================================================");

      // Both approaches should achieve sub-50 nanosecond operations in JIT
      assertTrue(newNsPerOp < 50.0);
    } finally {
      buf.release();
    }
  }

  /**
   * Benchmarks multi-threaded throughput and contention under concurrent execution.
   */
  @Test
  public void testMultiThreadedThroughput() throws Exception {
    byte[] payload = new byte[] {0x10, 0x20, 0x30, 0x40};
    ByteBuf buf = Unpooled.wrappedBuffer(payload);

    try {
      ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_THREADS);
      CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
      AtomicLong totalBytesRead = new AtomicLong();

      long startTime = System.nanoTime();
      for (int t = 0; t < CONCURRENT_THREADS; t++) {
        pool.submit(() -> {
          try {
            long localBytes = 0;
            for (int i = 0; i < CONCURRENT_ITERATIONS_PER_THREAD; i++) {
              WebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(buf);
              localBytes += buffer.readableBytes();
            }
            totalBytesRead.addAndGet(localBytes);
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(30, TimeUnit.SECONDS));
      long durationNs = System.nanoTime() - startTime;
      pool.shutdownNow();

      long totalOps = (long) CONCURRENT_THREADS * CONCURRENT_ITERATIONS_PER_THREAD;
      double totalOpsPerSec = totalOps / (durationNs / 1_000_000_000.0);
      double nsPerOp = (double) durationNs / totalOps;

      logger.info("================================================================================");
      logger.info("🚀 MULTI-THREADED ({} threads x {} ops = {} total ops)",
          CONCURRENT_THREADS, CONCURRENT_ITERATIONS_PER_THREAD, totalOps);
      logger.info("Throughput:                 {} ops/sec", String.format("%,.0f", totalOpsPerSec));
      logger.info("Average Latency per Op:     {} ns/op", String.format("%.2f", nsPerOp));
      logger.info("================================================================================");

      assertEquals(totalOps * payload.length, totalBytesRead.get());
    } finally {
      buf.release();
    }
  }

  /**
   * Demonstrates the safety advantage: verifies that asynchronous consumers do not suffer
   * data corruption when using standalone immutable wrappers vs mutable FastThreadLocal.
   */
  @Test
  public void testAsyncDataSafety() throws Exception {
    int asyncTasks = 10_000;
    ExecutorService consumerPool = Executors.newFixedThreadPool(4);
    CountDownLatch latch = new CountDownLatch(asyncTasks);
    AtomicInteger corruptedCount = new AtomicInteger();

    for (int i = 0; i < asyncTasks; i++) {
      final int messageId = i;
      ByteBuf msgBuf = Unpooled.copyInt(messageId);
      // Create independent wrapper
      WebTransportBuffer safeBuffer = new DefaultNettyWebTransportBuffer(msgBuf);

      consumerPool.submit(() -> {
        try {
          // Simulate non-zero async processing delay
          Thread.yield();
          int readVal = safeBuffer.nioBuffer().getInt(0);
          if (readVal != messageId) {
            corruptedCount.incrementAndGet();
          }
        } finally {
          msgBuf.release();
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    consumerPool.shutdownNow();

    logger.info("================================================================================");
    logger.info("🛡️ ASYNC DATA INTEGRITY TEST ({} tasks)", asyncTasks);
    logger.info("Corrupted messages with standalone wrapper: {}", corruptedCount.get());
    logger.info("================================================================================");

    assertEquals(0, corruptedCount.get());
  }

  /**
   * Demonstrates the hazard: when asynchronous tasks read a shared, mutable thread-local
   * buffer whose delegate is overwritten by subsequent loop iterations, massive data corruption occurs.
   */
  @Test
  public void testDemonstrateFastThreadLocalCorruptionInAsync() throws Exception {
    int asyncTasks = 10_000;
    ExecutorService consumerPool = Executors.newFixedThreadPool(4);
    CountDownLatch latch = new CountDownLatch(asyncTasks);
    AtomicInteger corruptedCount = new AtomicInteger();

    for (int i = 0; i < asyncTasks; i++) {
      final int messageId = i;
      ByteBuf msgBuf = Unpooled.copyInt(messageId);
      // Reusing the mutable thread-local buffer in the dispatch loop:
      WebTransportBuffer mutableBuffer = REUSABLE_BUFFER.get().wrap(msgBuf);

      consumerPool.submit(() -> {
        try {
          Thread.yield();
          int readVal = mutableBuffer.nioBuffer().getInt(0);
          if (readVal != messageId) {
            corruptedCount.incrementAndGet();
          }
        } catch (Exception e) {
          corruptedCount.incrementAndGet();
        } finally {
          msgBuf.release();
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    consumerPool.shutdownNow();

    logger.info("================================================================================");
    logger.info("⚠️ FASTTHREADLOCAL MUTABLE BUFFER HAZARD ({} tasks)", asyncTasks);
    logger.info("Corrupted messages when reusing mutable buffer: {} / {}", corruptedCount.get(), asyncTasks);
    logger.info("================================================================================");

    assertTrue(
        "Demonstrates that mutable buffer reuse corrupts async consumers: " + corruptedCount.get(),
        corruptedCount.get() > 0);
  }

  private static void warmup(ByteBuf buf) {
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      MutableTestBuffer b1 = REUSABLE_BUFFER.get().wrap(buf);
      int r1 = b1.readableBytes();
      DefaultNettyWebTransportBuffer b2 = new DefaultNettyWebTransportBuffer(buf);
      int r2 = b2.readableBytes();
      if (r1 != r2) {
        throw new IllegalStateException();
      }
    }
  }

  private static long runFastThreadLocalBenchmark(ByteBuf buf, int iterations) {
    long sum = 0;
    for (int i = 0; i < iterations; i++) {
      MutableTestBuffer b = REUSABLE_BUFFER.get().wrap(buf);
      sum += b.readableBytes();
    }
    return sum;
  }

  private static long runNewAllocationBenchmark(ByteBuf buf, int iterations) {
    long sum = 0;
    for (int i = 0; i < iterations; i++) {
      DefaultNettyWebTransportBuffer b = new DefaultNettyWebTransportBuffer(buf);
      sum += b.readableBytes();
    }
    return sum;
  }

  /**
   * Main entry point to run the benchmark directly from the CLI.
   */
  public static void main(String[] args) throws Exception {
    BufferAllocationBenchmarkTest test = new BufferAllocationBenchmarkTest();
    System.out.println("Running JIT Warmup & Single-Threaded Benchmark...");
    test.testSingleThreadedThroughput();
    System.out.println("Running Multi-Threaded Scalability Benchmark...");
    test.testMultiThreadedThroughput();
    System.out.println("Running Async Data Safety Verification...");
    test.testAsyncDataSafety();
  }
}
