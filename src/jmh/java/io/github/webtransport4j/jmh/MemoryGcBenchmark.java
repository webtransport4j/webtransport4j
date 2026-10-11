package io.github.webtransport4j.jmh;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import io.github.webtransport4j.internal.handles.RefHandle;
import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.github.webtransport4j.server.FlyweightWebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.invoke.MethodHandles;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH &amp; Standalone Memory and Garbage Collection Benchmark.
 *
 * <p>Measures memory footprint, allocation rate (B/op), and GC pause overhead
 * comparing the WebTransport4J zero-allocation handle abstraction (VarHandle on Java 11+)
 * against boxed Atomic* instances across high-frequency session operations.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(0)
public class MemoryGcBenchmark {

  /**
   * Session representation backed by primitive volatiles and static Handles.
   * Zero per-instance wrapper objects.
   */
  public static class HandleSessionHolder {
    volatile int streamsCreated;
    volatile int activeStreams;
    volatile int sessionState;
    volatile long datagramsSent;
    volatile long datagramsReceived;
    volatile long bytesSent;
    volatile long bytesReceived;
    volatile long smoothedRtt;
    volatile String connectionState = "OPEN";

    public static final IntHandle<HandleSessionHolder> STREAMS_CREATED_HANDLE =
        Handles.newIntHandle(
            HandleSessionHolder.class,
            "streamsCreated",
            MethodHandles.lookup(),
            () ->
                AtomicIntegerFieldUpdater.newUpdater(
                    HandleSessionHolder.class, "streamsCreated"));

    public static final LongHandle<HandleSessionHolder> BYTES_SENT_HANDLE =
        Handles.newLongHandle(
            HandleSessionHolder.class,
            "bytesSent",
            MethodHandles.lookup(),
            () ->
                AtomicLongFieldUpdater.newUpdater(HandleSessionHolder.class, "bytesSent"));

    public static final RefHandle<HandleSessionHolder, String> STATE_HANDLE =
        Handles.newRefHandle(
            HandleSessionHolder.class,
            String.class,
            "connectionState",
            MethodHandles.lookup(),
            () ->
                AtomicReferenceFieldUpdater.newUpdater(
                    HandleSessionHolder.class, String.class, "connectionState"));
  }

  /**
   * Traditional session representation allocating boxed Atomic* objects per session.
   */
  public static class BoxedSessionHolder {
    final AtomicInteger streamsCreated = new AtomicInteger();
    final AtomicInteger activeStreams = new AtomicInteger();
    final AtomicInteger sessionState = new AtomicInteger();
    final AtomicLong datagramsSent = new AtomicLong();
    final AtomicLong datagramsReceived = new AtomicLong();
    final AtomicLong bytesSent = new AtomicLong();
    final AtomicLong bytesReceived = new AtomicLong();
    final AtomicLong smoothedRtt = new AtomicLong();
    final AtomicReference<String> connectionState = new AtomicReference<>("OPEN");
  }

  private HandleSessionHolder handleSession;
  private BoxedSessionHolder boxedSession;
  private ByteBuf rawNettyBuf;
  private DefaultNettyWebTransportBuffer defaultBuffer;
  private FlyweightWebTransportBuffer flyweightBuffer;

  /** Sets up benchmark state. */
  @Setup
  public void setup() {
    handleSession = new HandleSessionHolder();
    boxedSession = new BoxedSessionHolder();
    rawNettyBuf = Unpooled.directBuffer(64);
    rawNettyBuf.writeLong(0x0102030405060708L);
    defaultBuffer = new DefaultNettyWebTransportBuffer(rawNettyBuf.retainedSlice());
    flyweightBuffer = FlyweightWebTransportBuffer.createFlyweight();
    flyweightBuffer.attach(rawNettyBuf);
  }

  /** Tears down benchmark fixtures. */
  @TearDown
  public void tearDown() {
    flyweightBuffer.detach();
    defaultBuffer.release();
    rawNettyBuf.release();
  }

  /** Benchmark for Handle-based integer increment. */
  @Benchmark
  public int testHandleIntIncrement() {
    return HandleSessionHolder.STREAMS_CREATED_HANDLE.incrementAndGet(handleSession);
  }

  /** Benchmark for Boxed AtomicInteger increment. */
  @Benchmark
  public int testBoxedAtomicIntegerIncrement() {
    return boxedSession.streamsCreated.incrementAndGet();
  }

  /** Benchmark for Handle-based long add. */
  @Benchmark
  public long testHandleLongAdd() {
    return HandleSessionHolder.BYTES_SENT_HANDLE.addAndGet(handleSession, 1024L);
  }

  /** Benchmark for Boxed AtomicLong add. */
  @Benchmark
  public long testBoxedAtomicLongAdd() {
    return boxedSession.bytesSent.addAndGet(1024L);
  }

  /** Benchmark for Handle-based reference CAS. */
  @Benchmark
  public boolean testHandleRefCas() {
    String curr = handleSession.connectionState;
    String next = "OPEN".equals(curr) ? "CLOSING" : "OPEN";
    return HandleSessionHolder.STATE_HANDLE.compareAndSet(handleSession, curr, next);
  }

  /** Benchmark for Boxed AtomicReference CAS. */
  @Benchmark
  public boolean testBoxedAtomicRefCas() {
    String curr = boxedSession.connectionState.get();
    String next = "OPEN".equals(curr) ? "CLOSING" : "OPEN";
    return boxedSession.connectionState.compareAndSet(curr, next);
  }

  /** Benchmark for DefaultNettyWebTransportBuffer reference counting. */
  @Benchmark
  public WebTransportBuffer testBufferRetainRelease() {
    defaultBuffer.retain();
    defaultBuffer.release();
    return defaultBuffer;
  }

  /** Benchmark for FlyweightWebTransportBuffer primitive read without allocation. */
  @Benchmark
  public byte testFlyweightBufferRead() {
    return flyweightBuffer.getByte(0);
  }

  /**
   * Main entry point to run memory and GC diagnostics.
   *
   * @param args CLI arguments ("--jmh" runs JMH with GCProfiler)
   * @throws Exception if benchmarking fails
   */
  public static void main(String[] args) throws Exception {
    boolean runJmh = false;
    for (String arg : args) {
      if ("--jmh".equals(arg)) {
        runJmh = true;
        break;
      }
    }

    if (runJmh) {
      runJmhWithGcProfiler();
    } else {
      runStandaloneMemoryGcDiagnostics();
    }
  }

  private static void runJmhWithGcProfiler() throws Exception {
    Options opt =
        new OptionsBuilder()
            .include(MemoryGcBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .forks(0)
            .warmupIterations(1)
            .measurementIterations(2)
            .build();
    new Runner(opt).run();
  }

  /**
   * Executes high-precision standalone memory and garbage collection diagnostics.
   */
  public static void runStandaloneMemoryGcDiagnostics() {
    System.out.println("================================================================================");
    System.out.println("   WebTransport4J - Memory & Garbage Collection (GC) Benchmark Report          ");
    System.out.println("================================================================================");
    System.out.println("Runtime: " + System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
    System.out.println("Arch:    " + System.getProperty("os.arch") + " (" + System.getProperty("os.name") + ")");
    System.out.println();

    // 1. Thread Allocation Profiling
    System.out.println("--- 1. STEADY-STATE PER-OPERATION HEAP ALLOCATION (B/op) ---");
    System.out.println("Measuring thread-allocated bytes across 10,000,000 operations per scenario...");
    System.out.println();

    HandleSessionHolder handleSession = new HandleSessionHolder();
    BoxedSessionHolder boxedSession = new BoxedSessionHolder();
    ByteBuf rawBuf = Unpooled.directBuffer(64);
    rawBuf.writeLong(0x12345678L);
    DefaultNettyWebTransportBuffer defaultBuf = new DefaultNettyWebTransportBuffer(rawBuf.retainedSlice());
    FlyweightWebTransportBuffer flyweight = FlyweightWebTransportBuffer.createFlyweight();
    flyweight.attach(rawBuf);

    // Warmup
    for (int i = 0; i < 500_000; i++) {
      HandleSessionHolder.STREAMS_CREATED_HANDLE.incrementAndGet(handleSession);
      boxedSession.streamsCreated.incrementAndGet();
      HandleSessionHolder.BYTES_SENT_HANDLE.addAndGet(handleSession, 64L);
      boxedSession.bytesSent.addAndGet(64L);
      defaultBuf.retain();
      defaultBuf.release();
    }

    final int rounds = 4;
    final int opsPerRound = 2_500_000;
    final int totalIterations = rounds * opsPerRound;

    long handleIntBytes = 0;
    long boxedIntBytes = 0;
    long handleLongBytes = 0;
    long boxedLongBytes = 0;
    long bufferRetainReleaseBytes = 0;
    long flyweightReadBytes = 0;

    List<String> roundRecords = new ArrayList<>();

    for (int r = 0; r < rounds; r++) {
      boolean handleFirst = (r % 2 == 0);
      long roundHandleInt;
      long roundBoxedInt;
      long roundHandleLong;
      long roundBoxedLong;

      if (handleFirst) {
        roundHandleInt = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            HandleSessionHolder.STREAMS_CREATED_HANDLE.incrementAndGet(handleSession);
          }
        });
        roundBoxedInt = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            boxedSession.streamsCreated.incrementAndGet();
          }
        });
        roundHandleLong = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            HandleSessionHolder.BYTES_SENT_HANDLE.addAndGet(handleSession, 128L);
          }
        });
        roundBoxedLong = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            boxedSession.bytesSent.addAndGet(128L);
          }
        });
      } else {
        roundBoxedInt = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            boxedSession.streamsCreated.incrementAndGet();
          }
        });
        roundHandleInt = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            HandleSessionHolder.STREAMS_CREATED_HANDLE.incrementAndGet(handleSession);
          }
        });
        roundBoxedLong = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            boxedSession.bytesSent.addAndGet(128L);
          }
        });
        roundHandleLong = measureAllocatedBytes(() -> {
          for (int i = 0; i < opsPerRound; i++) {
            HandleSessionHolder.BYTES_SENT_HANDLE.addAndGet(handleSession, 128L);
          }
        });
      }

      final long roundBufferBytes = measureAllocatedBytes(() -> {
        for (int i = 0; i < opsPerRound; i++) {
          defaultBuf.retain();
          defaultBuf.release();
        }
      });

      final long roundFlyweightBytes = measureAllocatedBytes(() -> {
        for (int i = 0; i < opsPerRound; i++) {
          flyweight.getByte(0);
        }
      });

      handleIntBytes += roundHandleInt;
      boxedIntBytes += roundBoxedInt;
      handleLongBytes += roundHandleLong;
      boxedLongBytes += roundBoxedLong;
      bufferRetainReleaseBytes += roundBufferBytes;
      flyweightReadBytes += roundFlyweightBytes;

      roundRecords.add(
          String.format(
              "Round %d [%s]: handleInt=%,d B, boxedInt=%,d B, handleLong=%,d B, boxedLong=%,d B",
              r + 1,
              handleFirst ? "Handle -> Boxed" : "Boxed -> Handle",
              roundHandleInt,
              roundBoxedInt,
              roundHandleLong,
              roundBoxedLong));
    }

    System.out.println("Execution order & per-round allocations (alternating to reduce order bias):");
    for (String record : roundRecords) {
      System.out.println("  " + record);
    }
    System.out.println();

    System.out.printf("%-40s | %15s | %15s%n", "Operation / Component", "Total Allocated", "Bytes / Op (B/op)");
    System.out.println("--------------------------------------------------------------------------------");
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "Handle Int Increment (VarHandle)", handleIntBytes / 1024, (double) handleIntBytes / totalIterations);
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "Boxed AtomicInteger Increment", boxedIntBytes / 1024, (double) boxedIntBytes / totalIterations);
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "Handle Long Add (VarHandle)", handleLongBytes / 1024, (double) handleLongBytes / totalIterations);
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "Boxed AtomicLong Add", boxedLongBytes / 1024, (double) boxedLongBytes / totalIterations);
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "Buffer Retain/Release (Handles refCnt)",
        bufferRetainReleaseBytes / 1024,
        (double) bufferRetainReleaseBytes / totalIterations);
    System.out.printf("%-40s | %12d KB | %15.2f B/op%n",
        "FlyweightBuffer Direct Read", flyweightReadBytes / 1024, (double) flyweightReadBytes / totalIterations);
    System.out.println();

    // 2. Retained Heap Footprint at Scale (50,000 sessions)
    System.out.println("--- 2. RETAINED HEAP FOOTPRINT & OBJECT COUNT AT SCALE ---");
    final int sessionCount = 50_000;
    System.out.printf("Instantiating %d concurrent sessions under both architectures...%n", sessionCount);

    forceGc();
    long heapBeforeHandles = getUsedHeapMemory();
    List<HandleSessionHolder> handleSessions = new ArrayList<>(sessionCount);
    for (int i = 0; i < sessionCount; i++) {
      handleSessions.add(new HandleSessionHolder());
    }
    forceGc();
    long heapAfterHandles = getUsedHeapMemory();
    final long handleDeltaBytes = Math.max(0, heapAfterHandles - heapBeforeHandles);

    // Clear handle sessions before boxed test
    handleSessions.clear();
    handleSessions = null;
    forceGc();

    long heapBeforeBoxed = getUsedHeapMemory();
    List<BoxedSessionHolder> boxedSessions = new ArrayList<>(sessionCount);
    for (int i = 0; i < sessionCount; i++) {
      boxedSessions.add(new BoxedSessionHolder());
    }
    forceGc();
    long heapAfterBoxed = getUsedHeapMemory();
    final long boxedDeltaBytes = Math.max(0, heapAfterBoxed - heapBeforeBoxed);

    boxedSessions.clear();
    boxedSessions = null;
    forceGc();

    long handleObjectsPerSession = 1; // Only the session instance itself
    long boxedObjectsPerSession = 10; // Session + 3 AtomicInts + 5 AtomicLongs + 1 AtomicRef

    System.out.println();
    System.out.printf("%-35s | %15s | %15s | %18s%n",
        "Architecture", "Objects Created", "Heap Consumed", "Bytes / Session");
    System.out.println("-----------------------------------------------------------------------------------------");
    System.out.printf("%-35s | %15d | %12.2f MB | %15.1f B%n",
        "WebTransport4J (Handles/VarHandle)",
        sessionCount * handleObjectsPerSession,
        (double) handleDeltaBytes / (1024 * 1024),
        (double) handleDeltaBytes / sessionCount);
    System.out.printf("%-35s | %15d | %12.2f MB | %15.1f B%n",
        "Traditional Boxed Atomics",
        sessionCount * boxedObjectsPerSession,
        (double) boxedDeltaBytes / (1024 * 1024),
        (double) boxedDeltaBytes / sessionCount);

    double memorySavingsPercent = boxedDeltaBytes > 0
        ? (1.0 - ((double) handleDeltaBytes / boxedDeltaBytes)) * 100.0
        : 0.0;
    System.out.printf("%n--> Memory Footprint Reduction: %.1f%% fewer bytes on heap.%n", memorySavingsPercent);
    System.out.printf("--> Object Allocation Reduction: 90.0%% fewer objects tracked by Garbage Collector.%n%n");

    // 3. Garbage Collection Pressure
    System.out.println("--- 3. GARBAGE COLLECTION RUNS & PAUSE TIME ---");
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      System.out.printf("Collector: %-25s | Count: %5d | Total Time: %5d ms%n",
          gcBean.getName(), gcBean.getCollectionCount(), gcBean.getCollectionTime());
    }
    System.out.println();
    System.out.println("================================================================================");
    System.out.println("   Benchmark Completed Successfully                                             ");
    System.out.println("================================================================================");

    flyweight.detach();
    defaultBuf.release();
    rawBuf.release();
  }

  private static long measureAllocatedBytes(Runnable task) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    long threadId = Thread.currentThread().getId();
    if (threadBean instanceof com.sun.management.ThreadMXBean) {
      com.sun.management.ThreadMXBean sunBean = (com.sun.management.ThreadMXBean) threadBean;
      if (sunBean.isThreadAllocatedMemorySupported()) {
        if (!sunBean.isThreadAllocatedMemoryEnabled()) {
          sunBean.setThreadAllocatedMemoryEnabled(true);
        }
        long start = sunBean.getThreadAllocatedBytes(threadId);
        task.run();
        long end = sunBean.getThreadAllocatedBytes(threadId);
        return Math.max(0, end - start);
      }
    }
    System.err.println(
        "WARNING: Thread allocated memory tracking not supported; falling back to heap delta estimate");
    long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    task.run();
    long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    return Math.max(0, memAfter - memBefore);
  }

  private static long getUsedHeapMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private static void forceGc() {
    for (int i = 0; i < 4; i++) {
      System.gc();
      try {
        Thread.sleep(50);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
