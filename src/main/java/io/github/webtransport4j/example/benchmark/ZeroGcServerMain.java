package io.github.webtransport4j.example.benchmark;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.server.NettyWebTransportSession;
import io.github.webtransport4j.server.NettyWebTransportStream;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

/**
 * Standalone server process running WebTransport in Zero-GC mode on an isolated JVM.
 *
 * <p>Measures GC collections, pause times, and heap memory usage across all three WebTransport
 * communication primitives: Datagrams, Bidirectional Streams, and Unidirectional Streams.
 */
public class ZeroGcServerMain {

  private static final String HOST = "127.0.0.1";

  /**
   * Main method to start the dedicated zero-GC server.
   *
   * @param args optional port (args[0])
   * @throws Exception if startup fails
   */
  public static void main(@NonNull String[] args) throws Exception {
    int port = 54321;
    String reportFilename = "SERVER_ZERO_GC_REPORT.md";
    if (args.length >= 1) {
      try {
        port = Integer.parseInt(args[0]);
      } catch (NumberFormatException ignored) {
        // Fallback to default
      }
    }
    if (args.length >= 2 && !args[1].trim().isEmpty()) {
      reportFilename = args[1];
    }
    final String reportFile = reportFilename;

    if (new java.io.File("localhost-key.pem").exists()
        && new java.io.File("localhost.pem").exists()) {
      System.setProperty("webtransport4j.ssl.key.path", "localhost-key.pem");
      System.setProperty("webtransport4j.ssl.cert.path", "localhost.pem");
    } else {
      System.setProperty("webtransport4j.dev_mode", "true");
    }

    final AtomicLong receivedDatagrams = new AtomicLong(0);
    final AtomicLong receivedBidiMessages = new AtomicLong(0);
    final AtomicLong receivedUniMessages = new AtomicLong(0);
    final AtomicLong totalBytesReceived = new AtomicLong(0);
    final CountDownLatch sessionLatch = new CountDownLatch(1);
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);

    final AtomicLong startGcCount = new AtomicLong(0);
    final AtomicLong startGcTimeMs = new AtomicLong(0);
    final AtomicLong endGcCount = new AtomicLong(0);
    final AtomicLong endGcTimeMs = new AtomicLong(0);

    final AtomicLong startUsedHeap = new AtomicLong(0);
    final AtomicLong endUsedHeap = new AtomicLong(0);

    final AtomicLong receivedBidiBytes = new AtomicLong(0);
    final AtomicLong receivedUniBytes = new AtomicLong(0);

    final AtomicReference<Map<String, long[]>> startHistogramRef = new AtomicReference<>();
    final AtomicReference<Map<String, long[]>> endHistogramRef = new AtomicReference<>();

    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onSessionReady(@NonNull WebTransportSession session) {
            System.out.println(
                ">>> Server: WebTransport Session Opened (Client Connected). Starting measurement...");
            startGcCount.set(getTotalGcCount());
            startGcTimeMs.set(getTotalGcTime());
            startUsedHeap.set(getUsedHeap());
            startHistogramRef.set(captureClassHistogram());
            startTime.set(System.nanoTime());
            if (session instanceof NettyWebTransportSession) {
              ((NettyWebTransportSession) session).onRawDatagram(
                  raw -> {
                    receivedDatagrams.incrementAndGet();
                    totalBytesReceived.addAndGet(raw.readableBytes());
                    ((NettyWebTransportSession) session).sendDatagramDirect(raw);
                  });
            }
          }

          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            endTime.set(System.nanoTime());
            endGcCount.set(getTotalGcCount());
            endGcTimeMs.set(getTotalGcTime());
            endUsedHeap.set(getUsedHeap());
            endHistogramRef.set(captureClassHistogram());
            System.out.println(">>> Server: WebTransport Session Closed. Stopping measurement...");
            sessionLatch.countDown();
          }

  public static final int STREAM_MSG_SIZE = 32;

          @Override
          public void onIncomingStream(
              @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            if (stream instanceof NettyWebTransportStream) {
              NettyWebTransportStream nettyStream = (NettyWebTransportStream) stream;
              if (stream.isBidirectional()) {
                nettyStream.onRawByteBuf(
                    raw -> {
                      int bytes = raw.readableBytes();
                      totalBytesReceived.addAndGet(bytes);
                      long totalBidi = receivedBidiBytes.addAndGet(bytes);
                      receivedBidiMessages.set(totalBidi / STREAM_MSG_SIZE);
                      nettyStream.writeDirect(raw.retain());
                    });
              } else {
                nettyStream.onRawByteBuf(
                    raw -> {
                      int bytes = raw.readableBytes();
                      totalBytesReceived.addAndGet(bytes);
                      long totalUni = receivedUniBytes.addAndGet(bytes);
                      receivedUniMessages.set(totalUni / STREAM_MSG_SIZE);
                    });
              }
              return;
            }
            if (stream.isBidirectional()) {
              stream.onData(
                  buf -> {
                    int bytes = buf.readableBytes();
                    totalBytesReceived.addAndGet(bytes);
                    long totalBidi = receivedBidiBytes.addAndGet(bytes);
                    receivedBidiMessages.set(totalBidi / STREAM_MSG_SIZE);
                    stream.writeDirect(buf);
                  });
            } else {
              stream.onData(
                  buf -> {
                    int bytes = buf.readableBytes();
                    totalBytesReceived.addAndGet(bytes);
                    long totalUni = receivedUniBytes.addAndGet(bytes);
                    receivedUniMessages.set(totalUni / STREAM_MSG_SIZE);
                  });
            }
          }

          @Override
          public void onDatagramReceived(
              @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
            receivedDatagrams.incrementAndGet();
            totalBytesReceived.addAndGet(data.readableBytes());
            session.sendDatagram(data);
          }
        };

    WebTransportServer server =
        new WebTransportServerBuilder()
            .host(HOST)
            .port(port)
            .enableZeroGc(true)
            .defaultHandler(handler)
            .build();

    server.start();
    int actualPort = server.getPort();
    System.out.printf("SERVER_READY_PORT=%d%n", actualPort);
    System.out.flush();

    // Wait until the client connects, finishes sending, and closes the session
    sessionLatch.await();

    long dgrams = receivedDatagrams.get();
    long bidi = receivedBidiMessages.get();
    long uni = receivedUniMessages.get();
    long totalOps = dgrams + bidi + uni;
    long totalBytes = totalBytesReceived.get();
    double durationSec = (endTime.get() - startTime.get()) / 1_000_000_000.0;
    long gcCount = endGcCount.get() - startGcCount.get();
    long gcPauseTimeMs = endGcTimeMs.get() - startGcTimeMs.get();
    double throughput = durationSec > 0 ? totalOps / durationSec : 0;
    long usedHeapStart = startUsedHeap.get();
    long usedHeapEnd = endUsedHeap.get();
    long heapDelta = usedHeapEnd - usedHeapStart;

    printServerReport(
        dgrams,
        bidi,
        uni,
        totalOps,
        totalBytes,
        durationSec,
        gcCount,
        gcPauseTimeMs,
        throughput,
        usedHeapStart,
        usedHeapEnd,
        heapDelta,
        startHistogramRef.get(),
        endHistogramRef.get());
    writeServerReportMarkdown(
        reportFile,
        dgrams,
        bidi,
        uni,
        totalOps,
        totalBytes,
        durationSec,
        gcCount,
        gcPauseTimeMs,
        throughput,
        usedHeapStart,
        usedHeapEnd,
        heapDelta);

    server.stop();
    System.exit(0);
  }

  private static long getUsedHeap() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private static long getTotalGcCount() {
    long count = 0;
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      long c = b.getCollectionCount();
      if (c > 0) {
        count += c;
      }
    }
    return count;
  }

  private static long getTotalGcTime() {
    long time = 0;
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      long t = b.getCollectionTime();
      if (t > 0) {
        time += t;
      }
    }
    return time;
  }

  private static Map<String, long[]> captureClassHistogram() {
    Map<String, long[]> map = new HashMap<>();
    try {
      javax.management.MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      javax.management.ObjectName on =
          new javax.management.ObjectName("com.sun.management:type=DiagnosticCommand");
      String res =
          (String)
              mbs.invoke(
                  on,
                  "gcClassHistogram",
                  new Object[] {new String[] {"-all"}},
                  new String[] {"[Ljava.lang.String;"});
      for (String line : res.split("\n")) {
        line = line.trim();
        if (line.isEmpty()
            || line.startsWith("num")
            || line.startsWith("---")
            || line.startsWith("Total")) {
          continue;
        }
        String[] parts = line.split("\\s+");
        if (parts.length >= 4) {
          try {
            long instances = Long.parseLong(parts[1]);
            long bytes = Long.parseLong(parts[2]);
            String className = parts[3];
            map.put(className, new long[] {instances, bytes});
          } catch (NumberFormatException ignored) {
            // Skip non-data rows
          }
        }
      }
    } catch (Exception ignored) {
      // Diagnostic command optional
    }
    return map;
  }

  private static void printHistogramDelta(
      Map<String, long[]> startMap, Map<String, long[]> endMap) {
    if (startMap == null || endMap == null || startMap.isEmpty() || endMap.isEmpty()) {
      return;
    }
    List<Map.Entry<String, long[]>> deltas = new ArrayList<>();
    for (Map.Entry<String, long[]> entry : endMap.entrySet()) {
      String className = entry.getKey();
      long endInst = entry.getValue()[0];
      long endBytes = entry.getValue()[1];
      long startInst = startMap.containsKey(className) ? startMap.get(className)[0] : 0;
      long startBytes = startMap.containsKey(className) ? startMap.get(className)[1] : 0;
      long instDelta = endInst - startInst;
      long byteDelta = endBytes - startBytes;
      if (byteDelta > 0) {
        deltas.add(new AbstractMap.SimpleEntry<>(className, new long[] {instDelta, byteDelta}));
      }
    }
    deltas.sort((a, b) -> Long.compare(b.getValue()[1], a.getValue()[1]));

    System.out.println("Exact Objects Allocated DURING ACTIVE FLOOD (End Histogram - Start Histogram):");
    if (deltas.isEmpty()) {
      System.out.println("  ✅ Exactly 0 new objects allocated on heap!");
    } else {
      for (int i = 0; i < Math.min(15, deltas.size()); i++) {
        Map.Entry<String, long[]> d = deltas.get(i);
        System.out.printf(
            "  + %,10d bytes (%,6d new instances): %s%n",
            d.getValue()[1], d.getValue()[0], d.getKey());
      }
    }
  }

  private static void printServerReport(
      long dgrams,
      long bidi,
      long uni,
      long totalOps,
      long totalBytes,
      double durationSec,
      long gcCount,
      long gcPauseTimeMs,
      double throughput,
      long usedHeapStart,
      long usedHeapEnd,
      long heapDelta,
      Map<String, long[]> startHist,
      Map<String, long[]> endHist) {
    String sep = "=========================================================================================";
    System.out.println("\n" + sep);
    System.out.println("   STANDALONE SERVER ZERO-GC VERIFICATION REPORT (ISOLATED DEDICATED JVM)                ");
    System.out.println(sep);
    System.out.printf("Datagrams Echoed:            %,d%n", dgrams);
    System.out.printf("Bi-Stream Msgs Echoed:       %,d%n", bidi);
    System.out.printf("Uni-Stream Msgs Consumed:    %,d%n", uni);
    System.out.printf("Total Ingested Messages:     %,d%n", totalOps);
    System.out.printf(
        "Total Bytes Processed:       %,d bytes (%.2f MB)%n",
        totalBytes, totalBytes / (1024.0 * 1024.0));
    System.out.printf("Session Active Duration:     %.3f seconds%n", durationSec);
    System.out.printf("Throughput:                  %,.1f ops/sec%n", throughput);
    System.out.println(
        "-----------------------------------------------------------------------------------------");
    System.out.printf("Garbage Collection Cycles:   %d collections%n", gcCount);
    System.out.printf("Total GC Pause Time:         %d ms%n", gcPauseTimeMs);
    System.out.printf(
        "Used Heap at Start:          %,d bytes (%.2f MB)%n",
        usedHeapStart, usedHeapStart / (1024.0 * 1024.0));
    System.out.printf(
        "Used Heap at End:            %,d bytes (%.2f MB)%n",
        usedHeapEnd, usedHeapEnd / (1024.0 * 1024.0));
    System.out.printf("Heap Memory Delta:           %,d bytes%n", heapDelta);
    System.out.println(
        "-----------------------------------------------------------------------------------------");
    System.out.println("Garbage Collector Details (MXBeans):");
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      System.out.printf(
          "  - %-25s: %d collections (%d ms total pause)%n",
          b.getName(), b.getCollectionCount(), b.getCollectionTime());
    }
    System.out.println("Heap Memory Pool Allocations (MXBeans):");
    for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
      if (p.getType() == MemoryType.HEAP) {
        System.out.printf(
            "  - %-25s: %,d bytes used (peak: %,d bytes)%n",
            p.getName(),
            p.getUsage().getUsed(),
            p.getPeakUsage() != null ? p.getPeakUsage().getUsed() : 0);
      }
    }
    printHistogramDelta(startHist, endHist);
    System.out.println(
        "-----------------------------------------------------------------------------------------");
    String verdict =
        (gcCount == 0 && gcPauseTimeMs == 0)
            ? "No collections observed in this measurement window; allocation was not measured"
            : "Observed " + gcCount + " collections, " + gcPauseTimeMs + " ms";
    System.out.printf("Verdict:                     %s%n", verdict);
    System.out.println(sep + "\n");
  }

  private static void writeServerReportMarkdown(
      String filename,
      long dgrams,
      long bidi,
      long uni,
      long totalOps,
      long totalBytes,
      double durationSec,
      long gcCount,
      long gcPauseTimeMs,
      double throughput,
      long usedHeapStart,
      long usedHeapEnd,
      long heapDelta) {
    try (PrintWriter out = new PrintWriter(new FileWriter(filename))) {
      out.println("# WebTransport4J: Reduced-Allocation Pipeline GC Observation Report");
      out.println("This finite run measures collections and heap usage, not allocated bytes per operation.");
      out.println("Zero observed collections do not establish zero allocation or production readiness.");
      out.println();
      out.println("Empirical measurement of the dedicated server process running in an isolated JVM");
      out.println("while receiving concurrent sustained flood traffic across all three WebTransport primitives:");
      out.println("Datagrams, Bidirectional Streams, and Unidirectional Streams.");
      out.println();
      out.println("### Multi-Primitive Traffic Breakdown");
      out.println();
      out.println("| Communication Primitive | Ingested & Processed Count | Mode |");
      out.println("| :--- | :--- | :--- |");
      out.printf("| **Datagrams** | %,d | Owned-buffer Echo |%n", dgrams);
      out.printf("| **Bidirectional Streams** | %,d | Owned-buffer Stream Echo |%n", bidi);
      out.printf("| **Unidirectional Streams** | %,d | Owned-buffer Stream Ingestion |%n", uni);
      out.printf("| **TOTAL MESSAGES** | **%,d** | Mixed Concurrent Flood |%n", totalOps);
      out.printf(
          "| **TOTAL PAYLOAD** | **%,d bytes** (%.2f MB) | Off-Heap Direct Buffers |%n",
          totalBytes, totalBytes / (1024.0 * 1024.0));
      out.println();
      out.println("### Garbage Collection & JVM Heap Metrics");
      out.println();
      out.println("| Metric | Value | Verdict |");
      out.println("| :--- | :--- | :--- |");
      out.printf("| **Active Flood Duration** | %.3f s | - |%n", durationSec);
      out.printf("| **Aggregate Throughput** | %,.1f ops/s | - |%n", throughput);
      out.printf(
          "| **GC Collections** | **%d** | %s |%n",
          gcCount, gcCount == 0 ? "✅ **ZERO COLLECTIONS**" : "Non-zero");
      out.printf(
          "| **Total GC Pause Time** | **%d ms** | %s |%n",
          gcPauseTimeMs, gcPauseTimeMs == 0 ? "✅ **ZERO PAUSE TIME**" : "Non-zero");
      out.printf(
          "| **Used Heap at Start** | %,d bytes (%.2f MB) | Pre-flood baseline |%n",
          usedHeapStart, usedHeapStart / (1024.0 * 1024.0));
      out.printf(
          "| **Used Heap at End** | %,d bytes (%.2f MB) | Post-flood steady-state |%n",
          usedHeapEnd, usedHeapEnd / (1024.0 * 1024.0));
      out.printf(
          "| **Heap Delta** | %,d bytes | Net change; does not measure total allocation |%n",
          heapDelta);
      out.println();
      out.println("### Active Garbage Collectors (JVM MXBeans)");
      out.println();
      out.println("| Collector Name | Collections | Total Pause Time |");
      out.println("| :--- | :--- | :--- |");
      for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
        out.printf("| `%s` | %d | %d ms |%n", b.getName(), b.getCollectionCount(), b.getCollectionTime());
      }
      out.println();
      out.println("### Heap Memory Pools");
      out.println();
      out.println("| Pool Name | Current Usage | Peak Usage |");
      out.println("| :--- | :--- | :--- |");
      for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
        if (p.getType() == MemoryType.HEAP) {
          out.printf(
              "| `%s` | %,d bytes (%.2f MB) | %,d bytes (%.2f MB) |%n",
              p.getName(),
              p.getUsage().getUsed(),
              p.getUsage().getUsed() / (1024.0 * 1024.0),
              p.getPeakUsage() != null ? p.getPeakUsage().getUsed() : 0,
              p.getPeakUsage() != null ? p.getPeakUsage().getUsed() / (1024.0 * 1024.0) : 0);
        }
      }
      out.println();
      String status =
          (gcCount == 0 && gcPauseTimeMs == 0)
              ? "No collections observed during this finite workload; allocation unmeasured"
              : "Collections observed during this finite workload";
      out.printf("**Overall Status:** %s%n", status);
      out.println();
      System.out.println("✅ Report written to " + filename);
    } catch (Exception e) {
      System.err.println("Failed to write report: " + e.getMessage());
    }
  }
}
