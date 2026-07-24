package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone benchmark server runner to measure pure server-side performance,
 * memory usage, GC pauses, and CPU load without client co-location overhead.
 */
public class BenchmarkServerRunner {

  private static final Logger logger = LoggerFactory.getLogger(BenchmarkServerRunner.class);

  public static final AtomicLong serverReceivedMsgs = new AtomicLong();
  public static final AtomicLong serverSentMsgs = new AtomicLong();

  public static class EchoHandler implements WebTransportHandler {
    @Override
    public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
      if (stream.isBidirectional()) {
        stream.onData(
            data -> {
              serverReceivedMsgs.incrementAndGet();
              stream.write(data);
              serverSentMsgs.incrementAndGet();
            });
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int port = 56159;
    if (args.length > 0) {
      try {
        port = Integer.parseInt(args[0]);
      } catch (NumberFormatException e) {
        logger.warn("Invalid port argument: {}. Falling back to default {}", args[0], port);
      }
    } else {
      port = Integer.getInteger("server.port", 56159);
    }
    int idleTimeoutSeconds = Integer.getInteger("benchmark.idle.timeout.seconds", 600);
    if (idleTimeoutSeconds < 1) {
      throw new IllegalArgumentException("benchmark.idle.timeout.seconds must be positive");
    }

    System.setProperty("webtransport4j.server.port", String.valueOf(port));
    System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
    System.setProperty("webtransport4j.webtransport.max_sessions_per_connection", "1000");
    System.setProperty("webtransport4j.quic.idle.timeout.seconds", String.valueOf(idleTimeoutSeconds));
    System.setProperty("webtransport4j.server.max_concurrent_sessions", "100000");
    System.setProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", "1000000");
    System.setProperty("webtransport4j.allowed.origins", "*");
    System.setProperty("webtransport4j.server.allowed_origins", "*");
    System.setProperty("webtransport4j.quic.token.handler", "insecure");
    System.setProperty("webtransport4j.dev_mode", "true");
    System.setProperty("webtransport4j.dispatch.execution.mode", "NETTY_EVENT_LOOP");

    WebTransportServer server = new WebTransportServer();
    server.registerHandler("/bench", new EchoHandler());

    logger.info("================================================================");
    logger.info("⚡ STANDALONE WEBTRANSPORT4J BENCHMARK SERVER ⚡");
    logger.info("   Port: {}", port);
    logger.info("   Path: /bench");
    logger.info("   QUIC idle timeout: {} seconds", idleTimeoutSeconds);
    logger.info("================================================================");

    server.start();

    logger.info("✅ Server started and listening on port {}", server.getPort());
    logger.info("   Run client benchmark targeting port {}:", server.getPort());
    logger.info(
        "   mvn test -Dtest=ConnectionScalabilityBenchmarkTest -Dtarget.port={}"
            + " -Dbenchmark.connections=10000 -Dbenchmark.hold.seconds=60",
        server.getPort());
    logger.info("================================================================");

    // Monitoring thread: prints memory, GC, and status every 5 seconds
    AtomicInteger peakSessionCount = new AtomicInteger();
    Thread monitorThread = new Thread(() -> {
      MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
      long prevGcCount = 0;
      long prevGcTime = 0;

      while (server.isStarted()) {
        try {
          Thread.sleep(5000);
          long heapMb = mem.getHeapMemoryUsage().getUsed() / (1024 * 1024);
          long gcCount = 0;
          long gcTime = 0;
          for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long c = gc.getCollectionCount();
            long t = gc.getCollectionTime();
            if (c > 0) {
              gcCount += c;
              gcTime += t;
            }
          }
          long dCount = gcCount - prevGcCount;
          long dTime = gcTime - prevGcTime;
          prevGcCount = gcCount;
          prevGcTime = gcTime;
          int activeSessionCount = server.getActiveSessionCount();
          int peakSessions = peakSessionCount.accumulateAndGet(activeSessionCount, Math::max);

          logger.info(
              "📊 SERVER STATS | Sessions: {} (peak {}) | Server Recv: {} | Server Sent: {} | Heap: {} MB | GC Count Δ (5s): {}"
                  + " | GC Time Δ (5s): {} ms",
              activeSessionCount,
              peakSessions,
              serverReceivedMsgs.get(),
              serverSentMsgs.get(),
              heapMb,
              dCount,
              dTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    });
    monitorThread.setDaemon(true);
    monitorThread.start();

    // Prevent main thread from exiting
    Thread.currentThread().join();
  }
}
