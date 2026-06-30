package io.github.webtransport4j.api;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A built-in {@link WebTransportMetricsListener} implementation that aggregates metric events using
 * lock-free atomic counters and periodically logs a telemetry summary at {@code INFO} level.
 *
 * <p>Default reporting interval is every 30 seconds (configurable via constructor).
 *
 * <p>Example output:
 *
 * <pre>
 * [WT Metrics] Active: 42 sessions | Opened: 1234 | Closed: 1192 | Streams: 5670 opened / 5640 closed
 *              Datagrams: 98765 recv | 102340 sent | 12 discarded
 *              Bytes recv: 123.4 MB | Bytes sent: 456.7 MB | Migrations: 3
 * </pre>
 *
 * @author https://github.com/sanjomo
 */
public class LoggingWebTransportMetricsListener implements WebTransportMetricsListener {

  private static final Logger logger =
      LoggerFactory.getLogger(LoggingWebTransportMetricsListener.class);

  // Session counters
  private final AtomicLong totalSessionsOpened = new AtomicLong(0);
  private final AtomicLong totalSessionsClosed = new AtomicLong(0);
  private final AtomicLong activeSessions = new AtomicLong(0);

  // Stream counters
  private final AtomicLong totalStreamsOpened = new AtomicLong(0);
  private final AtomicLong totalStreamsClosed = new AtomicLong(0);

  // Datagram counters
  private final AtomicLong datagramsReceived = new AtomicLong(0);
  private final AtomicLong datagramsSent = new AtomicLong(0);
  private final AtomicLong datagramsDiscarded = new AtomicLong(0);
  private final AtomicLong bytesReceived = new AtomicLong(0);
  private final AtomicLong bytesSent = new AtomicLong(0);

  // Connection migration counter
  private final AtomicLong connectionMigrations = new AtomicLong(0);

  private final ScheduledExecutorService scheduler;
  private final ScheduledFuture<?> reportFuture;

  /** Creates a new instance with a 30-second reporting interval. */
  public LoggingWebTransportMetricsListener() {
    this(30, TimeUnit.SECONDS);
  }

  /**
   * Creates a new instance with a custom reporting interval.
   *
   * @param interval the reporting interval value.
   * @param unit the reporting interval time unit.
   */
  public LoggingWebTransportMetricsListener(long interval, @NonNull TimeUnit unit) {
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "wt-metrics-reporter");
              t.setDaemon(true);
              return t;
            });
    this.reportFuture = scheduler.scheduleWithFixedDelay(this::report, interval, interval, unit);
  }

  // ─── SPI Callbacks ──────────────────────────────────────────────────────────

  @Override
  public void onSessionOpened(long sessionId, @NonNull String path) {
    totalSessionsOpened.incrementAndGet();
    activeSessions.incrementAndGet();
  }

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {
    totalSessionsClosed.incrementAndGet();
    activeSessions.decrementAndGet();
  }

  @Override
  public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {
    totalStreamsOpened.incrementAndGet();
  }

  @Override
  public void onStreamClosed(long sessionId, long streamId) {
    totalStreamsClosed.incrementAndGet();
  }

  @Override
  public void onDatagramReceived(long sessionId, int bytes) {
    datagramsReceived.incrementAndGet();
    bytesReceived.addAndGet(bytes);
  }

  @Override
  public void onDatagramSent(long sessionId, int bytes) {
    datagramsSent.incrementAndGet();
    bytesSent.addAndGet(bytes);
  }

  @Override
  public void onDatagramDiscarded(long sessionId, @NonNull String reason) {
    datagramsDiscarded.incrementAndGet();
  }

  @Override
  public void onConnectionMigration(
      long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {
    connectionMigrations.incrementAndGet();
    logger.info(
        "🔀 [WT Metrics] Connection Migration | Session: {} | {} → {}",
        sessionId,
        oldAddress,
        newAddress);
  }

  // ─── Periodic Report ────────────────────────────────────────────────────────

  private void report() {
    if (!logger.isInfoEnabled()) {
      return;
    }
    long active = activeSessions.get();
    long opened = totalSessionsOpened.get();
    long closed = totalSessionsClosed.get();
    long streamsOpened = totalStreamsOpened.get();
    long streamsClosed = totalStreamsClosed.get();
    long dgRecv = datagramsReceived.get();
    long dgSent = datagramsSent.get();
    long dgDiscard = datagramsDiscarded.get();
    long bytesReceivedTotal = bytesReceived.get();
    long bytesSentTotal = bytesSent.get();
    long migrations = connectionMigrations.get();

    logger.info(
        "📊 [WT Metrics] Active: {} sessions | Opened: {} | Closed: {} | Streams: {} opened / {}"
            + " closed",
        active,
        opened,
        closed,
        streamsOpened,
        streamsClosed);
    logger.info(
        "📊 [WT Metrics] Datagrams: {} recv | {} sent | {} discarded | Bytes recv: {} | Bytes sent:"
            + " {} | Migrations: {}",
        dgRecv,
        dgSent,
        dgDiscard,
        formatBytes(bytesReceivedTotal),
        formatBytes(bytesSentTotal),
        migrations);
  }

  /** Shuts down the reporting scheduler. Call this when the server stops. */
  public void shutdown() {
    if (reportFuture != null) {
      reportFuture.cancel(false);
    }
    scheduler.shutdownNow();
  }

  /** Provides a snapshot of all current metric values as a formatted string. */
  public String snapshot() {
    return String.format(
        "Sessions[active=%d, opened=%d, closed=%d] Streams[opened=%d, closed=%d] "
            + "Datagrams[recv=%d, sent=%d, discarded=%d] Bytes[recv=%s, sent=%s] Migrations=%d",
        activeSessions.get(),
        totalSessionsOpened.get(),
        totalSessionsClosed.get(),
        totalStreamsOpened.get(),
        totalStreamsClosed.get(),
        datagramsReceived.get(),
        datagramsSent.get(),
        datagramsDiscarded.get(),
        formatBytes(bytesReceived.get()),
        formatBytes(bytesSent.get()),
        connectionMigrations.get());
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    if (bytes < 1024 * 1024) {
      return String.format("%.1f KB", bytes / 1024.0);
    }
    if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.1f MB", bytes / (1024.0 * 1024));
    }
    return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
  }
}
