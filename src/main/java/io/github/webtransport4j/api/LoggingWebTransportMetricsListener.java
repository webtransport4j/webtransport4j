package io.github.webtransport4j.api;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.LongHandle;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
public class LoggingWebTransportMetricsListener
    implements WebTransportMetricsListener, AutoCloseable {

  private static final Logger logger =
      LoggerFactory.getLogger(LoggingWebTransportMetricsListener.class);

  private static final LongHandle<LoggingWebTransportMetricsListener>
      TOTAL_SESSIONS_OPENED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "totalSessionsOpened",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "totalSessionsOpened"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      TOTAL_SESSIONS_CLOSED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "totalSessionsClosed",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "totalSessionsClosed"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      ACTIVE_SESSIONS_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "activeSessions",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "activeSessions"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      TOTAL_STREAMS_OPENED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "totalStreamsOpened",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "totalStreamsOpened"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      TOTAL_STREAMS_CLOSED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "totalStreamsClosed",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "totalStreamsClosed"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      DATAGRAMS_RECEIVED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "datagramsReceived",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "datagramsReceived"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      DATAGRAMS_SENT_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "datagramsSent",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "datagramsSent"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      DATAGRAMS_DISCARDED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "datagramsDiscarded",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "datagramsDiscarded"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      BYTES_RECEIVED_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "bytesReceived",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "bytesReceived"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      BYTES_SENT_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "bytesSent",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "bytesSent"));

  private static final LongHandle<LoggingWebTransportMetricsListener>
      CONNECTION_MIGRATIONS_HANDLE =
          Handles.newLongHandle(
              LoggingWebTransportMetricsListener.class,
              "connectionMigrations",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      LoggingWebTransportMetricsListener.class, "connectionMigrations"));

  // Session counters
  private volatile long totalSessionsOpened;
  private volatile long totalSessionsClosed;
  private volatile long activeSessions;

  // Stream counters
  private volatile long totalStreamsOpened;
  private volatile long totalStreamsClosed;

  // Datagram counters
  private volatile long datagramsReceived;
  private volatile long datagramsSent;
  private volatile long datagramsDiscarded;
  private volatile long bytesReceived;
  private volatile long bytesSent;

  // Connection migration counter
  private volatile long connectionMigrations;

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
    TOTAL_SESSIONS_OPENED_HANDLE.incrementAndGet(this);
    ACTIVE_SESSIONS_HANDLE.incrementAndGet(this);
  }

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {
    TOTAL_SESSIONS_CLOSED_HANDLE.incrementAndGet(this);
    ACTIVE_SESSIONS_HANDLE.decrementAndGet(this);
  }

  @Override
  public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {
    TOTAL_STREAMS_OPENED_HANDLE.incrementAndGet(this);
  }

  @Override
  public void onStreamClosed(long sessionId, long streamId) {
    TOTAL_STREAMS_CLOSED_HANDLE.incrementAndGet(this);
  }

  @Override
  public void onDatagramReceived(long sessionId, int bytes) {
    DATAGRAMS_RECEIVED_HANDLE.incrementAndGet(this);
    BYTES_RECEIVED_HANDLE.addAndGet(this, bytes);
  }

  @Override
  public void onDatagramSent(long sessionId, int bytes) {
    DATAGRAMS_SENT_HANDLE.incrementAndGet(this);
    BYTES_SENT_HANDLE.addAndGet(this, bytes);
  }

  @Override
  public void onDatagramDiscarded(long sessionId, @NonNull String reason) {
    DATAGRAMS_DISCARDED_HANDLE.incrementAndGet(this);
  }

  @Override
  public void onConnectionMigration(
      long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {
    CONNECTION_MIGRATIONS_HANDLE.incrementAndGet(this);
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
    long active = activeSessions;
    long opened = totalSessionsOpened;
    long closed = totalSessionsClosed;
    long streamsOpened = totalStreamsOpened;
    long streamsClosed = totalStreamsClosed;
    long dgRecv = datagramsReceived;
    long dgSent = datagramsSent;
    long dgDiscard = datagramsDiscarded;
    long bytesReceivedTotal = bytesReceived;
    long bytesSentTotal = bytesSent;
    long migrations = connectionMigrations;

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

  @Override
  public void close() {
    shutdown();
  }

  /** Provides a snapshot of all current metric values as a formatted string. */
  public String snapshot() {
    return String.format(
        "Sessions[active=%d, opened=%d, closed=%d] Streams[opened=%d, closed=%d] "
            + "Datagrams[recv=%d, sent=%d, discarded=%d] Bytes[recv=%s, sent=%s] Migrations=%d",
        activeSessions,
        totalSessionsOpened,
        totalSessionsClosed,
        totalStreamsOpened,
        totalStreamsClosed,
        datagramsReceived,
        datagramsSent,
        datagramsDiscarded,
        formatBytes(bytesReceived),
        formatBytes(bytesSent),
        connectionMigrations);
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
