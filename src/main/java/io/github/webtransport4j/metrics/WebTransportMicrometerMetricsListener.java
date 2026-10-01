package io.github.webtransport4j.metrics;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;

/**
 * Production-ready Micrometer {@link MeterBinder} that exports rich WebTransport metrics
 * to any telemetry backend (Prometheus, Datadog, InfluxDB, CloudWatch, OpenTelemetry).
 *
 * <p>Exposes active session gauges, opened/closed session counters with path/status tags,
 * stream counters (bidi/uni), datagram throughput counters, drop counters, and packet
 * size distribution summaries.
 */
public class WebTransportMicrometerMetricsListener implements WebTransportMetricsListener, MeterBinder {

  private final String prefix;
  private final AtomicLong activeSessions = new AtomicLong(0);
  private final AtomicLong activeStreams = new AtomicLong(0);
  private final Map<Long, Long> sessionStartTimes = new ConcurrentHashMap<>();

  private MeterRegistry registry;
  private Counter datagramsSentCounter;
  private Counter datagramsReceivedCounter;
  private Counter datagramsDroppedCounter;
  private DistributionSummary datagramSentBytesSummary;
  private DistributionSummary datagramReceivedBytesSummary;
  private Timer sessionDurationTimer;

  /**
   * Constructs a listener with the default metric prefix {@code "webtransport"}.
   */
  public WebTransportMicrometerMetricsListener() {
    this("webtransport");
  }

  /**
   * Constructs a listener with a custom metric prefix.
   *
   * @param prefix the metric name prefix (e.g. {@code "my_app.webtransport"})
   */
  public WebTransportMicrometerMetricsListener(@NonNull String prefix) {
    this.prefix = Objects.requireNonNull(prefix, "prefix");
  }

  @Override
  public void bindTo(@NonNull MeterRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");

    Gauge.builder(prefix + ".sessions.active", activeSessions, AtomicLong::get)
        .description("Number of currently active WebTransport sessions")
        .register(registry);

    Gauge.builder(prefix + ".streams.active", activeStreams, AtomicLong::get)
        .description("Number of currently open WebTransport streams")
        .register(registry);

    datagramsSentCounter = Counter.builder(prefix + ".datagrams.sent")
        .description("Total number of WebTransport datagrams sent")
        .register(registry);

    datagramsReceivedCounter = Counter.builder(prefix + ".datagrams.received")
        .description("Total number of WebTransport datagrams received")
        .register(registry);

    datagramsDroppedCounter = Counter.builder(prefix + ".datagrams.dropped")
        .description("Total number of WebTransport datagrams dropped due to queue saturation")
        .register(registry);

    datagramSentBytesSummary = DistributionSummary.builder(prefix + ".datagram.sent.bytes")
        .description("Distribution of sent datagram payload sizes")
        .baseUnit("bytes")
        .register(registry);

    datagramReceivedBytesSummary = DistributionSummary.builder(prefix + ".datagram.received.bytes")
        .description("Distribution of received datagram payload sizes")
        .baseUnit("bytes")
        .register(registry);

    sessionDurationTimer = Timer.builder(prefix + ".session.duration")
        .description("Lifespan duration of completed WebTransport sessions")
        .register(registry);
  }

  @Override
  public void onSessionOpened(long sessionId, @NonNull String path) {
    activeSessions.incrementAndGet();
    sessionStartTimes.put(sessionId, System.nanoTime());
    if (registry != null) {
      registry.counter(prefix + ".sessions.opened", "path", path).increment();
    }
  }

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {
    activeSessions.decrementAndGet();
    Long startTime = sessionStartTimes.remove(sessionId);
    if (startTime != null && sessionDurationTimer != null) {
      sessionDurationTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }
    if (registry != null) {
      registry.counter(prefix + ".sessions.closed", "status", String.valueOf(closeCode)).increment();
    }
  }

  @Override
  public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {
    activeStreams.incrementAndGet();
    if (registry != null) {
      String type = bidirectional ? "bidi" : "uni";
      registry.counter(prefix + ".streams.opened", "type", type).increment();
    }
  }

  @Override
  public void onStreamClosed(long sessionId, long streamId) {
    activeStreams.decrementAndGet();
    if (registry != null) {
      registry.counter(prefix + ".streams.closed").increment();
    }
  }

  @Override
  public void onDatagramSent(long sessionId, int bytes) {
    if (datagramsSentCounter != null) {
      datagramsSentCounter.increment();
    }
    if (datagramSentBytesSummary != null) {
      datagramSentBytesSummary.record(bytes);
    }
  }

  @Override
  public void onDatagramReceived(long sessionId, int bytes) {
    if (datagramsReceivedCounter != null) {
      datagramsReceivedCounter.increment();
    }
    if (datagramReceivedBytesSummary != null) {
      datagramReceivedBytesSummary.record(bytes);
    }
  }

  @Override
  public void onDatagramDiscarded(long sessionId, @NonNull String reason) {
    if (datagramsDroppedCounter != null) {
      datagramsDroppedCounter.increment();
    }
  }

  @Override
  public void onConnectionMigration(
      long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {
    if (registry != null) {
      registry.counter(prefix + ".connections.migrated").increment();
    }
  }

  /**
   * Returns current active sessions count.
   *
   * @return active session count
   */
  public long getActiveSessions() {
    return activeSessions.get();
  }

  /**
   * Returns current active streams count.
   *
   * @return active stream count
   */
  public long getActiveStreams() {
    return activeStreams.get();
  }
}
