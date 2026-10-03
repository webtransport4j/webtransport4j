package io.github.webtransport4j.observability.otlp;

import io.github.webtransport4j.metrics.WebTransportMicrometerMetricsListener;
import io.micrometer.core.instrument.Clock;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * WebTransport metrics listener that automatically exports all session, stream, and datagram
 * telemetry via OpenTelemetry Protocol (OTLP).
 */
public class WebTransportOtlpMetricsListener extends WebTransportMicrometerMetricsListener
    implements AutoCloseable {

  private final OtlpMeterRegistry otlpRegistry;

  /**
   * Constructs an OTLP listener with default configuration pointing to {@code
   * http://localhost:4318/v1/metrics}.
   */
  public WebTransportOtlpMetricsListener() {
    this(WebTransportOtlpConfig.builder().build());
  }

  /**
   * Constructs an OTLP listener with a custom {@link WebTransportOtlpConfig}.
   *
   * @param config OTLP configuration parameters
   */
  public WebTransportOtlpMetricsListener(@NonNull WebTransportOtlpConfig config) {
    this(
        new OtlpMeterRegistry(
            Objects.requireNonNull(config, "config must not be null"), Clock.SYSTEM));
  }

  /**
   * Constructs an OTLP listener wrapping an existing {@link OtlpMeterRegistry}.
   *
   * @param registry pre-configured OTLP meter registry
   */
  public WebTransportOtlpMetricsListener(@NonNull OtlpMeterRegistry registry) {
    super("webtransport");
    this.otlpRegistry = Objects.requireNonNull(registry, "registry must not be null");
    bindTo(this.otlpRegistry);
  }

  /**
   * Returns the underlying {@link OtlpMeterRegistry}.
   *
   * @return OTLP meter registry
   */
  public @NonNull OtlpMeterRegistry getRegistry() {
    return otlpRegistry;
  }

  /** Closes the underlying OTLP meter registry and releases resources. */
  @Override
  public void close() {
    otlpRegistry.close();
  }
}
