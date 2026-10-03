package io.github.webtransport4j.observability.otlp;

import io.micrometer.registry.otlp.OtlpConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Configuration implementation for exporting WebTransport metrics via OpenTelemetry Protocol
 * (OTLP).
 */
public class WebTransportOtlpConfig implements OtlpConfig {

  /** Default OTLP HTTP metrics receiver endpoint. */
  public static final String DEFAULT_URL = "http://localhost:4318/v1/metrics";

  private final boolean enabled;
  private final String url;
  private final Duration step;
  private final Map<String, String> headers;
  private final Map<String, String> resourceAttributes;

  /**
   * Constructs an OTLP config with specified parameters.
   *
   * @param url the OTLP collector endpoint URL
   * @param step metric export interval
   * @param headers HTTP headers to send with OTLP requests
   * @param resourceAttributes OpenTelemetry resource attributes (e.g. service.name)
   */
  public WebTransportOtlpConfig(
      @NonNull String url,
      @NonNull Duration step,
      @NonNull Map<String, String> headers,
      @NonNull Map<String, String> resourceAttributes) {
    this(true, url, step, headers, resourceAttributes);
  }

  /**
   * Constructs an OTLP config with specified parameters.
   *
   * @param enabled whether metric export publishing is enabled
   * @param url the OTLP collector endpoint URL
   * @param step metric export interval
   * @param headers HTTP headers to send with OTLP requests
   * @param resourceAttributes OpenTelemetry resource attributes (e.g. service.name)
   */
  public WebTransportOtlpConfig(
      boolean enabled,
      @NonNull String url,
      @NonNull Duration step,
      @NonNull Map<String, String> headers,
      @NonNull Map<String, String> resourceAttributes) {
    this.enabled = enabled;
    this.url = Objects.requireNonNull(url, "url must not be null");
    this.step = Objects.requireNonNull(step, "step must not be null");
    this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
    this.resourceAttributes = Collections.unmodifiableMap(new HashMap<>(resourceAttributes));
  }

  /**
   * Returns a builder for configuring {@link WebTransportOtlpConfig}.
   *
   * @return new builder instance
   */
  public static @NonNull Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether metric export publishing to the OTLP collector is enabled.
   *
   * @return true if publishing is enabled
   */
  @Override
  public boolean enabled() {
    return enabled;
  }

  /**
   * Looks up a configuration property value by key.
   *
   * @param key configuration property key
   * @return property value or null
   */
  @Override
  public @Nullable String get(@NonNull String key) {
    return null;
  }

  /**
   * Returns the OTLP collector receiver endpoint URL.
   *
   * @return endpoint URL
   */
  @Override
  public @NonNull String url() {
    return url;
  }

  /**
   * Returns the metric export interval duration.
   *
   * @return step duration
   */
  @Override
  public @NonNull Duration step() {
    return step;
  }

  /**
   * Returns the HTTP headers configured for OTLP export requests.
   *
   * @return map of header names to values
   */
  @Override
  public @NonNull Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns OpenTelemetry resource attributes configured for exported metrics.
   *
   * @return map of resource attribute keys to values
   */
  @Override
  public @NonNull Map<String, String> resourceAttributes() {
    return resourceAttributes;
  }

  /** Builder for creating {@link WebTransportOtlpConfig} instances. */
  public static final class Builder {
    private boolean enabled = true;
    private String url = DEFAULT_URL;
    private Duration step = Duration.ofSeconds(10);
    private final Map<String, String> headers = new HashMap<>();
    private final Map<String, String> resourceAttributes = new HashMap<>();

    /** Constructs a new builder initialized with default service name. */
    private Builder() {
      resourceAttributes.put("service.name", "webtransport4j");
    }

    /**
     * Sets whether metric export publishing to the OTLP collector is enabled.
     *
     * @param enabled true to enable publishing, false to disable
     * @return this builder
     */
    public @NonNull Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the OTLP collector endpoint URL (e.g. {@code http://localhost:4318/v1/metrics}).
     *
     * @param url OTLP endpoint URL
     * @return this builder
     */
    public @NonNull Builder url(@NonNull String url) {
      this.url = Objects.requireNonNull(url, "url must not be null");
      return this;
    }

    /**
     * Sets the metric export publishing interval.
     *
     * @param step export step interval
     * @return this builder
     */
    public @NonNull Builder step(@NonNull Duration step) {
      this.step = Objects.requireNonNull(step, "step must not be null");
      return this;
    }

    /**
     * Adds an HTTP header to outgoing OTLP export requests.
     *
     * @param name header name
     * @param value header value
     * @return this builder
     */
    public @NonNull Builder header(@NonNull String name, @NonNull String value) {
      headers.put(name, value);
      return this;
    }

    /**
     * Adds an OpenTelemetry resource attribute (e.g. {@code service.version}).
     *
     * @param key attribute key
     * @param value attribute value
     * @return this builder
     */
    public @NonNull Builder resourceAttribute(@NonNull String key, @NonNull String value) {
      resourceAttributes.put(key, value);
      return this;
    }

    /**
     * Builds and returns a new {@link WebTransportOtlpConfig}.
     *
     * @return configured OTLP config
     */
    public @NonNull WebTransportOtlpConfig build() {
      return new WebTransportOtlpConfig(enabled, url, step, headers, resourceAttributes);
    }
  }
}
