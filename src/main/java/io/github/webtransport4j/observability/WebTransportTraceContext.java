package io.github.webtransport4j.observability;

import java.security.SecureRandom;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Representation and parser for W3C TraceContext (RFC / W3C Recommendation) headers
 * ({@code traceparent} and {@code tracestate}) over WebTransport sessions and HTTP/3 streams.
 *
 * <p>Enables end-to-end distributed tracing across microservices with OpenTelemetry, Zipkin,
 * and Jaeger.
 */
public final class WebTransportTraceContext {

  public static final String HEADER_TRACEPARENT = "traceparent";
  public static final String HEADER_TRACESTATE = "tracestate";

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final String VERSION = "00";

  private final String version;
  private final String traceId;
  private final String parentId;
  private final String traceFlags;
  private final @Nullable String tracestate;

  private WebTransportTraceContext(
      @NonNull String version,
      @NonNull String traceId,
      @NonNull String parentId,
      @NonNull String traceFlags,
      @Nullable String tracestate) {
    this.version = version;
    this.traceId = traceId;
    this.parentId = parentId;
    this.traceFlags = traceFlags;
    this.tracestate = tracestate;
  }

  /**
   * Parses a W3C traceparent header string and optional tracestate.
   *
   * @param traceparent the traceparent header value
   *     (e.g. {@code "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
   * @param tracestate optional tracestate header value
   * @return parsed {@link WebTransportTraceContext}, or {@code null} if traceparent is invalid
   */
  public static @Nullable WebTransportTraceContext fromHeaders(
      @Nullable CharSequence traceparent,
      @Nullable CharSequence tracestate) {
    if (traceparent == null) {
      return null;
    }
    String s = traceparent.toString().trim();
    String[] parts = s.split("-");
    if (parts.length < 4) {
      return null;
    }
    String ver = parts[0];
    String traceId = parts[1];
    String parentId = parts[2];
    String flags = parts[3];

    if (ver.length() != 2 || traceId.length() != 32 || parentId.length() != 16 || flags.length() != 2) {
      return null;
    }
    String state = (tracestate != null) ? tracestate.toString().trim() : null;
    return new WebTransportTraceContext(ver, traceId, parentId, flags, state);
  }

  /**
   * Generates a new root trace context with random traceId and spanId.
   *
   * @param sampled whether tracing is sampled
   * @return a new {@link WebTransportTraceContext}
   */
  public static @NonNull WebTransportTraceContext createNew(boolean sampled) {
    byte[] traceBytes = new byte[16];
    byte[] parentBytes = new byte[8];
    RANDOM.nextBytes(traceBytes);
    RANDOM.nextBytes(parentBytes);

    String traceId = bytesToHex(traceBytes);
    String parentId = bytesToHex(parentBytes);
    String flags = sampled ? "01" : "00";
    return new WebTransportTraceContext(VERSION, traceId, parentId, flags, null);
  }

  /**
   * Creates a child trace context for a new span derived from this context.
   *
   * @return a child {@link WebTransportTraceContext} with the same traceId and a new spanId
   */
  public @NonNull WebTransportTraceContext createChildSpan() {
    byte[] childSpanBytes = new byte[8];
    RANDOM.nextBytes(childSpanBytes);
    String newSpanId = bytesToHex(childSpanBytes);
    return new WebTransportTraceContext(version, traceId, newSpanId, traceFlags, tracestate);
  }

  /**
   * Formats this context into the W3C traceparent header string.
   *
   * @return the {@code traceparent} header string
   */
  public @NonNull String toTraceparent() {
    return version + "-" + traceId + "-" + parentId + "-" + traceFlags;
  }

  public @NonNull String getVersion() {
    return version;
  }

  public @NonNull String getTraceId() {
    return traceId;
  }

  public @NonNull String getSpanId() {
    return parentId;
  }

  public @NonNull String getTraceFlags() {
    return traceFlags;
  }

  public boolean isSampled() {
    return "01".equals(traceFlags);
  }

  public @Nullable String getTracestate() {
    return tracestate;
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return toTraceparent() + (tracestate != null ? " (" + tracestate + ")" : "");
  }
}
