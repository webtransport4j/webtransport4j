package io.github.webtransport4j.observability;

import java.security.SecureRandom;
import java.util.Locale;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Representation and parser for W3C TraceContext (RFC / W3C Recommendation) headers ({@code
 * traceparent} and {@code tracestate}) over WebTransport sessions and HTTP/3 streams.
 *
 * <p>Enables end-to-end distributed tracing across microservices with OpenTelemetry, Zipkin, and
 * Jaeger.
 */
public final class WebTransportTraceContext {

  /** W3C HTTP header name for traceparent. */
  public static final String HEADER_TRACEPARENT = "traceparent";

  /** W3C HTTP header name for tracestate. */
  public static final String HEADER_TRACESTATE = "tracestate";

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final String VERSION = "00";

  private final String version;
  private final String traceId;
  private final String parentId;
  private final String traceFlags;
  private final @Nullable String tracestate;

  /**
   * Internal constructor for WebTransportTraceContext.
   *
   * @param version W3C trace context version
   * @param traceId 16-byte hex trace identifier
   * @param parentId 8-byte hex parent span identifier
   * @param traceFlags 8-bit hex trace flags
   * @param tracestate optional W3C tracestate value
   */
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
   * @param traceparent the traceparent header value (e.g. {@code
   *     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
   * @param tracestate optional tracestate header value
   * @return parsed {@link WebTransportTraceContext}, or {@code null} if traceparent is invalid
   */
  public static @Nullable WebTransportTraceContext fromHeaders(
      @Nullable CharSequence traceparent, @Nullable CharSequence tracestate) {
    if (traceparent == null) {
      return null;
    }
    final String s = traceparent.toString().trim();
    final String[] parts = s.split("-", 5);
    if (parts.length < 4) {
      return null;
    }
    final String ver = parts[0];
    if (VERSION.equals(ver) && parts.length != 4) {
      return null;
    }
    final String traceId = parts[1];
    final String parentId = parts[2];
    final String flags = parts[3];

    // Version validation: 2 hex chars, "ff" is forbidden in W3C spec
    if (ver.length() != 2 || "ff".equalsIgnoreCase(ver) || !isHex(ver)) {
      return null;
    }
    // Trace ID validation: 32 hex chars, all-zeros is forbidden
    if (traceId.length() != 32 || isAllZeros(traceId) || !isHex(traceId)) {
      return null;
    }
    // Parent/Span ID validation: 16 hex chars, all-zeros is forbidden
    if (parentId.length() != 16 || isAllZeros(parentId) || !isHex(parentId)) {
      return null;
    }
    // Flags validation: 2 hex chars
    if (flags.length() != 2 || !isHex(flags)) {
      return null;
    }
    final String state = (tracestate != null) ? tracestate.toString().trim() : null;
    return new WebTransportTraceContext(ver, traceId, parentId, flags, state);
  }

  /**
   * Generates a new root trace context with random traceId and spanId.
   *
   * @param sampled whether tracing is sampled
   * @return a new {@link WebTransportTraceContext}
   */
  public static @NonNull WebTransportTraceContext createNew(boolean sampled) {
    final byte[] traceBytes = new byte[16];
    final byte[] parentBytes = new byte[8];
    RANDOM.nextBytes(traceBytes);
    RANDOM.nextBytes(parentBytes);

    final String traceId = bytesToHex(traceBytes);
    final String parentId = bytesToHex(parentBytes);
    final String flags = sampled ? "01" : "00";
    return new WebTransportTraceContext(VERSION, traceId, parentId, flags, null);
  }

  /**
   * Creates a child trace context for a new span derived from this context.
   *
   * @return a child {@link WebTransportTraceContext} with the same traceId and a new spanId
   */
  public @NonNull WebTransportTraceContext createChildSpan() {
    final byte[] childSpanBytes = new byte[8];
    RANDOM.nextBytes(childSpanBytes);
    final String newSpanId = bytesToHex(childSpanBytes);
    return new WebTransportTraceContext(
        VERSION, traceId, newSpanId, isSampled() ? "01" : "00", tracestate);
  }

  /**
   * Formats this context into the W3C traceparent header string.
   *
   * @return the {@code traceparent} header string
   */
  public @NonNull String toTraceparent() {
    return version + "-" + traceId + "-" + parentId + "-" + traceFlags;
  }

  /**
   * Returns the W3C version string.
   *
   * @return version string
   */
  public @NonNull String getVersion() {
    return version;
  }

  /**
   * Returns the 16-byte hex-encoded trace identifier.
   *
   * @return trace identifier
   */
  public @NonNull String getTraceId() {
    return traceId;
  }

  /**
   * Returns the 8-byte hex-encoded span identifier.
   *
   * @return span identifier
   */
  public @NonNull String getSpanId() {
    return parentId;
  }

  /**
   * Returns the 8-bit hex-encoded trace flags.
   *
   * @return trace flags
   */
  public @NonNull String getTraceFlags() {
    return traceFlags;
  }

  /**
   * Returns whether the sampled flag bit is set (least significant bit of traceFlags).
   *
   * @return true if sampled
   */
  public boolean isSampled() {
    try {
      return (Integer.parseInt(traceFlags, 16) & 1) != 0;
    } catch (NumberFormatException ignored) {
      return false;
    }
  }

  /**
   * Returns the optional W3C tracestate string, or null if not present.
   *
   * @return tracestate string or null
   */
  public @Nullable String getTracestate() {
    return tracestate;
  }

  /**
   * Checks whether the string contains exclusively hexadecimal characters.
   *
   * @param s string to test
   * @return true if valid hexadecimal
   */
  private static boolean isHex(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether the string contains exclusively '0' characters.
   *
   * @param s string to test
   * @return true if all zeros
   */
  private static boolean isAllZeros(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) != '0') {
        return false;
      }
    }
    return true;
  }

  /**
   * Formats a byte array as a lowercase hexadecimal string.
   *
   * @param bytes byte array
   * @return hexadecimal representation
   */
  private static String bytesToHex(byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format(Locale.ROOT, "%02x", b & 0xff));
    }
    return sb.toString();
  }

  /**
   * Returns a string representation of this trace context.
   *
   * @return traceparent string with optional tracestate
   */
  @Override
  public String toString() {
    return toTraceparent() + (tracestate != null ? " (" + tracestate + ")" : "");
  }
}
