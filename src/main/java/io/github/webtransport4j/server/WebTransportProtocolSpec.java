package io.github.webtransport4j.server;

import org.jspecify.annotations.Nullable;

/**
 * Stateless specification constants and pure validation predicates for IETF {@code
 * draft-ietf-webtrans-http3-17}.
 *
 * <p>Centralizing wire codepoints and invariant checks here keeps Netty channel handlers decoupled
 * from raw protocol constants and allows direct unit testing without transport mocks.
 */
public final class WebTransportProtocolSpec {

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_ENABLED} wire codepoint. */
  public static final long SETTING_WT_ENABLED = 0x20b8933bL;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_DATA} wire codepoint. */
  public static final long SETTING_WT_INITIAL_MAX_DATA = 0x2b61L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_STREAMS_UNI} wire codepoint. */
  public static final long SETTING_WT_INITIAL_MAX_STREAMS_UNI = 0x2b64L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI} wire codepoint. */
  public static final long SETTING_WT_INITIAL_MAX_STREAMS_BIDI = 0x2b65L;

  /** Legacy / multi-session extension setting {@code SETTINGS_WEBTRANSPORT_MAX_SESSIONS}. */
  public static final long SETTING_WEBTRANSPORT_MAX_SESSIONS = 0x2b603742L;

  /** Default comma-separated list of allowed non-standard HTTP/3 settings for Draft-17. */
  public static final String DEFAULT_ALLOWED_SETTINGS =
      "0x20b8933b,0x2b64,0x2b65,0x2b61,0x2b603742";

  /** RFC 9114 Section 8.1 / Draft-17 Section 4.3: {@code H3_FRAME_UNEXPECTED} error code. */
  public static final int H3_FRAME_UNEXPECTED = 0x0105;

  /** RFC 9114 Section 8.1 / Draft-17 Section 3.1: {@code H3_SETTINGS_ERROR} error code. */
  public static final int H3_SETTINGS_ERROR = 0x0109;

  /** RFC 9114 Section 6.2.1: HTTP/3 unidirectional Control Stream type. */
  public static final long H3_STREAM_TYPE_CONTROL = 0x00L;

  /** RFC 9114 Section 6.2.2: HTTP/3 unidirectional Push Stream type. */
  public static final long H3_STREAM_TYPE_PUSH = 0x01L;

  /** Maximum 62-bit unsigned QUIC variable-length integer value ({@code 2^62 - 1}). */
  public static final long MAX_QUIC_VAR_INT = 0x3FFFFFFFFFFFFFFFL;

  /** Draft-17 Section 5.6.2: Maximum legal {@code WT_MAX_STREAMS} value ({@code 2^60}). */
  public static final long MAX_WT_STREAMS_LIMIT = 1L << 60;

  private WebTransportProtocolSpec() {}

  /**
   * Validates the value of {@code SETTINGS_WT_ENABLED} received by a client from a server per
   * Draft-17 Section 3.1.
   *
   * <p>A missing value defaults to {@code 0}. Clients MUST treat values greater than {@code 1} as a
   * connection error of type {@code H3_SETTINGS_ERROR}.
   *
   * @param settingValue decoded setting value, or {@code null} when omitted
   * @return {@code true} if the setting value is {@code null}, {@code 0}, or {@code 1}
   */
  public static boolean isValidServerWtEnabledValue(@Nullable Long settingValue) {
    return settingValue == null || settingValue == 0L || settingValue == 1L;
  }

  /**
   * Checks whether a server advertised support for Draft-17 WebTransport ({@code
   * SETTINGS_WT_ENABLED == 1}).
   *
   * @param settingValue decoded setting value, or {@code null} when omitted
   * @return {@code true} when {@code settingValue} equals {@code 1}
   */
  public static boolean isServerWebTransportEnabled(@Nullable Long settingValue) {
    return settingValue != null && settingValue == 1L;
  }

  /**
   * Checks strict monotonicity for {@code WT_MAX_STREAMS} and {@code WT_MAX_DATA} capsules per
   * Draft-17 Sections 5.6.1, 5.6.2, and 5.6.4.
   *
   * <p>Each received or sent flow-control capsule MUST strictly increase the previously received
   * capsule value, or the corresponding initial {@code SETTINGS} value (defaulting to {@code 0}) if
   * no capsule of that type has been received yet.
   *
   * @param candidateLimit the limit carried in the incoming or outgoing capsule
   * @param currentOrInitialLimit the current cumulative limit (from prior capsule or initial
   *     setting)
   * @return {@code true} if {@code candidateLimit > currentOrInitialLimit}
   */
  public static boolean isStrictlyIncreasingLimit(
      long candidateLimit, long currentOrInitialLimit) {
    return candidateLimit > currentOrInitialLimit;
  }

  /**
   * Validates the {@code https} URI request target pseudo-headers per Draft-17 Section 2.2, RFC
   * 9110 Section 4.2.2, and RFC 9114 Section 4.3.1.
   *
   * @param scheme the {@code :scheme} pseudo-header
   * @param authority the {@code :authority} pseudo-header
   * @param path the {@code :path} pseudo-header
   * @return {@code true} when scheme is {@code "https"}, authority is non-empty, and path is a
   *     non-empty origin-form path starting with {@code '/'}
   */
  public static boolean isValidHttpsRequestTarget(
      @Nullable CharSequence scheme,
      @Nullable CharSequence authority,
      @Nullable CharSequence path) {
    if (scheme == null || !"https".contentEquals(scheme)) {
      return false;
    }
    if (authority == null || authority.length() == 0 || containsPathSeparator(authority)) {
      return false;
    }
    return path != null && path.length() > 0 && path.charAt(0) == '/' && !containsFragment(path);
  }

  private static boolean containsPathSeparator(CharSequence seq) {
    for (int i = 0; i < seq.length(); i++) {
      char c = seq.charAt(i);
      if (c == '/' || c == '\\') {
        return true;
      }
    }
    return false;
  }

  private static boolean containsFragment(CharSequence seq) {
    for (int i = 0; i < seq.length(); i++) {
      if (seq.charAt(i) == '#') {
        return true;
      }
    }
    return false;
  }

  /**
   * Computes the exact byte length of the WebTransport stream header written on the local send side
   * of a stream per Draft-17 Sections 4.2, 4.3, and 4.4.
   *
   * @param localInitiated {@code true} if this endpoint initiated the QUIC stream
   * @param bidirectional {@code true} for bidirectional {@code WT_STREAM} ({@code 0x41}), {@code
   *     false} for unidirectional ({@code 0x54})
   * @param sessionId the WebTransport session ID (CONNECT stream ID)
   * @return exact WebTransport header byte length on the local send side ({@code 0} for
   *     peer-initiated bidirectional streams)
   */
  public static int computeLocalStreamHeaderBytes(
      boolean localInitiated, boolean bidirectional, long sessionId) {
    if (!localInitiated) {
      return 0;
    }
    long marker =
        bidirectional ? WebTransportUtils.BI_STREAM_TYPE : WebTransportUtils.UNI_STREAM_TYPE;
    return WebTransportUtils.varIntLength(marker) + WebTransportUtils.varIntLength(sessionId);
  }

  /**
   * Computes the QUIC {@code RESET_STREAM_AT} Reliable Size per Draft-17 Section 4.4: the size of
   * the WebTransport header on that stream's send side plus any application-supplied offset.
   *
   * @param localInitiated {@code true} if this endpoint initiated the QUIC stream
   * @param bidirectional {@code true} for bidirectional stream, {@code false} for unidirectional
   * @param sessionId the WebTransport session ID
   * @param appReliableOffset non-negative application reliable byte offset
   * @return wire reliable size in bytes
   * @throws IllegalArgumentException if {@code appReliableOffset < 0} or the resulting offset
   *     exceeds the 62-bit QUIC varint maximum
   */
  public static long computeResetReliableSize(
      boolean localInitiated, boolean bidirectional, long sessionId, long appReliableOffset) {
    if (appReliableOffset < 0L) {
      throw new IllegalArgumentException(
          "Application reliableSize must be non-negative: " + appReliableOffset);
    }
    int headerBytes = computeLocalStreamHeaderBytes(localInitiated, bidirectional, sessionId);
    if (appReliableOffset > MAX_QUIC_VAR_INT - headerBytes) {
      throw new IllegalArgumentException(
          "Computed wire reliableSize exceeds 62-bit QUIC varint limit: " + appReliableOffset);
    }
    return headerBytes + appReliableOffset;
  }
}

