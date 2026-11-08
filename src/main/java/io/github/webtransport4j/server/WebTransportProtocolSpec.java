package io.github.webtransport4j.server;

import org.jspecify.annotations.Nullable;

/**
 * Authoritative constants and pure validation predicates for IETF WebTransport over HTTP/3
 * ({@code draft-ietf-webtrans-http3-17}), HTTP/3 ({@code RFC 9114}), HTTP Datagrams and Capsules
 * ({@code RFC 9297}), Bootstrapping WebSockets with HTTP/2 and HTTP/3 ({@code RFC 8441} /
 * {@code RFC 9220}), and QUIC ({@code RFC 9000}).
 *
 * <p>Centralizing all RFC and specification-defined wire codepoints, error codes, stream types,
 * capsule identifiers, and invariant checks here establishes a single source of truth across the
 * codebase, decouples Netty channel handlers from scattered protocol literals, and simplifies
 * protocol conformance verification.
 */
public final class WebTransportProtocolSpec {

  /* =============================================================================================
   * HTTP/3 Settings Wire Codepoints (Draft-17 §9.2, RFC 9297 §2.1.1, RFC 8441 §3)
   * ============================================================================================= */

  /** RFC 8441 Section 3: {@code SETTINGS_ENABLE_CONNECT_PROTOCOL} wire codepoint ({@code 0x08}). */
  public static final long SETTING_ENABLE_CONNECT_PROTOCOL = 0x08L;

  /** RFC 9297 Section 2.1.1: {@code SETTINGS_H3_DATAGRAM} wire codepoint ({@code 0x33}). */
  public static final long SETTING_H3_DATAGRAM = 0x33L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_DATA} wire codepoint ({@code 0x2b61}). */
  public static final long SETTING_WT_INITIAL_MAX_DATA = 0x2b61L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_STREAMS_UNI} wire codepoint ({@code 0x2b64}). */
  public static final long SETTING_WT_INITIAL_MAX_STREAMS_UNI = 0x2b64L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI} wire codepoint ({@code 0x2b65}). */
  public static final long SETTING_WT_INITIAL_MAX_STREAMS_BIDI = 0x2b65L;

  /** Draft-17 Section 9.2: {@code SETTINGS_WT_ENABLED} wire codepoint ({@code 0x20b8933b}). */
  public static final long SETTING_WT_ENABLED = 0x20b8933bL;

  /** Legacy / multi-session extension setting {@code SETTINGS_WEBTRANSPORT_MAX_SESSIONS} ({@code 0x2b603742}). */
  public static final long SETTING_WEBTRANSPORT_MAX_SESSIONS = 0x2b603742L;

  /** Default comma-separated list of allowed non-standard HTTP/3 settings for Draft-17. */
  public static final String DEFAULT_ALLOWED_SETTINGS =
      "0x20b8933b,0x2b64,0x2b65,0x2b61,0x2b603742";

  /* =============================================================================================
   * Stream Type Identifiers (RFC 9114 §6.2, RFC 9204 §4.2, Draft-17 §9.3 & §9.4)
   * ============================================================================================= */

  /** RFC 9114 Section 6.2.1: HTTP/3 unidirectional Control Stream type ({@code 0x00}). */
  public static final long H3_STREAM_TYPE_CONTROL = 0x00L;

  /** RFC 9114 Section 6.2.2: HTTP/3 unidirectional Push Stream type ({@code 0x01}). */
  public static final long H3_STREAM_TYPE_PUSH = 0x01L;

  /** RFC 9204 Section 4.2: QPACK unidirectional encoder stream type ({@code 0x02}). */
  public static final long H3_STREAM_TYPE_QPACK_ENCODER = 0x02L;

  /** RFC 9204 Section 4.2: QPACK unidirectional decoder stream type ({@code 0x03}). */
  public static final long H3_STREAM_TYPE_QPACK_DECODER = 0x03L;

  /** Draft-17 Section 9.3: WebTransport bidirectional stream type prefix ({@code 0x41}). */
  public static final long WT_STREAM_TYPE_BIDI = 0x41L;

  /** Draft-17 Section 9.4: WebTransport unidirectional stream type ({@code 0x54}). */
  public static final long WT_STREAM_TYPE_UNI = 0x54L;

  /* =============================================================================================
   * HTTP/3 Frame Types (RFC 9114 §7.2)
   * ============================================================================================= */

  /** RFC 9114 Section 7.2.1: HTTP/3 {@code DATA} frame type ({@code 0x00}). */
  public static final long H3_FRAME_TYPE_DATA = 0x00L;

  /** RFC 9114 Section 7.2.2: HTTP/3 {@code HEADERS} frame type ({@code 0x01}). */
  public static final long H3_FRAME_TYPE_HEADERS = 0x01L;

  /** RFC 9114 Section 7.2.3: HTTP/3 {@code CANCEL_PUSH} frame type ({@code 0x03}). */
  public static final long H3_FRAME_TYPE_CANCEL_PUSH = 0x03L;

  /** RFC 9114 Section 7.2.4: HTTP/3 {@code SETTINGS} frame type ({@code 0x04}). */
  public static final long H3_FRAME_TYPE_SETTINGS = 0x04L;

  /** RFC 9114 Section 7.2.5: HTTP/3 {@code PUSH_PROMISE} frame type ({@code 0x05}). */
  public static final long H3_FRAME_TYPE_PUSH_PROMISE = 0x05L;

  /** RFC 9114 Section 7.2.6: HTTP/3 {@code GOAWAY} frame type ({@code 0x07}). */
  public static final long H3_FRAME_TYPE_GOAWAY = 0x07L;

  /** RFC 9114 Section 7.2.7: HTTP/3 {@code MAX_PUSH_ID} frame type ({@code 0x0d}). */
  public static final long H3_FRAME_TYPE_MAX_PUSH_ID = 0x0dL;

  /* =============================================================================================
   * Capsule Type Identifiers (Draft-17 §9.7, RFC 9297 §3.2, draft-ietf-webtrans-overview)
   * ============================================================================================= */

  /** Draft-17 Section 6.1 &amp; §9.7: {@code CLOSE_WEBTRANSPORT_SESSION} capsule type ({@code 0x2843}). */
  public static final long CAPSULE_CLOSE_WEBTRANSPORT_SESSION = 0x2843L;

  /** Draft-17 Section 9.7: {@code DRAIN_WEBTRANSPORT_SESSION} capsule type ({@code 0x78ae}). */
  public static final long CAPSULE_DRAIN_WEBTRANSPORT_SESSION = 0x78aeL;

  /** Draft-17 Section 5.6.4 &amp; §9.7: {@code WT_MAX_DATA} capsule type ({@code 0x190b4d3d}). */
  public static final long CAPSULE_WT_MAX_DATA = 0x190b4d3dL;

  /** Draft-17 Section 5.6.4 &amp; §9.7: {@code WT_MAX_STREAM_DATA} capsule type ({@code 0x190b4d3e}). */
  public static final long CAPSULE_WT_MAX_STREAM_DATA = 0x190b4d3eL;

  /** Draft-17 Section 5.6.2 &amp; §9.7: {@code WT_MAX_STREAMS} bidirectional capsule type ({@code 0x190b4d3f}). */
  public static final long CAPSULE_WT_MAX_STREAMS_BIDI = 0x190b4d3fL;

  /** Draft-17 Section 5.6.2 &amp; §9.7: {@code WT_MAX_STREAMS} unidirectional capsule type ({@code 0x190b4d40}). */
  public static final long CAPSULE_WT_MAX_STREAMS_UNI = 0x190b4d40L;

  /** Draft-17 Section 5.6.5 &amp; §9.7: {@code WT_DATA_BLOCKED} capsule type ({@code 0x190b4d41}). */
  public static final long CAPSULE_WT_DATA_BLOCKED = 0x190b4d41L;

  /** Draft-17 Section 5.6.5 &amp; §9.7: {@code WT_STREAM_DATA_BLOCKED} capsule type ({@code 0x190b4d42}). */
  public static final long CAPSULE_WT_STREAM_DATA_BLOCKED = 0x190b4d42L;

  /** Draft-17 Section 5.6.3 &amp; §9.7: {@code WT_STREAMS_BLOCKED} bidi capsule type ({@code 0x190b4d43}). */
  public static final long CAPSULE_WT_STREAMS_BLOCKED_BIDI = 0x190b4d43L;

  /** Draft-17 Section 5.6.3 &amp; §9.7: {@code WT_STREAMS_BLOCKED} uni capsule type ({@code 0x190b4d44}). */
  public static final long CAPSULE_WT_STREAMS_BLOCKED_UNI = 0x190b4d44L;

  /* =============================================================================================
   * WebTransport Protocol Error Codes (Draft-17 §9.5)
   * ============================================================================================= */

  /** Draft-17 Section 9.5: {@code WT_BUFFERED_STREAM_REJECTED} error code ({@code 0x3994bd84}). */
  public static final int WT_BUFFERED_STREAM_REJECTED = 0x3994bd84;

  /** Draft-17 Section 9.5: {@code WT_SESSION_GONE} error code ({@code 0x170d7b68}). */
  public static final int WT_SESSION_GONE = 0x170d7b68;

  /** Draft-17 Section 9.5: {@code WT_FLOW_CONTROL_ERROR} error code ({@code 0x045d4487}). */
  public static final int WT_FLOW_CONTROL_ERROR = 0x045d4487;

  /** Draft-17 Section 9.5: {@code WT_ALPN_ERROR} error code ({@code 0x0817b3dd}). */
  public static final int WT_ALPN_ERROR = 0x0817b3dd;

  /** Draft-17 Section 9.5: {@code WT_REQUIREMENTS_NOT_MET} error code ({@code 0x212c0d48}). */
  public static final int WT_REQUIREMENTS_NOT_MET = 0x212c0d48;

  /* =============================================================================================
   * WebTransport Application Error Code Mapping Range & GREASE (Draft-17 §4.4 & §9.5)
   * ============================================================================================= */

  /** Draft-17 Section 9.5: First HTTP/3 error code of the {@code WT_APPLICATION_ERROR} range. */
  public static final long WT_APPLICATION_ERROR_FIRST = 0x52e4a40fa8dbL;

  /** Draft-17 Section 9.5: Last HTTP/3 error code of the {@code WT_APPLICATION_ERROR} range. */
  public static final long WT_APPLICATION_ERROR_LAST = 0x52e5ac983162L;

  /** Alias for {@link #WT_APPLICATION_ERROR_FIRST} for backwards compatibility. */
  public static final long WT_ERROR_FIRST = WT_APPLICATION_ERROR_FIRST;

  /** Alias for {@link #WT_APPLICATION_ERROR_LAST} for backwards compatibility. */
  public static final long WT_ERROR_LAST = WT_APPLICATION_ERROR_LAST;

  /** RFC 9114 Section 8.1 / Draft-17 Section 4.4: GREASE remainder term {@code 0x21}. */
  public static final long WT_APPLICATION_ERROR_GREASE_REMAINDER = 0x21L;

  /** RFC 9114 Section 8.1 / Draft-17 Section 4.4: GREASE modulo step {@code 0x1f} (31). */
  public static final long WT_APPLICATION_ERROR_GREASE_MODULO = 31L;

  /* =============================================================================================
   * HTTP/3 Standard Error Codes (RFC 9114 §8.1, RFC 9297 §3.3)
   * ============================================================================================= */

  /** RFC 9114 Section 8.1: {@code H3_NO_ERROR} ({@code 0x0100}). */
  public static final int H3_NO_ERROR = 0x0100;

  /** RFC 9114 Section 8.1: {@code H3_GENERAL_PROTOCOL_ERROR} ({@code 0x0101}). */
  public static final int H3_GENERAL_PROTOCOL_ERROR = 0x0101;

  /** RFC 9114 Section 8.1: {@code H3_INTERNAL_ERROR} ({@code 0x0102}). */
  public static final int H3_INTERNAL_ERROR = 0x0102;

  /** RFC 9114 Section 8.1: {@code H3_STREAM_CREATION_ERROR} ({@code 0x0103}). */
  public static final int H3_STREAM_CREATION_ERROR = 0x0103;

  /** RFC 9114 Section 8.1: {@code H3_CLOSED_CRITICAL_STREAM} ({@code 0x0104}). */
  public static final int H3_CLOSED_CRITICAL_STREAM = 0x0104;

  /** RFC 9114 Section 8.1 / Draft-17 Section 4.3: {@code H3_FRAME_UNEXPECTED} ({@code 0x0105}). */
  public static final int H3_FRAME_UNEXPECTED = 0x0105;

  /** RFC 9114 Section 8.1: {@code H3_FRAME_ERROR} ({@code 0x0106}). */
  public static final int H3_FRAME_ERROR = 0x0106;

  /** RFC 9114 Section 8.1: {@code H3_EXCESSIVE_LOAD} ({@code 0x0107}). */
  public static final int H3_EXCESSIVE_LOAD = 0x0107;

  /** RFC 9114 Section 8.1: {@code H3_ID_ERROR} ({@code 0x0108}). */
  public static final int H3_ID_ERROR = 0x0108;

  /** RFC 9114 Section 8.1 / Draft-17 Section 3.1: {@code H3_SETTINGS_ERROR} ({@code 0x0109}). */
  public static final int H3_SETTINGS_ERROR = 0x0109;

  /** RFC 9114 Section 8.1: {@code H3_MISSING_SETTINGS} ({@code 0x010a}). */
  public static final int H3_MISSING_SETTINGS = 0x010a;

  /** RFC 9114 Section 8.1: {@code H3_REQUEST_REJECTED} ({@code 0x010b}). */
  public static final int H3_REQUEST_REJECTED = 0x010b;

  /** RFC 9114 Section 8.1: {@code H3_REQUEST_CANCELLED} ({@code 0x010c}). */
  public static final int H3_REQUEST_CANCELLED = 0x010c;

  /** RFC 9114 Section 8.1: {@code H3_REQUEST_INCOMPLETE} ({@code 0x010d}). */
  public static final int H3_REQUEST_INCOMPLETE = 0x010d;

  /** RFC 9297 Section 3.3 / RFC 9114 Section 8.1: {@code H3_MESSAGE_ERROR} ({@code 0x010e}). */
  public static final int H3_MESSAGE_ERROR = 0x010e;

  /** RFC 9114 Section 8.1: {@code H3_CONNECT_ERROR} ({@code 0x010f}). */
  public static final int H3_CONNECT_ERROR = 0x010f;

  /** RFC 9114 Section 8.1: {@code H3_VERSION_FALLBACK} ({@code 0x0110}). */
  public static final int H3_VERSION_FALLBACK = 0x0110;

  /* =============================================================================================
   * HTTP Upgrade Tokens and Header Names (Draft-17 §9.1 & §9.6)
   * ============================================================================================= */

  /** Draft-17 Section 9.1: Draft-17 HTTP/3 Connect protocol upgrade token {@code "webtransport-h3"}. */
  public static final String UPGRADE_TOKEN_H3 = "webtransport-h3";

  /** Legacy / Draft-16 upgrade token {@code "webtransport"}. */
  public static final String UPGRADE_TOKEN_LEGACY = "webtransport";

  /** Draft-17 Section 9.6: Available protocols header {@code "wt-available-protocols"}. */
  public static final String HEADER_WT_AVAILABLE_PROTOCOLS = "wt-available-protocols";

  /** Draft-17 Section 9.6: Selected protocol response header {@code "wt-protocol"}. */
  public static final String HEADER_WT_PROTOCOL = "wt-protocol";

  /* =============================================================================================
   * QUIC Variable-Length Integer and Protocol Limits (RFC 9000, Draft-17 §5.6.2, §6.1)
   * ============================================================================================= */

  /** RFC 9000 Section 16: Maximum 62-bit unsigned QUIC variable-length integer ({@code 2^62 - 1}). */
  public static final long MAX_QUIC_VAR_INT = 0x3FFFFFFFFFFFFFFFL;

  /** Draft-17 Section 5.6.2: Maximum legal {@code WT_MAX_STREAMS} value ({@code 2^60}). */
  public static final long MAX_WT_STREAMS_LIMIT = 1L << 60;

  /** Draft-17 Section 6.1: Maximum allowed byte length for {@code CLOSE_WEBTRANSPORT_SESSION} reason. */
  public static final int MAX_CLOSE_REASON_PHRASE_BYTES = 1024;

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
      Long candidateLimit, Long currentOrInitialLimit) {
    return candidateLimit != null
        && currentOrInitialLimit != null
        && candidateLimit > currentOrInitialLimit;
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
    return isValidHttpsScheme(scheme) && isValidAuthority(authority) && isValidPath(path);
  }

  private static boolean isValidHttpsScheme(@Nullable CharSequence scheme) {
    return scheme != null && "https".contentEquals(scheme);
  }

  private static boolean isValidAuthority(@Nullable CharSequence authority) {
    return authority != null && authority.length() > 0 && !containsPathSeparator(authority);
  }

  private static boolean isValidPath(@Nullable CharSequence path) {
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
      Boolean localInitiated, Boolean bidirectional, Long sessionId) {
    if (!Boolean.TRUE.equals(localInitiated)) {
      return 0;
    }
    long marker = Boolean.TRUE.equals(bidirectional) ? WT_STREAM_TYPE_BIDI : WT_STREAM_TYPE_UNI;
    long sid = sessionId != null ? sessionId : 0L;
    return WebTransportUtils.varIntLength(marker) + WebTransportUtils.varIntLength(sid);
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
      Boolean localInitiated, Boolean bidirectional, Long sessionId, Long appReliableOffset) {
    long offset = appReliableOffset != null ? appReliableOffset : 0L;
    if (offset < 0L) {
      throw new IllegalArgumentException(
          "Application reliableSize must be non-negative: " + offset);
    }
    int headerBytes = computeLocalStreamHeaderBytes(localInitiated, bidirectional, sessionId);
    if (offset > MAX_QUIC_VAR_INT - headerBytes) {
      throw new IllegalArgumentException(
          "Computed wire reliableSize exceeds 62-bit QUIC varint limit: " + offset);
    }
    return headerBytes + offset;
  }
}

