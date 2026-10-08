package io.github.webtransport4j.api;

import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Represents a WebTransport session and manages its streams and datagrams.
 *
 * <p>A WebTransport session is established over an HTTP/3 extended CONNECT stream. It allows
 * multiplexing bidirectional and unidirectional reliable streams alongside unreliable datagrams
 * within the same secure session.
 *
 * @author https://github.com/sanjomo
 */
public interface WebTransportSession {

  /**
   * Returns the session stream ID (the HTTP/3 CONNECT stream ID that established the session).
   *
   * @return the unique session stream ID
   */
  long getSessionStreamId();

  /**
   * Alias for {@link #getSessionStreamId()}.
   *
   * @return the session ID
   */
  default long sessionId() {
    return getSessionStreamId();
  }

  /**
   * Returns a unique positive identifier for this session instance within the JVM / server.
   *
   * @return unique session identifier
   */
  default long getUniqueSessionId() {
    return Math.abs((long) System.identityHashCode(this));
  }

  /**
   * Returns the URI path associated with this session.
   *
   * @return the URI request path
   */
  @NonNull String path();

  /**
   * Returns the negotiated application subprotocol, or {@code null} if no subprotocol was selected.
   *
   * @return the selected subprotocol name, or null
   */
  @Nullable String getSubprotocol();

  /**
   * Returns the session resumption token.
   *
   * @return the session resumption token
   */
  @NonNull String getResumptionToken();

  /**
   * Returns true if graceful shutdown was signaled locally or by the peer.
   *
   * @return true if the session is draining
   */
  boolean isDraining();

  /**
   * Marks this session as draining upon receiving or sending a {@code WT_DRAIN_SESSION} capsule.
   */
  default void markDraining() {}

  /**
   * Begins graceful draining. New streams and datagrams remain permitted until the session closes;
   * applications should finish their work and terminate promptly (Draft-16 Section 4.7).
   *
   * @throws UnsupportedOperationException when the implementation does not support draining
   */
  default void drain() {
    throw new UnsupportedOperationException("Session draining is not supported");
  }

  /**
   * Returns true if draining was signaled and there are currently no active streams. This is a
   * point-in-time predicate, not a lifecycle state; the session stays open until explicitly closed.
   *
   * @return true if the draining session currently has no active streams
   */
  default boolean isDrained() {
    return isDraining() && getActiveStreams().isEmpty();
  }

  /**
   * Returns true if this session is open and active.
   *
   * @return true if the session is open
   */
  default boolean isOpen() {
    return true;
  }

  /**
   * Creates an outbound unidirectional stream with the default pipeline.
   *
   * @return a future that completes with the opened stream
   */
  @NonNull CompletableFuture<WebTransportStream> createUniStream();

  /**
   * Creates an outbound unidirectional stream with the given priority per RFC 9218.
   *
   * @param priority the stream priority
   * @return a future that completes with the opened stream
   */
  @NonNull CompletableFuture<WebTransportStream> createUniStream(@NonNull StreamPriority priority);

  /**
   * Creates an outbound unidirectional stream with the given urgency and incremental flag per RFC
   * 9218.
   *
   * @param urgency urgency level between 0 (highest) and 7 (lowest)
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a future that completes with the opened stream
   */
  default @NonNull CompletableFuture<WebTransportStream> createUniStream(
      int urgency, boolean incremental) {
    return createUniStream(StreamPriority.of(urgency, incremental));
  }

  /**
   * Creates an outbound bidirectional stream with the default pipeline.
   *
   * @return a future that completes with the opened stream
   */
  @NonNull CompletableFuture<WebTransportStream> createBiStream();

  /**
   * Creates an outbound bidirectional stream with the given priority per RFC 9218.
   *
   * @param priority the stream priority
   * @return a future that completes with the opened stream
   */
  @NonNull CompletableFuture<WebTransportStream> createBiStream(@NonNull StreamPriority priority);

  /**
   * Creates an outbound bidirectional stream with the given urgency and incremental flag per RFC
   * 9218.
   *
   * @param urgency urgency level between 0 (highest) and 7 (lowest)
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a future that completes with the opened stream
   */
  default @NonNull CompletableFuture<WebTransportStream> createBiStream(
      int urgency, boolean incremental) {
    return createBiStream(StreamPriority.of(urgency, incremental));
  }

  /**
   * Sends a datagram packet over the WebTransport session.
   *
   * @param data the datagram payload buffer
   */
  void sendDatagram(@NonNull WebTransportBuffer data);

  /**
   * Sends a datagram packet over the WebTransport session.
   *
   * @param data the datagram payload byte array
   */
  void sendDatagram(byte @NonNull [] data);

  /**
   * Exports keying material for this WebTransport session using the TLS Exporter mechanism defined
   * in draft-16 Section 4.8.
   *
   * @param label the application-supplied exporter label
   * @param context optional application-supplied exporter context (can be null)
   * @param length the desired length of exported keying material in bytes
   * @return the exported keying material bytes
   */
  byte[] exportKeyingMaterial(@NonNull String label, byte @Nullable [] context, int length);

  /**
   * Gracefully closes the WebTransport session by closing the CONNECT stream and all active
   * streams.
   */
  void close();

  /**
   * Abruptly closes the WebTransport session by resetting the CONNECT stream with the specified
   * HTTP/3 error code and resetting all active data streams.
   *
   * @param httpErrorCode the HTTP/3 error code
   */
  void abort(long httpErrorCode);

  /**
   * Registers a listener to be invoked when the session is closed.
   *
   * @param onClosedCallback the callback listener
   */
  void setOnClosedCallback(@Nullable OnCloseListener onClosedCallback);

  /**
   * Returns the close code for this session (0 = graceful by default).
   *
   * @return the session close code
   */
  default int getCloseCode() {
    return 0;
  }

  /**
   * Returns the closure reason string if provided by the peer, or {@code null}.
   *
   * @return the session closure reason, or null
   */
  default @Nullable String getCloseReason() {
    return null;
  }

  /**
   * Returns the peer certificates presented during the TLS handshake, or an empty array if no
   * client certificates were presented or verified.
   *
   * @return array of client certificates, or empty array if none
   */
  default Certificate[] getPeerCertificates() {
    return new Certificate[0];
  }

  /**
   * Returns the remote network address of the connected peer, or {@code null} if unknown.
   *
   * @return the remote socket address, or null
   */
  default @Nullable SocketAddress getRemoteAddress() {
    return null;
  }

  /**
   * Returns the local network address the session is bound to, or {@code null} if unknown.
   *
   * @return the local socket address, or null
   */
  default @Nullable SocketAddress getLocalAddress() {
    return null;
  }

  /**
   * Returns the cumulative total of payload bytes transmitted over this session.
   *
   * @return bytes sent
   */
  default long getCumulativeBytesSent() {
    return 0L;
  }

  /**
   * Returns the cumulative total of payload bytes received over this session.
   *
   * @return bytes received
   */
  default long getCumulativeBytesReceived() {
    return 0L;
  }

  /**
   * Returns true if session-level flow control is enabled.
   *
   * @return true if flow control is enabled
   */
  default boolean isFlowControlEnabled() {
    return true;
  }

  /**
   * Returns our local maximum unidirectional streams limit advertised to the peer.
   *
   * @return local max uni streams limit
   */
  default long getSettingsMaxStreamsUni() {
    return 0L;
  }

  /**
   * Returns our local maximum bidirectional streams limit advertised to the peer.
   *
   * @return local max bidi streams limit
   */
  default long getSettingsMaxStreamsBidi() {
    return 0L;
  }

  /**
   * Returns our local maximum data limit in bytes advertised to the peer.
   *
   * @return local max data limit
   */
  default long getSettingsMaxData() {
    return 0L;
  }

  /**
   * Returns the peer's maximum unidirectional streams limit.
   *
   * @return peer max uni streams limit
   */
  default long getPeerSettingsMaxStreamsUni() {
    return 0L;
  }

  /**
   * Returns the peer's maximum bidirectional streams limit.
   *
   * @return peer max bidi streams limit
   */
  default long getPeerSettingsMaxStreamsBidi() {
    return 0L;
  }

  /**
   * Returns the peer's maximum data limit in bytes.
   *
   * @return peer max data limit
   */
  default long getPeerSettingsMaxData() {
    return 0L;
  }

  /**
   * Returns an unmodifiable collection of all active WebTransport stream summaries, or an empty
   * collection if stream enumeration is not supported.
   *
   * @return collection of stream summaries
   */
  default @NonNull Collection<WebTransportStreamSummary> getActiveStreams() {
    return Collections.emptyList();
  }
}
