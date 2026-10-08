package io.github.webtransport4j.api;

import java.net.SocketAddress;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

/**
 * A reactive handler for WebTransport sessions. Extends reactive hooks for session lifecycles,
 * incoming streams, and datagrams, returning Publisher&lt;Void&gt; to cleanly bind processing
 * pipelines in an agnostic way.
 */
public interface ReactiveWebTransportHandler {

  /**
   * Invoked when an extended CONNECT session request is received before admission.
   *
   * @param requestContext the incoming request context
   * @return true to admit the session, false to reject with 403 Forbidden
   */
  default boolean onSessionRequest(@NonNull SessionRequestContext requestContext) {
    return true;
  }

  /**
   * Invoked when a WebTransport session is successfully established.
   *
   * @param session the reactive session.
   * @return a Publisher that completes when initialization logic is done.
   */
  default @NonNull Publisher<Void> onSessionReady(@NonNull ReactiveWebTransportSession session) {
    return EmptyPublisher.instance();
  }

  /**
   * Invoked when a WebTransport session is closed.
   *
   * @param session the reactive session.
   * @return a Publisher that completes when cleanup logic is done.
   */
  default @NonNull Publisher<Void> onSessionClosed(@NonNull ReactiveWebTransportSession session) {
    return EmptyPublisher.instance();
  }

  /**
   * Invoked when a WebTransport session is closed with status code and diagnostic reason.
   * By default, delegates to {@link #onSessionClosed(ReactiveWebTransportSession)}.
   *
   * @param session the reactive session
   * @param closeCode the session termination status code (0 = graceful)
   * @param reason the closure reason string, or null
   * @return a Publisher that completes when cleanup logic is done
   */
  default @NonNull Publisher<Void> onSessionClosed(
      @NonNull ReactiveWebTransportSession session, int closeCode, @Nullable String reason) {
    return onSessionClosed(session);
  }

  /**
   * Invoked when an unhandled transport, decoding, or execution error occurs on the session.
   *
   * @param session the reactive session
   * @param cause the error cause
   * @return a Publisher that completes when error handling is done
   */
  default @NonNull Publisher<Void> onError(
      @NonNull ReactiveWebTransportSession session, @NonNull Throwable cause) {
    return EmptyPublisher.instance();
  }

  /**
   * Invoked when the client peer migrates to a new network path (IP address or port change).
   *
   * @param session the reactive session
   * @param oldAddress the previous remote client socket address
   * @param newAddress the new remote client socket address
   * @return a Publisher that completes when migration notification handling is done
   */
  default @NonNull Publisher<Void> onConnectionMigration(
      @NonNull ReactiveWebTransportSession session,
      @NonNull SocketAddress oldAddress,
      @NonNull SocketAddress newAddress) {
    return EmptyPublisher.instance();
  }

  /**
   * Invoked when a client initiates a new unidirectional or bidirectional stream.
   *
   * @param session the reactive session.
   * @param stream the reactive stream.
   * @return a Publisher that completes when stream processing is done.
   */
  default @NonNull Publisher<Void> onIncomingStream(
      @NonNull ReactiveWebTransportSession session, @NonNull ReactiveWebTransportStream stream) {
    return EmptyPublisher.instance();
  }

  /**
   * Invoked when a datagram is received from the client. The buffer is borrowed for this callback.
   * For asynchronous processing, retain a separate reference before returning and close it when
   * processing finishes. The reference delivered through {@link
   * ReactiveWebTransportSession#receiveDatagrams()} belongs to its subscriber.
   *
   * @param session the reactive session.
   * @param data the received datagram payload buffer.
   * @return a Publisher that completes when datagram processing is done.
   */
  default @NonNull Publisher<Void> onDatagramReceived(
      @NonNull ReactiveWebTransportSession session, @NonNull WebTransportBuffer data) {
    return EmptyPublisher.instance();
  }
}
