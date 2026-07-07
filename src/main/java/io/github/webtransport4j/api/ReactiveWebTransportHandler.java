package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;

/**
 * A reactive handler for WebTransport sessions.
 * Extends reactive hooks for session lifecycles, incoming streams, and datagrams,
 * returning Publisher&lt;Void&gt; to cleanly bind processing pipelines in an agnostic way.
 */
public interface ReactiveWebTransportHandler {

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
   * Invoked when a datagram is received from the client.
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
