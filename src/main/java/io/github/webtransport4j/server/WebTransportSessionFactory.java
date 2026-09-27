package io.github.webtransport4j.server;

import io.netty.handler.codec.quic.QuicStreamChannel;
import org.jspecify.annotations.NonNull;

/**
 * Factory for creating {@link NettyWebTransportSession} instances.
 * Allows decoupling session creation from session management and provides extensibility for testing.
 *
 * @author https://github.com/sanjomo
 */
@FunctionalInterface
public interface WebTransportSessionFactory {

  /**
   * Creates a new {@link NettyWebTransportSession}.
   *
   * @param sessionStreamId session stream ID
   * @param connectStream CONNECT stream channel
   * @param path request path
   * @param maxStreamsUni local max uni streams limit
   * @param maxStreamsBidi local max bidi streams limit
   * @param maxData local max data limit
   * @param peerMaxStreamsUni peer max uni streams limit
   * @param peerMaxStreamsBidi peer max bidi streams limit
   * @param peerMaxData peer max data limit
   * @param peerMaxDataNegotiated true if peer max data was negotiated
   * @param flowControlEnabled true if flow control is enabled
   * @return newly created session instance
   */
  @NonNull NettyWebTransportSession createSession(
      long sessionStreamId,
      @NonNull QuicStreamChannel connectStream,
      @NonNull String path,
      long maxStreamsUni,
      long maxStreamsBidi,
      long maxData,
      long peerMaxStreamsUni,
      long peerMaxStreamsBidi,
      long peerMaxData,
      boolean peerMaxDataNegotiated,
      boolean flowControlEnabled);
}
