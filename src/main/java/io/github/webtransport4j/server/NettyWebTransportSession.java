package io.github.webtransport4j.server;

import io.github.webtransport4j.api.StreamPriority;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Netty-specific SPI extension of {@link WebTransportSession}.
 *
 * <p>Exposes low-level Netty QUIC channel management, draft-16 flow control counters, capsule
 * state, and internal stream tracking required by the Netty server engine handlers.
 *
 * @author https://github.com/sanjomo
 */
public interface NettyWebTransportSession extends WebTransportSession {

  /**
   * Returns the underlying HTTP/3 CONNECT stream channel for this session.
   *
   * @return the QuicStreamChannel of the CONNECT stream
   */
  @NonNull QuicStreamChannel getConnectStream();

  /**
   * Updates the underlying CONNECT stream channel (e.g. during migration or resumption).
   *
   * @param newConnectStream the new QuicStreamChannel
   */
  void updateConnectStream(@NonNull QuicStreamChannel newConnectStream);

  /** Marks this session as draining upon receiving a {@code WT_DRAIN_SESSION} capsule. */
  void markDraining();

  /**
   * Returns the timestamp (in ms) of the last read activity on this session.
   *
   * @return last read timestamp
   */
  long getLastReadTime();

  /** Updates the timestamp of the last read activity to the current system time. */
  void updateLastReadTime();

  /**
   * Returns the set of active client-initiated unidirectional streams.
   *
   * @return set of active stream channels
   */
  @NonNull Set<QuicStreamChannel> getActiveClientInitiatedUni();

  /**
   * Returns the set of active server-initiated unidirectional streams.
   *
   * @return set of active stream channels
   */
  @NonNull Set<QuicStreamChannel> getActiveServerInitiatedUni();

  /**
   * Returns the set of active client-initiated bidirectional streams.
   *
   * @return set of active stream channels
   */
  @NonNull Set<QuicStreamChannel> getActiveClientInitiatedBi();

  /**
   * Returns the set of active server-initiated bidirectional streams.
   *
   * @return set of active stream channels
   */
  @NonNull Set<QuicStreamChannel> getActiveServerInitiatedBi();

  /**
   * Returns all active unidirectional and bidirectional WebTransport streams for this session.
   *
   * @return set of all active QuicStreamChannel instances
   */
  @NonNull Set<QuicStreamChannel> getAllActiveWebTransportStreams();

  /**
   * Returns true if session-level flow control is enabled.
   *
   * @return true if flow control was negotiated
   */
  boolean isFlowControlEnabled();

  /**
   * Sets whether session-level flow control is enabled.
   *
   * @param enabled true to enable flow control, false otherwise
   */
  void setFlowControlEnabled(boolean enabled);

  /**
   * Returns our local maximum unidirectional streams limit advertised to the peer.
   *
   * @return local max uni streams limit
   */
  long getSettingsMaxStreamsUni();

  /**
   * Sets our local maximum unidirectional streams limit.
   *
   * @param value new limit
   */
  void setSettingsMaxStreamsUni(long value);

  /**
   * Returns our local maximum bidirectional streams limit advertised to the peer.
   *
   * @return local max bidi streams limit
   */
  long getSettingsMaxStreamsBidi();

  /**
   * Sets our local maximum bidirectional streams limit.
   *
   * @param value new limit
   */
  void setSettingsMaxStreamsBidi(long value);

  /**
   * Returns our local maximum data limit advertised to the peer.
   *
   * @return local max data limit
   */
  long getSettingsMaxData();

  /**
   * Sets our local maximum data limit.
   *
   * @param value new limit
   */
  void setSettingsMaxData(long value);

  /**
   * Returns the peer's maximum unidirectional streams limit.
   *
   * @return peer max uni streams limit
   */
  long getPeerSettingsMaxStreamsUni();

  /**
   * Sets the peer's maximum unidirectional streams limit.
   *
   * @param value new limit
   */
  void setPeerSettingsMaxStreamsUni(long value);

  /**
   * Returns the peer's maximum bidirectional streams limit.
   *
   * @return peer max bidi streams limit
   */
  long getPeerSettingsMaxStreamsBidi();

  /**
   * Sets the peer's maximum bidirectional streams limit.
   *
   * @param value new limit
   */
  void setPeerSettingsMaxStreamsBidi(long value);

  /**
   * Returns the peer's maximum data limit.
   *
   * @return peer max data limit
   */
  long getPeerSettingsMaxData();

  /**
   * Sets the peer's maximum data limit.
   *
   * @param value new limit
   */
  void setPeerSettingsMaxData(long value);

  /**
   * Marks that a WT_MAX_DATA capsule has been received from the peer.
   *
   * @return true if this was the first time marked
   */
  boolean markPeerMaxDataCapsuleReceived();

  /**
   * Returns the cumulative number of client-initiated unidirectional streams.
   *
   * @return stream count
   */
  long getClientInitiatedStreamsUni();

  /**
   * Sets the cumulative number of client-initiated unidirectional streams.
   *
   * @param clientInitiatedStreamsUni stream count
   */
  void setClientInitiatedStreamsUni(long clientInitiatedStreamsUni);

  /**
   * Returns the cumulative number of client-initiated bidirectional streams.
   *
   * @return stream count
   */
  long getClientInitiatedStreamsBidi();

  /**
   * Sets the cumulative number of client-initiated bidirectional streams.
   *
   * @param clientInitiatedStreamsBidi stream count
   */
  void setClientInitiatedStreamsBidi(long clientInitiatedStreamsBidi);

  /**
   * Atomically increments and returns the cumulative client-initiated bidirectional stream count.
   *
   * @return incremented stream count
   */
  long incrementAndGetClientInitiatedStreamsBidi();

  /**
   * Atomically increments and returns the cumulative client-initiated unidirectional stream count.
   *
   * @return incremented stream count
   */
  long incrementAndGetClientInitiatedStreamsUni();

  /**
   * Returns initial maximum unidirectional streams limit set at session inception.
   *
   * @return initial max uni streams
   */
  long getInitialMaxStreamsUni();

  /**
   * Returns initial maximum bidirectional streams limit set at session inception.
   *
   * @return initial max bidi streams
   */
  long getInitialMaxStreamsBidi();

  /**
   * Returns initial maximum data limit set at session inception.
   *
   * @return initial max data
   */
  long getInitialMaxData();

  /**
   * Sets the negotiated application subprotocol.
   *
   * @param subprotocol the subprotocol name
   */
  void setSubprotocol(@Nullable String subprotocol);

  /**
   * Returns cumulative server-initiated unidirectional stream count.
   *
   * @return stream count
   */
  long getServerInitiatedStreamsUni();

  /**
   * Returns cumulative server-initiated bidirectional stream count.
   *
   * @return stream count
   */
  long getServerInitiatedStreamsBidi();

  /**
   * Atomically increments and returns cumulative server-initiated unidirectional stream count.
   *
   * @return incremented stream count
   */
  long incrementAndGetServerInitiatedStreamsUni();

  /**
   * Atomically increments and returns cumulative server-initiated bidirectional stream count.
   *
   * @return incremented stream count
   */
  long incrementAndGetServerInitiatedStreamsBidi();

  /**
   * Returns cumulative bytes sent on this session.
   *
   * @return bytes sent
   */
  long getCumulativeBytesSent();

  /**
   * Returns cumulative bytes received on this session.
   *
   * @return bytes received
   */
  long getCumulativeBytesReceived();

  /**
   * Atomically increments cumulative bytes sent and returns the updated value.
   *
   * @param value bytes to add
   * @return updated bytes sent
   */
  long incrementCumulativeBytesSent(long value);

  /**
   * Atomically increments cumulative bytes received and returns the updated value.
   *
   * @param value bytes to add
   * @return updated bytes received
   */
  long incrementCumulativeBytesReceived(long value);

  /**
   * Returns the AtomicLong tracking the last peer limit for which a WT_DATA_BLOCKED capsule was
   * sent.
   *
   * @return AtomicLong tracking data blocked limit
   */
  @NonNull AtomicLong getLastSentDataBlockedLimit();

  /**
   * Resets a WebTransport data stream with an application error code mapped per Section 4.4.
   *
   * @param dataStream the stream channel
   * @param appErrorCode the application error code
   */
  void resetStream(@NonNull QuicStreamChannel dataStream, long appErrorCode);

  /**
   * Sets the session close code.
   *
   * @param closeCode close error code
   */
  void setCloseCode(int closeCode);

  /** Rotates the session resumption token. */
  void rotateResumptionToken();

  /**
   * Creates an outbound unidirectional stream with a custom Netty channel handler.
   *
   * @param streamHandler custom stream pipeline handler
   * @return future completing with the stream
   */
  @NonNull CompletableFuture<WebTransportStream> createUniStream(
      @NonNull ChannelHandler streamHandler);

  /**
   * Creates an outbound unidirectional stream with a custom Netty channel handler and priority.
   *
   * @param streamHandler custom stream pipeline handler
   * @param priority stream priority
   * @return future completing with the stream
   */
  @NonNull CompletableFuture<WebTransportStream> createUniStream(
      @NonNull ChannelHandler streamHandler, @NonNull StreamPriority priority);

  /**
   * Creates an outbound bidirectional stream with a custom Netty channel handler.
   *
   * @param streamHandler custom stream pipeline handler
   * @return future completing with the stream
   */
  @NonNull CompletableFuture<WebTransportStream> createBiStream(
      @NonNull ChannelHandler streamHandler);

  /**
   * Creates an outbound bidirectional stream with a custom Netty channel handler and priority.
   *
   * @param streamHandler custom stream pipeline handler
   * @param priority stream priority
   * @return future completing with the stream
   */
  @NonNull CompletableFuture<WebTransportStream> createBiStream(
      @NonNull ChannelHandler streamHandler, @NonNull StreamPriority priority);
}
