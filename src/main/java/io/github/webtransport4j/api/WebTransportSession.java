package io.github.webtransport4j.api;

import io.github.webtransport4j.server.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.concurrent.Future;
import java.util.HashSet;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.concurrent.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:21 am
 */
public class WebTransportSession {

  // Default initial capacity for stream tracking sets.
  // Most sessions have few concurrent streams; avoids 16-bucket default of ConcurrentHashMap.
  private static final int STREAM_SET_INITIAL_CAPACITY = 4;



  private final long sessionStreamId;
  private final String path;
  private final QuicStreamChannel connectStream;

  private final Set<QuicStreamChannel> activeClientInitiatedUni;
  private final Set<QuicStreamChannel> activeServerInitiatedUni;
  private final Set<QuicStreamChannel> activeClientInitiatedBi;
  private final Set<QuicStreamChannel> activeServerInitiatedBi;

  // Local stream limits (how many streams we allow the client to initiate)
  private final AtomicLong settingsMaxStreamsUni;
  private final AtomicLong settingsMaxStreamsBidi;
  private final AtomicLong settingsMaxData;

  // Peer stream limits (how many streams the client allows the server to
  // initiate)
  private final AtomicLong peerSettingsMaxStreamsUni;
  private final AtomicLong peerSettingsMaxStreamsBidi;
  private final AtomicLong peerSettingsMaxData;

  // Cumulative stream counters for streams initiated by the Client
  private final AtomicLong clientInitiatedStreamsUni = new AtomicLong(0L);
  private final AtomicLong clientInitiatedStreamsBidi = new AtomicLong(0L);

  // Cumulative stream counters for streams initiated by the Server
  private final AtomicLong serverInitiatedStreamsUni = new AtomicLong(0L);
  private final AtomicLong serverInitiatedStreamsBidi = new AtomicLong(0L);

  // Flow control fields — only allocated when flowControlEnabled is true
  private final AtomicLong cumulativeBytesSent;
  private final AtomicLong cumulativeBytesReceived;
  private final AtomicLong lastSentDataBlockedLimit;

  private final boolean flowControlEnabled;

  // Initial allowed concurrent limits set at the start of the session
  private final long initialMaxStreamsUni;
  private final long initialMaxStreamsBidi;
  private final long initialMaxData;

  public WebTransportSession(
      long sessionStreamId,
      @NotNull QuicStreamChannel connectStream,
      @NotNull String path,
      long maxStreamsUni,
      long maxStreamsBidi,
      long maxData,
      long peerMaxStreamsUni,
      long peerMaxStreamsBidi,
      long peerMaxData,
      boolean flowControlEnabled) {
    this.sessionStreamId = sessionStreamId;
    this.path = path;
    this.connectStream = connectStream;
    this.flowControlEnabled = flowControlEnabled;

    // Stream limits — always needed
    this.settingsMaxStreamsUni = new AtomicLong(maxStreamsUni);
    this.settingsMaxStreamsBidi = new AtomicLong(maxStreamsBidi);
    this.settingsMaxData = new AtomicLong(maxData);
    this.initialMaxStreamsUni = maxStreamsUni;
    this.initialMaxStreamsBidi = maxStreamsBidi;
    this.initialMaxData = maxData;

    this.peerSettingsMaxStreamsUni = new AtomicLong(peerMaxStreamsUni);
    this.peerSettingsMaxStreamsBidi = new AtomicLong(peerMaxStreamsBidi);
    this.peerSettingsMaxData = new AtomicLong(peerMaxData);

    // Active stream sets — use small initial capacity to reduce memory footprint
    this.activeClientInitiatedBi = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeServerInitiatedBi = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeClientInitiatedUni = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeServerInitiatedUni = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);

    // Flow control objects — skip allocation when disabled
    if (flowControlEnabled) {
      this.cumulativeBytesSent = new AtomicLong(0L);
      this.cumulativeBytesReceived = new AtomicLong(0L);
      this.lastSentDataBlockedLimit = new AtomicLong(-1L);
    } else {
      this.cumulativeBytesSent = null;
      this.cumulativeBytesReceived = null;
      this.lastSentDataBlockedLimit = null;
    }
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedUni() {
    return activeClientInitiatedUni;
  }

  public Set<WebTransportStream> getClientInitiatedUniStreams() {
    return toWebTransportStreams(activeClientInitiatedUni);
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedUni() {
    return activeServerInitiatedUni;
  }

  public Set<WebTransportStream> getServerInitiatedUniStreams() {
    return toWebTransportStreams(activeServerInitiatedUni);
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedBi() {
    return activeClientInitiatedBi;
  }

  public Set<WebTransportStream> getClientInitiatedBiStreams() {
   return toWebTransportStreams(activeClientInitiatedBi);
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedBi() {
    return activeServerInitiatedBi;
  }

  public Set<WebTransportStream> getServerInitiatedBiStreams() {
    return toWebTransportStreams(activeServerInitiatedBi);
  }

  private Set<WebTransportStream> toWebTransportStreams(Set<QuicStreamChannel> quicStreamChannelSet) {
    Set<WebTransportStream> streams = new HashSet<>();
    for (QuicStreamChannel ch : quicStreamChannelSet) {
      WebTransportStream s = ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (s != null) {
        streams.add(s);
      }
    }
    return streams;
  }

  public boolean isFlowControlEnabled() {
    return flowControlEnabled;
  }

  public long getSessionStreamId() {
    return sessionStreamId;
  }

  public QuicStreamChannel getConnectStream() {
    return connectStream;
  }

  public long getSettingsMaxStreamsUni() {
    return settingsMaxStreamsUni.get();
  }

  public long getSettingsMaxStreamsBidi() {
    return settingsMaxStreamsBidi.get();
  }

  public long getSettingsMaxData() {
    return settingsMaxData.get();
  }

  public long getPeerSettingsMaxStreamsUni() {
    return peerSettingsMaxStreamsUni.get();
  }

  public void setPeerSettingsMaxStreamsUni(long value) {
    this.peerSettingsMaxStreamsUni.set(value);
  }

  public long getPeerSettingsMaxStreamsBidi() {
    return peerSettingsMaxStreamsBidi.get();
  }

  public void setPeerSettingsMaxStreamsBidi(long value) {
    this.peerSettingsMaxStreamsBidi.set(value);
  }

  public long getPeerSettingsMaxData() {
    return peerSettingsMaxData.get();
  }

  public void setPeerSettingsMaxData(long value) {
    this.peerSettingsMaxData.set(value);
  }

  public long getClientInitiatedStreamsUni() {
    return clientInitiatedStreamsUni.get();
  }

  public long getClientInitiatedStreamsBidi() {
    return clientInitiatedStreamsBidi.get();
  }

  public long incrementAndGetClientInitiatedStreamsBidi() {
    return clientInitiatedStreamsBidi.incrementAndGet();
  }

  public long incrementAndGetClientInitiatedStreamsUni() {
    return clientInitiatedStreamsUni.incrementAndGet();
  }

  public void setClientInitiatedStreamsUni(long clientInitiatedStreamsUni) {
    this.clientInitiatedStreamsUni.set(clientInitiatedStreamsUni);
  }

  public void setClientInitiatedStreamsBidi(long clientInitiatedStreamsBidi) {
    this.clientInitiatedStreamsBidi.set(clientInitiatedStreamsBidi);
  }

  public long getInitialMaxStreamsUni() {
    return initialMaxStreamsUni;
  }

  public long getInitialMaxStreamsBidi() {
    return initialMaxStreamsBidi;
  }

  public void setSettingsMaxStreamsUni(long value) {
    this.settingsMaxStreamsUni.set(value);
  }

  public void setSettingsMaxStreamsBidi(long value) {
    this.settingsMaxStreamsBidi.set(value);
  }

  public long getServerInitiatedStreamsUni() {
    return serverInitiatedStreamsUni.get();
  }

  public long getServerInitiatedStreamsBidi() {
    return serverInitiatedStreamsBidi.get();
  }

  public long incrementAndGetServerInitiatedStreamsUni() {
    return serverInitiatedStreamsUni.incrementAndGet();
  }

  public long incrementAndGetServerInitiatedStreamsBidi() {
    return serverInitiatedStreamsBidi.incrementAndGet();
  }

  public long getInitialMaxData() {
    return initialMaxData;
  }

  public void setSettingsMaxData(long value) {
    this.settingsMaxData.set(value);
  }

  public long getCumulativeBytesSent() {
    return cumulativeBytesSent.get();
  }

  public long getCumulativeBytesReceived() {
    return cumulativeBytesReceived.get();
  }

  public long incrementCumulativeBytesSent(long value) {
    return this.cumulativeBytesSent.addAndGet(value);
  }

  public long incrementCumulativeBytesReceived(long value) {
    return this.cumulativeBytesReceived.addAndGet(value);
  }

  /**
   * Returns the AtomicLong tracking the last peer limit for which a WT_DATA_BLOCKED capsule was
   * sent. Callers use CAS operations on this.
   */
  public AtomicLong getLastSentDataBlockedLimit() {
    return lastSentDataBlockedLimit;
  }

  /** Gracefully closes the WebTransport session by closing the CONNECT stream. */
  public void close() {
    connectStream.close();
  }

  /**
   * Abruptly closes the WebTransport session by resetting the CONNECT stream with the specified
   * HTTP/3 error code and resetting all active data streams.
   */
  public void abort(long httpErrorCode) {
    int code = (int) httpErrorCode;
    if (code < 0) {
      code = 0; // fallback to safe code to prevent native JVM crash
    }
    connectStream.shutdown(code, connectStream.newPromise());

    // Reset all associated data streams
    for (QuicStreamChannel activeStream : activeClientInitiatedBi) {
      activeStream.shutdown(code, activeStream.newPromise());
    }
    for (QuicStreamChannel activeStream : activeServerInitiatedBi) {
      activeStream.shutdown(code, activeStream.newPromise());
    }
    for (QuicStreamChannel activeStream : activeClientInitiatedUni) {
      activeStream.shutdown(code, activeStream.newPromise());
    }
    for (QuicStreamChannel activeStream : activeServerInitiatedUni) {
      activeStream.shutdown(code, activeStream.newPromise());
    }
  }

  /**
   * Resets a WebTransport data stream with a WebTransport application error code (automatically
   * mapped to the HTTP/3 error range as per Section 4.4).
   */
  public void resetStream(QuicStreamChannel dataStream, long appErrorCode) {
    WebTransportUtils.resetStream(dataStream, appErrorCode);
  }

  /**
   * Sends a datagram package over the WebTransport session.
   *
   * @param data The datagram payload.
   */
  public void sendDatagram(ByteBuf data) {
    Channel parentChannel = connectStream.parent();
    ByteBuf header = parentChannel.alloc().directBuffer();
    WebTransportUtils.writeVarInt(header, sessionStreamId);
    
    CompositeByteBuf composite = parentChannel.alloc().compositeBuffer(2);
    composite.addComponent(true, header);
    composite.addComponent(true, data);

    parentChannel.writeAndFlush(composite);
  }

  /**
   * Sends a datagram package over the WebTransport session.
   *
   * @param data The datagram payload byte array.
   * @return A future that is notified when the write completes.
   */
  public Future<Void> sendDatagram(byte[] data) {
    Channel parentChannel = connectStream.parent();
    ByteBuf buffer = parentChannel.alloc().directBuffer();
    WebTransportUtils.writeVarInt(buffer, sessionStreamId);
    buffer.writeBytes(data);
    return parentChannel.writeAndFlush(buffer);
  }




  public String path() {
    return path;
  }

  public static final ChannelHandler DEFAULT_UNI_INITIALIZER = new ChannelInitializer<QuicStreamChannel>() {
    @Override
    protected void initChannel(QuicStreamChannel ch) {
      // Unidirectional write-only stream, no read pipeline handlers needed
      ch.pipeline().addLast(new WebTransportChunkedWriteHandler());//write pipeline
    }
  };

  public static final ChannelHandler DEFAULT_BI_INITIALIZER = new ChannelInitializer<QuicStreamChannel>() {
    @Override
    protected void initChannel(QuicStreamChannel ch) {
      ch.pipeline().addLast(new WebTransportChunkedWriteHandler());
      ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
      ch.pipeline().addLast(new WebTransportCapsuleHandler());
      ch.pipeline().addLast(new MessageDispatcher());
    }
  };

  public Future<WebTransportStream> createUniStream() {
    return createUniStream(DEFAULT_UNI_INITIALIZER);
  }

  public Future<WebTransportStream> createUniStream(
          ChannelHandler streamHandler) {

    return wrapStreamFuture(
            WebTransportUtils.createUniStream(
                    connectStream,
                    false,
                    streamHandler));
  }

  public Future<WebTransportStream> createBiStream() {
    return createBiStream(DEFAULT_BI_INITIALIZER);
  }

  public Future<WebTransportStream> createBiStream(
          ChannelHandler streamHandler) {

    return wrapStreamFuture(
            WebTransportUtils.createBiStream(
                    connectStream,
                    false,
                    streamHandler));
  }

  private Future<WebTransportStream> wrapStreamFuture(
          Future<QuicStreamChannel> streamFuture) {

    Promise<WebTransportStream> promise =
            connectStream.parent().eventLoop().newPromise();

    streamFuture.addListener((Future<QuicStreamChannel> f) -> {
      if (f.isSuccess()) {
        QuicStreamChannel ch = f.getNow();

        WebTransportStream stream =
                new DefaultNettyWebTransportStream(ch, sessionStreamId);

        ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY)
                .set(stream);

        promise.setSuccess(stream);
      } else {
        promise.setFailure(f.cause());
      }
    });

    return promise;
  }


}
