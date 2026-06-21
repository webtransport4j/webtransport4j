package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.concurrent.Promise;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:21 am
 */
public class WebTransportSession {

  // Default initial capacity for stream tracking sets.
  // Most sessions have few concurrent streams; avoids 16-bucket default of ConcurrentHashMap.
  private static final int STREAM_SET_INITIAL_CAPACITY = 4;

  // Sentinel empty queue for sessions without flow control
  @SuppressWarnings("rawtypes")
  private static final Queue EMPTY_QUEUE =
      new java.util.AbstractQueue() {
        @Override
        public boolean offer(Object e) {
          return false;
        }

        @Override
        public Object poll() {
          return null;
        }

        @Override
        public Object peek() {
          return null;
        }

        @Override
        public java.util.Iterator iterator() {
          return java.util.Collections.emptyIterator();
        }

        @Override
        public int size() {
          return 0;
        }
      };

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
  private final Queue<PendingWrite> pendingWrites;
  private final AtomicLong lastSentDataBlockedLimit;

  private final boolean flowControlEnabled;

  // Initial allowed concurrent limits set at the start of the session
  private final long initialMaxStreamsUni;
  private final long initialMaxStreamsBidi;
  private final long initialMaxData;

  @SuppressWarnings("unchecked")
  WebTransportSession(
      long sessionStreamId,
      QuicStreamChannel connectStream,
      String path,
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
      this.pendingWrites = new ConcurrentLinkedQueue<>();
      this.lastSentDataBlockedLimit = new AtomicLong(-1L);
    } else {
      this.cumulativeBytesSent = null;
      this.cumulativeBytesReceived = null;
      this.pendingWrites = (Queue<PendingWrite>) EMPTY_QUEUE;
      this.lastSentDataBlockedLimit = null;
    }
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedUni() {
    return activeClientInitiatedUni;
  }

  public Set<WebTransportStream> getClientInitiatedUniStreams() {
    Set<WebTransportStream> streams = new java.util.HashSet<>();
    for (QuicStreamChannel ch : activeClientInitiatedUni) {
      WebTransportStream s = ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (s != null) {
        streams.add(s);
      }
    }
    return streams;
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedUni() {
    return activeServerInitiatedUni;
  }

  public Set<WebTransportStream> getServerInitiatedUniStreams() {
    Set<WebTransportStream> streams = new java.util.HashSet<>();
    for (QuicStreamChannel ch : activeServerInitiatedUni) {
      WebTransportStream s = ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (s != null) {
        streams.add(s);
      }
    }
    return streams;
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedBi() {
    return activeClientInitiatedBi;
  }

  public Set<WebTransportStream> getClientInitiatedBiStreams() {
    Set<WebTransportStream> streams = new java.util.HashSet<>();
    for (QuicStreamChannel ch : activeClientInitiatedBi) {
      WebTransportStream s = ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (s != null) {
        streams.add(s);
      }
    }
    return streams;
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedBi() {
    return activeServerInitiatedBi;
  }

  public Set<WebTransportStream> getServerInitiatedBiStreams() {
    Set<WebTransportStream> streams = new java.util.HashSet<>();
    for (QuicStreamChannel ch : activeServerInitiatedBi) {
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

  // --- Flow control pending write support ---

  /** Returns the queue of pending writes that are blocked by flow control. */
  public Queue<PendingWrite> getPendingWrites() {
    return pendingWrites;
  }

  /**
   * Returns the AtomicLong tracking the last peer limit for which a WT_DATA_BLOCKED capsule was
   * sent. Callers use CAS operations on this.
   */
  public AtomicLong getLastSentDataBlockedLimit() {
    return lastSentDataBlockedLimit;
  }

  /**
   * Flushes pending writes that now fit within the updated peer limit. Called when a WT_MAX_DATA
   * capsule is received and the limit increases.
   */
  public void flushPendingWrites() {
    long peerLimit = getPeerSettingsMaxData();
    long currentSent = getCumulativeBytesSent();
    while (true) {
      PendingWrite pw = pendingWrites.peek();
      if (pw == null) {
        break;
      }
      int bytesToWrite = pw.getData().readableBytes();
      if (currentSent + bytesToWrite > peerLimit) {
        break; // Still blocked
      }
      // Remove and flush — bytes will be counted when the write
      // re-enters the pipeline through RawWebTransportHandler.write()
      pw = pendingWrites.poll();
      if (pw != null) {
        currentSent += pw.getData().readableBytes();
        pw.getStreamChannel().writeAndFlush(pw.getData(), pw.getPromise());
      }
    }
  }

  /** Releases all pending writes and fails their promises. Called on session close. */
  public void cleanupPendingWrites(Throwable cause) {
    PendingWrite pw;
    while ((pw = pendingWrites.poll()) != null) {
      try {
        pw.getData().release();
      } catch (Exception ignored) {
      }
      try {
        pw.getPromise().tryFailure(cause);
      } catch (Exception ignored) {
      }
    }
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
    cleanupPendingWrites(
        new IllegalStateException(
            "Session aborted with error: 0x" + Long.toHexString(httpErrorCode)));

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
   * @return A future that is notified when the write completes.
   */
  public io.netty.util.concurrent.Future<Void> sendDatagram(ByteBuf data) {
    io.netty.channel.Channel parentChannel = connectStream.parent();
    ByteBuf header = parentChannel.alloc().directBuffer();
    WebTransportUtils.writeVarInt(header, sessionStreamId);
    
    io.netty.buffer.CompositeByteBuf composite = parentChannel.alloc().compositeBuffer(2);
    composite.addComponent(true, header);
    composite.addComponent(true, data);
    
    return parentChannel.writeAndFlush(composite);
  }

  /**
   * Sends a datagram package over the WebTransport session.
   *
   * @param data The datagram payload byte array.
   * @return A future that is notified when the write completes.
   */
  public io.netty.util.concurrent.Future<Void> sendDatagram(byte[] data) {
    io.netty.channel.Channel parentChannel = connectStream.parent();
    ByteBuf buffer = parentChannel.alloc().directBuffer();
    WebTransportUtils.writeVarInt(buffer, sessionStreamId);
    buffer.writeBytes(data);
    return parentChannel.writeAndFlush(buffer);
  }


  /**
   * Represents a write that was buffered because it would exceed the session-level flow control
   * limit.
   */
  public static class PendingWrite {
    private final QuicStreamChannel streamChannel;
    private final ByteBuf data;
    private final ChannelPromise promise;

    public PendingWrite(QuicStreamChannel streamChannel, ByteBuf data, ChannelPromise promise) {
      this.streamChannel = streamChannel;
      this.data = data;
      this.promise = promise;
    }

    public QuicStreamChannel getStreamChannel() {
      return streamChannel;
    }

    public ByteBuf getData() {
      return data;
    }

    public ChannelPromise getPromise() {
      return promise;
    }
  }

  public String path() {
    return path;
  }

  public static final ChannelHandler DEFAULT_UNI_INITIALIZER = new ChannelInitializer<QuicStreamChannel>() {
    @Override
    protected void initChannel(QuicStreamChannel ch) {
      // Unidirectional write-only stream, no read pipeline handlers needed
    }
  };

  public static final ChannelHandler DEFAULT_BI_INITIALIZER = new ChannelInitializer<QuicStreamChannel>() {
    @Override
    protected void initChannel(QuicStreamChannel ch) {
      ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
      ch.pipeline().addLast(new WebTransportCapsuleHandler());
      ch.pipeline().addLast(new MessageDispatcher());
    }
  };

  public io.netty.util.concurrent.Future<WebTransportStream> createUniStream() {
    return createUniStream(DEFAULT_UNI_INITIALIZER);
  }

  public io.netty.util.concurrent.Future<WebTransportStream> createUniStream(ChannelHandler streamHandler) {
    Promise<WebTransportStream> promise = this.connectStream.parent().eventLoop().newPromise();
    WebTransportUtils.createUniStream(this.connectStream, java.util.Optional.empty(), streamHandler)
        .addListener((io.netty.util.concurrent.Future<QuicStreamChannel> f) -> {
          if (f.isSuccess()) {
            QuicStreamChannel ch = f.getNow();
            WebTransportStream stream = new WebTransportStream(ch, this.sessionStreamId);
            ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
            promise.setSuccess(stream);
          } else {
            promise.setFailure(f.cause());
          }
        });
    return promise;
  }

  public io.netty.util.concurrent.Future<WebTransportStream> createUniStream(boolean bypassLimit, ChannelHandler streamHandler) {
    Promise<WebTransportStream> promise = this.connectStream.parent().eventLoop().newPromise();
    WebTransportUtils.createUniStream(this.connectStream, java.util.Optional.of(bypassLimit), streamHandler)
        .addListener((io.netty.util.concurrent.Future<QuicStreamChannel> f) -> {
          if (f.isSuccess()) {
            QuicStreamChannel ch = f.getNow();
            WebTransportStream stream = new WebTransportStream(ch, this.sessionStreamId);
            ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
            promise.setSuccess(stream);
          } else {
            promise.setFailure(f.cause());
          }
        });
    return promise;
  }

  public io.netty.util.concurrent.Future<WebTransportStream> createBiStream() {
    return createBiStream(DEFAULT_BI_INITIALIZER);
  }

  public io.netty.util.concurrent.Future<WebTransportStream> createBiStream(ChannelHandler streamHandler) {
    Promise<WebTransportStream> promise = this.connectStream.parent().eventLoop().newPromise();
    WebTransportUtils.createBiStream(this.connectStream, java.util.Optional.empty(), streamHandler)
        .addListener((io.netty.util.concurrent.Future<QuicStreamChannel> f) -> {
          if (f.isSuccess()) {
            QuicStreamChannel ch = f.getNow();
            WebTransportStream stream = new WebTransportStream(ch, this.sessionStreamId);
            ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
            promise.setSuccess(stream);
          } else {
            promise.setFailure(f.cause());
          }
        });
    return promise;
  }

  public io.netty.util.concurrent.Future<WebTransportStream> createBiStream(boolean bypassLimit, ChannelHandler streamHandler) {
    Promise<WebTransportStream> promise = this.connectStream.parent().eventLoop().newPromise();
    WebTransportUtils.createBiStream(this.connectStream, java.util.Optional.of(bypassLimit), streamHandler)
        .addListener((io.netty.util.concurrent.Future<QuicStreamChannel> f) -> {
          if (f.isSuccess()) {
            QuicStreamChannel ch = f.getNow();
            WebTransportStream stream = new WebTransportStream(ch, this.sessionStreamId);
            ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
            promise.setSuccess(stream);
          } else {
            promise.setFailure(f.cause());
          }
        });
    return promise;
  }
}
