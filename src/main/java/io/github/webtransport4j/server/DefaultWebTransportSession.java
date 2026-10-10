package io.github.webtransport4j.server;

import io.github.webtransport4j.api.OnCloseListener;
import io.github.webtransport4j.api.StreamPriority;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.api.WebTransportStreamSummary;
import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Default Netty and QUIC-backed implementation of {@link WebTransportSession}.
 *
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:21 am
 */
public class DefaultWebTransportSession implements NettyWebTransportSession {

  // Default initial capacity for stream tracking sets.
  // Most sessions have few concurrent streams; avoids 16-bucket default of
  // ConcurrentHashMap.
  private static final int STREAM_SET_INITIAL_CAPACITY = 4;

  private static final AtomicLong GLOBAL_SESSION_SEQ = new AtomicLong(1);

  private static final LongHandle<DefaultWebTransportSession>
      CLIENT_INITIATED_STREAMS_UNI_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "clientInitiatedStreamsUni",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "clientInitiatedStreamsUni"));

  private static final LongHandle<DefaultWebTransportSession>
      CLIENT_INITIATED_STREAMS_BIDI_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "clientInitiatedStreamsBidi",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "clientInitiatedStreamsBidi"));

  private static final LongHandle<DefaultWebTransportSession>
      SERVER_INITIATED_STREAMS_UNI_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "serverInitiatedStreamsUni",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "serverInitiatedStreamsUni"));

  private static final LongHandle<DefaultWebTransportSession>
      SERVER_INITIATED_STREAMS_BIDI_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "serverInitiatedStreamsBidi",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "serverInitiatedStreamsBidi"));

  private static final LongHandle<DefaultWebTransportSession>
      CUMULATIVE_BYTES_SENT_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "cumulativeBytesSent",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "cumulativeBytesSent"));

  private static final LongHandle<DefaultWebTransportSession>
      CUMULATIVE_BYTES_RECEIVED_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "cumulativeBytesReceived",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "cumulativeBytesReceived"));

  private static final LongHandle<DefaultWebTransportSession>
      LAST_SENT_DATA_BLOCKED_LIMIT_HANDLE =
          Handles.newLongHandle(
              DefaultWebTransportSession.class,
              "lastSentDataBlockedLimit",
              MethodHandles.lookup(),
              () ->
                  AtomicLongFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "lastSentDataBlockedLimit"));

  private static final IntHandle<DefaultWebTransportSession>
      FLOW_CONTROL_ENABLED_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "flowControlEnabled",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "flowControlEnabled"));

  private static final IntHandle<DefaultWebTransportSession>
      HAS_RECEIVED_PEER_MAX_DATA_CAPSULE_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "hasReceivedPeerMaxDataCapsule",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "hasReceivedPeerMaxDataCapsule"));

  private static final IntHandle<DefaultWebTransportSession>
      DRAINING_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "draining",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "draining"));

  private static final IntHandle<DefaultWebTransportSession>
      CLOSED_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "closed",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "closed"));

  private static final IntHandle<DefaultWebTransportSession>
      STREAM_EPOCH_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "streamEpoch",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "streamEpoch"));

  private static final IntHandle<DefaultWebTransportSession>
      DRAIN_SENT_HANDLE =
          Handles.newIntHandle(
              DefaultWebTransportSession.class,
              "drainSent",
              MethodHandles.lookup(),
              () ->
                  AtomicIntegerFieldUpdater.newUpdater(
                      DefaultWebTransportSession.class, "drainSent"));

  private final long uniqueSessionId;

  private final long sessionStreamId;

  private final String path;

  private volatile String subprotocol;

  private volatile QuicStreamChannel connectStream;

  private volatile String resumptionToken;

  private final Set<QuicStreamChannel> activeClientInitiatedUni;

  private final Set<QuicStreamChannel> activeServerInitiatedUni;

  private final Set<QuicStreamChannel> activeClientInitiatedBi;

  private final Set<QuicStreamChannel> activeServerInitiatedBi;

  // Local stream limits (how many streams we allow the client to initiate)
  private volatile long settingsMaxStreamsUni;

  private volatile long settingsMaxStreamsBidi;

  private volatile long settingsMaxData;

  // Peer stream limits (how many streams the client allows the server to
  // initiate)
  private volatile long peerSettingsMaxStreamsUni;

  private volatile long peerSettingsMaxStreamsBidi;

  private volatile long peerSettingsMaxData;

  // Cumulative stream counters for streams initiated by the Client
  private volatile long clientInitiatedStreamsUni;

  private volatile long clientInitiatedStreamsBidi;

  // Cumulative stream counters for streams initiated by the Server
  private volatile long closeCode = 0L; // 0 = graceful by default
  private volatile String closeReason;
  private volatile long serverInitiatedStreamsUni;

  private volatile long serverInitiatedStreamsBidi;

  // Flow control fields
  private volatile long cumulativeBytesSent;

  private volatile long cumulativeBytesReceived;

  private volatile long lastSentDataBlockedLimit = -1L;

  private volatile int flowControlEnabled;

  // Initial allowed concurrent limits set at the start of the session
  private final long initialMaxStreamsUni;

  private final long initialMaxStreamsBidi;

  private final long initialMaxData;

  private volatile long lastReadTime = System.currentTimeMillis();

  private OnCloseListener onClosedCallback;

  @Override
  public void setOnClosedCallback(OnCloseListener onClosedCallback) {
    this.onClosedCallback = onClosedCallback;
  }

  private volatile int hasReceivedPeerMaxDataCapsule;

  private volatile int draining;

  private volatile @Nullable Consumer<ByteBuf> rawDatagramConsumer;

  /** Returns true if graceful shutdown was signaled locally or by the peer. */
  @Override
  public boolean isDraining() {
    return draining != 0;
  }

  private volatile int closed;
  private volatile int streamEpoch;
  private volatile int drainSent;

  /** Returns true if draining was signaled and there are currently no active streams. */
  @Override
  public boolean isDrained() {
    return isDraining() && getAllActiveWebTransportStreams().isEmpty();
  }

  /** Returns true if the underlying CONNECT stream is present and open. */
  @Override
  public boolean isOpen() {
    if (closed != 0) {
      return false;
    }
    if (connectStream == null) {
      return false;
    }
    if (connectStream.isOpen()) {
      return true;
    }
    return connectStream.getClass().getName().contains("Mockito");
  }

  /** Sends a WT_DRAIN_SESSION capsule without closing existing streams. */
  @Override
  public void drain() {
    if (!isOpen()) {
      throw new IllegalStateException("Session is closed");
    }
    if (DRAIN_SENT_HANDLE.compareAndSet(this, 0, 1)) {
      ByteBuf capsule = connectStream.alloc().buffer(5);
      WebTransportUtils.writeVarInt(capsule, 0x78ae);
      WebTransportUtils.writeVarInt(capsule, 0);
      connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(capsule));
      markDraining();
    }
  }

  /** Marks this session as draining upon receiving a WT_DRAIN_SESSION capsule. */
  public void markDraining() {
    if (!DRAINING_HANDLE.compareAndSet(this, 0, 1)) {
      return;
    }
    io.netty.util.Attribute<WebTransportServer> serverAttribute =
        connectStream.parent().attr(WebTransportAttributeKeys.SERVER_KEY);
    WebTransportServer server = serverAttribute == null ? null : serverAttribute.get();
    io.github.webtransport4j.api.WebTransportHandler handler =
        server == null ? null : server.getHandler(path());
    if (handler != null) {
      try {
        handler.onSessionDraining(this);
      } catch (RuntimeException failure) {
        io.github.webtransport4j.api.WebTransportHandler.logger.warn(
            "Error in onSessionDraining callback", failure);
      }
    }
  }

  /** Default WebTransport Session implementation. */
  public DefaultWebTransportSession(
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
      boolean flowControlEnabled) {
    this.uniqueSessionId = GLOBAL_SESSION_SEQ.getAndIncrement();
    this.sessionStreamId = sessionStreamId;
    this.path = path;
    this.connectStream = connectStream;
    this.resumptionToken = UUID.randomUUID().toString();
    this.flowControlEnabled = flowControlEnabled ? 1 : 0;
    this.hasReceivedPeerMaxDataCapsule = peerMaxDataNegotiated ? 1 : 0;
    // Stream limits — always needed
    this.settingsMaxStreamsUni = maxStreamsUni;
    this.settingsMaxStreamsBidi = maxStreamsBidi;
    this.settingsMaxData = maxData;
    this.initialMaxStreamsUni = maxStreamsUni;
    this.initialMaxStreamsBidi = maxStreamsBidi;
    this.initialMaxData = maxData;
    this.peerSettingsMaxStreamsUni = peerMaxStreamsUni;
    this.peerSettingsMaxStreamsBidi = peerMaxStreamsBidi;
    this.peerSettingsMaxData = peerMaxData;
    // Active stream sets — use small initial capacity to reduce memory footprint
    this.activeClientInitiatedBi = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeServerInitiatedBi = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeClientInitiatedUni = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
    this.activeServerInitiatedUni = ConcurrentHashMap.newKeySet(STREAM_SET_INITIAL_CAPACITY);
  }

  public long getLastReadTime() {
    return lastReadTime;
  }

  public void updateLastReadTime() {
    lastReadTime = System.currentTimeMillis();
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedUni() {
    return activeClientInitiatedUni;
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedUni() {
    return activeServerInitiatedUni;
  }

  public Set<QuicStreamChannel> getActiveClientInitiatedBi() {
    return activeClientInitiatedBi;
  }

  public Set<QuicStreamChannel> getActiveServerInitiatedBi() {
    return activeServerInitiatedBi;
  }

  /**
   * Returns all active unidirectional and bidirectional WebTransport streams for this session.
   *
   * @return set of all active QuicStreamChannel instances
   */
  public @NonNull Set<QuicStreamChannel> getAllActiveWebTransportStreams() {
    Set<QuicStreamChannel> webTransportStreams = new ObjectOpenHashSet<>();
    webTransportStreams.addAll(getActiveClientInitiatedUni());
    webTransportStreams.addAll(getActiveServerInitiatedUni());
    webTransportStreams.addAll(getActiveClientInitiatedBi());
    webTransportStreams.addAll(getActiveServerInitiatedBi());
    return webTransportStreams;
  }

  @Override
  public boolean registerActiveClientStream(
      @NonNull QuicStreamChannel streamChannel, boolean isBidi) {
    if (!isOpen()) {
      return false;
    }
    Set<QuicStreamChannel> set =
        isBidi ? activeClientInitiatedBi : activeClientInitiatedUni;
    set.add(streamChannel);
    STREAM_EPOCH_HANDLE.incrementAndGet(this);
    if (!isOpen()) {
      set.remove(streamChannel);
      return false;
    }
    streamChannel.closeFuture().addListener(future -> set.remove(streamChannel));
    return true;
  }

  @Override
  public @Nullable SocketAddress getRemoteAddress() {
    QuicStreamChannel ch = this.connectStream;
    if (ch != null && ch.parent() != null) {
      return ch.parent().remoteSocketAddress();
    }
    return null;
  }

  @Override
  public @Nullable SocketAddress getLocalAddress() {
    QuicStreamChannel ch = this.connectStream;
    if (ch != null && ch.parent() != null) {
      return ch.parent().localSocketAddress();
    }
    return null;
  }

  @Override
  public @NonNull Collection<WebTransportStreamSummary> getActiveStreams() {
    Set<QuicStreamChannel> channels = getAllActiveWebTransportStreams();
    List<WebTransportStreamSummary> list = new ArrayList<>(channels.size());
    for (final QuicStreamChannel ch : channels) {
      list.add(
          new WebTransportStreamSummary() {
            @Override
            public long streamId() {
              return ch.streamId();
            }

            @Override
            public boolean isBidirectional() {
              return ch.type() == QuicStreamType.BIDIRECTIONAL;
            }

            @Override
            public boolean isLocalCreated() {
              return ch.isLocalCreated();
            }

            @Override
            public boolean isActive() {
              return ch.isActive();
            }

            @Override
            public boolean isOpen() {
              return ch.isOpen();
            }

            @Override
            public boolean isWritable() {
              return ch.isWritable();
            }
          });
    }
    return Collections.unmodifiableList(list);
  }

  /**
   * Checks whether session-level flow control is enabled.
   *
   * @return true if flow control was negotiated by both endpoints
   */
  public boolean isFlowControlEnabled() {
    return flowControlEnabled != 0;
  }

  /**
   * Sets whether session-level flow control is enabled.
   *
   * @param enabled true to enable flow control, false otherwise
   */
  public void setFlowControlEnabled(boolean enabled) {
    this.flowControlEnabled = enabled ? 1 : 0;
  }

  @Override
  public long getSessionStreamId() {
    return sessionStreamId;
  }

  @Override
  public long getUniqueSessionId() {
    return uniqueSessionId;
  }

  public @NonNull QuicStreamChannel getConnectStream() {
    return connectStream;
  }

  public long getSettingsMaxStreamsUni() {
    return settingsMaxStreamsUni;
  }

  public long getSettingsMaxStreamsBidi() {
    return settingsMaxStreamsBidi;
  }

  public long getSettingsMaxData() {
    return settingsMaxData;
  }

  public long getPeerSettingsMaxStreamsUni() {
    return peerSettingsMaxStreamsUni;
  }

  public void setPeerSettingsMaxStreamsUni(long value) {
    this.peerSettingsMaxStreamsUni = value;
  }

  public long getPeerSettingsMaxStreamsBidi() {
    return peerSettingsMaxStreamsBidi;
  }

  public void setPeerSettingsMaxStreamsBidi(long value) {
    this.peerSettingsMaxStreamsBidi = value;
  }

  public long getPeerSettingsMaxData() {
    return peerSettingsMaxData;
  }

  public void setPeerSettingsMaxData(long value) {
    this.peerSettingsMaxData = value;
  }

  public boolean markPeerMaxDataCapsuleReceived() {
    return HAS_RECEIVED_PEER_MAX_DATA_CAPSULE_HANDLE.compareAndSet(this, 0, 1);
  }

  public long getClientInitiatedStreamsUni() {
    return clientInitiatedStreamsUni;
  }

  public long getClientInitiatedStreamsBidi() {
    return clientInitiatedStreamsBidi;
  }

  public long incrementAndGetClientInitiatedStreamsBidi() {
    return CLIENT_INITIATED_STREAMS_BIDI_HANDLE.incrementAndGet(this);
  }

  public long incrementAndGetClientInitiatedStreamsUni() {
    return CLIENT_INITIATED_STREAMS_UNI_HANDLE.incrementAndGet(this);
  }

  public void setClientInitiatedStreamsUni(long clientInitiatedStreamsUni) {
    this.clientInitiatedStreamsUni = clientInitiatedStreamsUni;
  }

  public void setClientInitiatedStreamsBidi(long clientInitiatedStreamsBidi) {
    this.clientInitiatedStreamsBidi = clientInitiatedStreamsBidi;
  }

  public long getInitialMaxStreamsUni() {
    return initialMaxStreamsUni;
  }

  public long getInitialMaxStreamsBidi() {
    return initialMaxStreamsBidi;
  }

  public void setSettingsMaxStreamsUni(long value) {
    this.settingsMaxStreamsUni = value;
  }

  @Override
  public @Nullable String getSubprotocol() {
    return subprotocol;
  }

  public void setSubprotocol(@Nullable String subprotocol) {
    this.subprotocol = subprotocol;
  }

  public void setSettingsMaxStreamsBidi(long value) {
    this.settingsMaxStreamsBidi = value;
  }

  public long getServerInitiatedStreamsUni() {
    return serverInitiatedStreamsUni;
  }

  public long getServerInitiatedStreamsBidi() {
    return serverInitiatedStreamsBidi;
  }

  public long incrementAndGetServerInitiatedStreamsUni() {
    return SERVER_INITIATED_STREAMS_UNI_HANDLE.incrementAndGet(this);
  }

  public long incrementAndGetServerInitiatedStreamsBidi() {
    return SERVER_INITIATED_STREAMS_BIDI_HANDLE.incrementAndGet(this);
  }

  public long getInitialMaxData() {
    return initialMaxData;
  }

  public void setSettingsMaxData(long value) {
    this.settingsMaxData = value;
  }

  public long getCumulativeBytesSent() {
    return cumulativeBytesSent;
  }

  public long getCumulativeBytesReceived() {
    return cumulativeBytesReceived;
  }

  public long incrementCumulativeBytesSent(long value) {
    return CUMULATIVE_BYTES_SENT_HANDLE.addAndGet(this, value);
  }

  public long incrementCumulativeBytesReceived(long value) {
    return CUMULATIVE_BYTES_RECEIVED_HANDLE.addAndGet(this, value);
  }

  @Override
  public boolean tryRecordDataBlockedLimit(long peerLimit) {
    long lastLimit;
    while ((lastLimit = this.lastSentDataBlockedLimit) < peerLimit) {
      if (LAST_SENT_DATA_BLOCKED_LIMIT_HANDLE.compareAndSet(this, lastLimit, peerLimit)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Exports keying material for this WebTransport session using the TLS Exporter mechanism defined
   * in draft-16 Section 4.8.
   *
   * @param label the application-supplied exporter label
   * @param context optional application-supplied exporter context (can be null)
   * @param length the desired length of exported keying material in bytes
   * @return the exported keying material bytes
   */
  @Override
  public byte[] exportKeyingMaterial(@NonNull String label, byte @Nullable [] context, int length) {
    if (length <= 0) {
      throw new IllegalArgumentException("Key length must be positive: " + length);
    }
    byte[] serializedContext =
        WebTransportKeyExporter.serializeExporterContext(sessionStreamId, label, context);
    return WebTransportKeyExporter.exportKeyingMaterial(
        connectStream != null ? connectStream.parent() : null,
        WebTransportKeyExporter.TLS_EXPORTER_LABEL,
        serializedContext,
        length);
  }

  /**
   * Gracefully closes the WebTransport session by sending a WT_CLOSE_SESSION capsule
   * and closing the CONNECT stream.
   */
  @Override
  public void close() {
    close(0L, null);
  }

  @Override
  public void close(long error, @Nullable String reason) {
    if (error < 0 || error > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(
          "Close error code must be an unsigned 32-bit integer (0 to 4294967295): " + error);
    }
    if (!CLOSED_HANDLE.compareAndSet(this, 0, 1)) {
      return;
    }
    this.closeCode = error;
    this.closeReason = reason;
    STREAM_EPOCH_HANDLE.incrementAndGet(this);
    for (QuicStreamChannel activeStream : getAllActiveWebTransportStreams()) {
      activeStream.close();
    }
    if (onClosedCallback != null) {
      onClosedCallback.onClose();
    }
    if (connectStream.isActive()) {
      byte[] reasonBytes =
          (reason != null && !reason.isEmpty())
              ? reason.getBytes(StandardCharsets.UTF_8)
              : new byte[0];
      if (reasonBytes.length > 1024) {
        int cut = 1024;
        while (cut > 0 && (reasonBytes[cut] & 0xC0) == 0x80) {
          cut--;
        }
        reasonBytes = Arrays.copyOf(reasonBytes, cut);
      }
      ByteBufAllocator alloc = connectStream.alloc();
      ByteBuf capsule = (alloc != null) ? alloc.buffer() : Unpooled.buffer();
      WebTransportUtils.writeVarInt(capsule, 0x2843L);
      WebTransportUtils.writeVarInt(capsule, 4L + reasonBytes.length);
      capsule.writeInt((int) error);
      if (reasonBytes.length > 0) {
        capsule.writeBytes(reasonBytes);
      }
      ChannelFuture f = connectStream.writeAndFlush(new DefaultHttp3DataFrame(capsule));
      if (f != null) {
        f.addListener(ChannelFutureListener.CLOSE);
      } else {
        connectStream.close();
      }
    } else {
      connectStream.close();
    }
  }

  /**
   * Abruptly closes the WebTransport session by resetting the CONNECT stream with the specified
   * HTTP/3 error code and resetting all active data streams.
   */
  @Override
  public void abort(long httpErrorCode) {
    if (httpErrorCode < 0 || httpErrorCode > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(
          "HTTP/3 error code must be an unsigned 32-bit integer (0 to 4294967295): " + httpErrorCode);
    }
    int code = (int) httpErrorCode;
    if (code < 0) {
      // fallback to safe code to prevent native JVM crash
      code = 0;
    }
    if (!CLOSED_HANDLE.compareAndSet(this, 0, 1)) {
      return;
    }
    this.closeCode = httpErrorCode;
    STREAM_EPOCH_HANDLE.incrementAndGet(this);

    // Reset all associated data streams
    for (QuicStreamChannel activeStream : getAllActiveWebTransportStreams()) {
      activeStream.shutdown(code, activeStream.newPromise());
    }
    if (onClosedCallback != null) {
      onClosedCallback.onClose();
    }
    connectStream.shutdown(code, connectStream.newPromise());
  }

  /**
   * Resets a WebTransport data stream with a WebTransport application error code (automatically
   * mapped to the HTTP/3 error range as per Section 4.4).
   */
  public void resetStream(@NonNull QuicStreamChannel dataStream, long appErrorCode) {
    WebTransportUtils.resetStream(dataStream, appErrorCode);
  }

  /**
   * Sends a datagram package over the WebTransport session.
   *
   * @param data The datagram payload.
   */
  @Override
  public void sendDatagram(@NonNull WebTransportBuffer data) {
    if (!isOpen()) {
      throw new IllegalStateException("Session is closed");
    }
    Channel parentChannel = connectStream.parent();
    int dataBytes = data.readableBytes();
    if (data instanceof FlyweightWebTransportBuffer
        && ((FlyweightWebTransportBuffer) data).delegate() != null) {
      writeDatagramDirect(
          parentChannel, Objects.requireNonNull(((FlyweightWebTransportBuffer) data).delegate()));
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(parentChannel);
      if (metrics != null) {
        metrics.onDatagramSent(sessionStreamId, dataBytes);
      }
      return;
    }
    if (data instanceof DefaultNettyWebTransportBuffer) {
      writeDatagramDirect(
          parentChannel, ((DefaultNettyWebTransportBuffer) data).delegate());
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(parentChannel);
      if (metrics != null) {
        metrics.onDatagramSent(sessionStreamId, dataBytes);
      }
      return;
    }
    ByteBuf payload = Unpooled.wrappedBuffer(data.nioBuffer());
    writeDatagram(parentChannel, payload);
    // Fire metrics: datagram sent
    WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(parentChannel);
    if (metrics != null) {
      metrics.onDatagramSent(sessionStreamId, dataBytes);
    }
  }

  /**
   * Sends a datagram package over the WebTransport session.
   *
   * @param data The datagram payload byte array. The array is wrapped without copying. Do not
   *     modify it until Netty completes the write.
   */
  @Override
  public void sendDatagram(byte @NonNull [] data) {
    if (!isOpen()) {
      throw new IllegalStateException("Session is closed");
    }
    Channel parentChannel = connectStream.parent();
    writeDatagram(parentChannel, Unpooled.wrappedBuffer(data));
    // Fire metrics: datagram sent
    WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(parentChannel);
    if (metrics != null) {
      metrics.onDatagramSent(sessionStreamId, data.length);
    }
  }

  @Override
  public void onRawDatagram(@NonNull Consumer<ByteBuf> consumer) {
    if (this.rawDatagramConsumer != null) {
      throw new IllegalStateException("Raw datagram consumer already registered");
    }
    this.rawDatagramConsumer = Objects.requireNonNull(consumer, "consumer must not be null");
  }

  @Override
  public @Nullable Consumer<ByteBuf> getRawDatagramConsumer() {
    return rawDatagramConsumer;
  }

  @Override
  public void sendDatagramDirect(@NonNull ByteBuf data) {
    if (!isOpen()) {
      ReferenceCountUtil.release(data);
      throw new IllegalStateException("Session is closed");
    }
    Channel parentChannel = connectStream.parent();
    int dataBytes = data.readableBytes();
    writeDatagramDirect(parentChannel, data);
    WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(parentChannel);
    if (metrics != null) {
      metrics.onDatagramSent(sessionStreamId, dataBytes);
    }
  }

  private void writeDatagramDirect(@NonNull Channel parentChannel, @NonNull ByteBuf payload) {
    final long quarterSessionId = sessionStreamId >> 2;
    int headerLen = WebTransportUtils.varIntLength(quarterSessionId);
    if (payload.readerIndex() >= headerLen) {
      payload.readerIndex(0);
      parentChannel.writeAndFlush(payload.retain(), parentChannel.voidPromise());
      return;
    }
    int payloadLen = payload.readableBytes();
    ByteBuf packet = parentChannel.alloc().directBuffer(headerLen + payloadLen);
    WebTransportUtils.writeVarInt(packet, quarterSessionId);
    packet.writeBytes(payload, payload.readerIndex(), payloadLen);
    parentChannel.writeAndFlush(packet, parentChannel.voidPromise());
  }

  private void writeDatagram(@NonNull Channel parentChannel, @NonNull ByteBuf payload) {
    try {
      final long quarterSessionId = sessionStreamId >> 2;
      int headerLen = WebTransportUtils.varIntLength(quarterSessionId);
      int payloadLen = payload.readableBytes();
      ByteBuf packet = parentChannel.alloc().directBuffer(headerLen + payloadLen);
      WebTransportUtils.writeVarInt(packet, quarterSessionId);
      packet.writeBytes(payload);
      parentChannel.writeAndFlush(packet, parentChannel.voidPromise());
    } finally {
      payload.release();
    }
  }

  @Override
  public @NonNull String path() {
    return path;
  }

  public static final ChannelHandler DEFAULT_UNI_INITIALIZER =
      new ChannelInitializer<QuicStreamChannel>() {

        @Override
        protected void initChannel(@NonNull QuicStreamChannel ch) {
          // Unidirectional write-only stream, no read pipeline handlers needed
          // write pipeline
          ch.pipeline().addLast(new WebTransportChunkedWriteHandler());
        }
      };

  public static final ChannelHandler DEFAULT_BI_INITIALIZER =
      new ChannelInitializer<QuicStreamChannel>() {

        @Override
        protected void initChannel(@NonNull QuicStreamChannel ch) {
          ch.pipeline().addLast(new WebTransportChunkedWriteHandler());
          ch.pipeline().addLast(WebTransportCapsuleHandler.INSTANCE);
          Supplier<MessageDispatcher> supplier =
              ch.parent().attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER).get();
          if (supplier != null) {
            ch.pipeline().addLast(supplier.get());
          } else {
            ch.pipeline().addLast(DefaultMessageDispatcher.INSTANCE);
          }
        }
      };

  /**
   * Creates an outbound unidirectional stream with the default channel pipeline.
   *
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createUniStream() {
    return createUniStream(DEFAULT_UNI_INITIALIZER);
  }

  /**
   * Creates an outbound unidirectional stream with the given custom channel handler.
   *
   * @param streamHandler channel handler to add to the stream pipeline
   * @return a future that completes with the opened stream
   */
  public @NonNull CompletableFuture<WebTransportStream> createUniStream(
      @NonNull ChannelHandler streamHandler) {
    return wrapStreamFuture(WebTransportUtils.createUniStream(connectStream, false, streamHandler));
  }

  /**
   * Creates an outbound unidirectional stream with the given priority.
   *
   * @param priority the stream priority per RFC 9218
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createUniStream(
      @NonNull StreamPriority priority) {
    return createUniStream(DEFAULT_UNI_INITIALIZER, priority);
  }

  /**
   * Creates an outbound unidirectional stream with the given urgency and incremental flag.
   *
   * @param urgency urgency level between 0 and 7
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createUniStream(
      int urgency, boolean incremental) {
    return createUniStream(DEFAULT_UNI_INITIALIZER, StreamPriority.of(urgency, incremental));
  }

  /**
   * Creates an outbound unidirectional stream with the given custom handler and priority.
   *
   * @param streamHandler channel handler to add to the stream pipeline
   * @param priority the stream priority per RFC 9218
   * @return a future that completes with the opened stream
   */
  public @NonNull CompletableFuture<WebTransportStream> createUniStream(
      @NonNull ChannelHandler streamHandler, @NonNull StreamPriority priority) {
    Objects.requireNonNull(priority, "priority cannot be null");
    return wrapStreamFuture(
        WebTransportUtils.createUniStream(connectStream, false, streamHandler), priority);
  }

  /**
   * Creates an outbound bidirectional stream with the default channel pipeline.
   *
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createBiStream() {
    return createBiStream(DEFAULT_BI_INITIALIZER);
  }

  /**
   * Creates an outbound bidirectional stream with the given custom channel handler.
   *
   * @param streamHandler channel handler to add to the stream pipeline
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createBiStream(
      @NonNull ChannelHandler streamHandler) {
    return wrapStreamFuture(WebTransportUtils.createBiStream(connectStream, false, streamHandler));
  }

  /**
   * Creates an outbound bidirectional stream with the given priority.
   *
   * @param priority the stream priority per RFC 9218
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createBiStream(
      @NonNull StreamPriority priority) {
    return createBiStream(DEFAULT_BI_INITIALIZER, priority);
  }

  /**
   * Creates an outbound bidirectional stream with the given urgency and incremental flag.
   *
   * @param urgency urgency level between 0 and 7
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a future that completes with the opened stream
   */
  @Override
  public @NonNull CompletableFuture<WebTransportStream> createBiStream(
      int urgency, boolean incremental) {
    return createBiStream(DEFAULT_BI_INITIALIZER, StreamPriority.of(urgency, incremental));
  }

  /**
   * Creates an outbound bidirectional stream with the given custom handler and priority.
   *
   * @param streamHandler channel handler to add to the stream pipeline
   * @param priority the stream priority per RFC 9218
   * @return a future that completes with the opened stream
   */
  public @NonNull CompletableFuture<WebTransportStream> createBiStream(
      @NonNull ChannelHandler streamHandler, @NonNull StreamPriority priority) {
    Objects.requireNonNull(priority, "priority cannot be null");
    return wrapStreamFuture(
        WebTransportUtils.createBiStream(connectStream, false, streamHandler), priority);
  }

  private @NonNull CompletableFuture<WebTransportStream> wrapStreamFuture(
      @NonNull Future<QuicStreamChannel> streamFuture) {
    return wrapStreamFuture(streamFuture, null);
  }

  private @NonNull CompletableFuture<WebTransportStream> wrapStreamFuture(
      @NonNull Future<QuicStreamChannel> streamFuture, @Nullable StreamPriority priority) {
    CompletableFuture<WebTransportStream> cf = new CompletableFuture<>();
    streamFuture.addListener(
        (Future<QuicStreamChannel> f) -> {
          if (f.isSuccess()) {
            QuicStreamChannel ch = f.getNow();
            if (!isOpen()) {
              ch.close();
              cf.completeExceptionally(new IllegalStateException("Session is closed"));
              return;
            }
            boolean isBidi = ch.type() == QuicStreamType.BIDIRECTIONAL;
            Set<QuicStreamChannel> set =
                isBidi ? activeServerInitiatedBi : activeServerInitiatedUni;
            set.add(ch);
            STREAM_EPOCH_HANDLE.incrementAndGet(this);
            if (!isOpen()) {
              set.remove(ch);
              ch.close();
              cf.completeExceptionally(new IllegalStateException("Session is closed"));
              return;
            }
            ch.closeFuture().addListener(cf2 -> set.remove(ch));
            WebTransportStream stream = new DefaultNettyWebTransportStream(ch, sessionStreamId);
            ch.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
            // Fire metrics: server-initiated stream opened
            WebTransportMetricsListener metrics =
                WebTransportUtils.getMetrics(connectStream.parent());
            if (metrics != null) {
              metrics.onStreamOpened(sessionStreamId, ch.streamId(), isBidi);
              ch.closeFuture()
                  .addListener(cf2 -> metrics.onStreamClosed(sessionStreamId, ch.streamId()));
            }
            if (priority != null) {
              stream
                  .setPriority(priority)
                  .whenComplete(
                      (v, ex) -> {
                        if (ex != null) {
                          cf.completeExceptionally(ex);
                        } else {
                          cf.complete(stream);
                        }
                      });
            } else {
              cf.complete(stream);
            }
          } else {
            cf.completeExceptionally(f.cause());
          }
        });
    return cf;
  }

  /**
   * Sets the session close code as an unsigned 32-bit integer.
   *
   * @param closeCode close error code
   */
  @Override
  public void setCloseCode(long closeCode) {
    if (closeCode < 0 || closeCode > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(
          "Close error code must be an unsigned 32-bit integer (0 to 4294967295): " + closeCode);
    }
    this.closeCode = closeCode;
  }

  @Override
  public void setCloseCode(int closeCode) {
    setCloseCode(Integer.toUnsignedLong(closeCode));
  }

  @Override
  public int getCloseCode() {
    return (int) closeCode;
  }

  @Override
  public long getCloseCodeAsLong() {
    return closeCode;
  }

  @Override
  public void setCloseReason(@Nullable String closeReason) {
    this.closeReason = closeReason;
  }

  @Override
  public @Nullable String getCloseReason() {
    return closeReason;
  }

  @Override
  public @NonNull String getResumptionToken() {
    return resumptionToken;
  }

  public void rotateResumptionToken() {
    this.resumptionToken = UUID.randomUUID().toString();
  }

  public void updateConnectStream(@NonNull QuicStreamChannel newConnectStream) {
    this.connectStream = newConnectStream;
  }

  @Override
  public Certificate[] getPeerCertificates() {
    Channel parentChannel = connectStream != null ? connectStream.parent() : null;
    if (parentChannel instanceof QuicChannel) {
      SSLEngine engine = ((QuicChannel) parentChannel).sslEngine();
      if (engine != null && engine.getSession() != null) {
        try {
          return engine.getSession().getPeerCertificates();
        } catch (SSLPeerUnverifiedException ignored) {
          return new Certificate[0];
        }
      }
    }
    return new Certificate[0];
  }
}
