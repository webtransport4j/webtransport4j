package io.github.webtransport4j.server;

import io.github.webtransport4j.api.BinarySource;
import io.github.webtransport4j.api.OnCloseListener;
import io.github.webtransport4j.api.StreamCodec;
import io.github.webtransport4j.api.StreamPriority;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamPriority;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default Netty-based WebTransport stream implementation. */
public class DefaultNettyWebTransportStream implements NettyWebTransportStream {

  private final @NonNull QuicStreamChannel streamChannel;

  private final long sessionId;

  private final long streamId;

  private final boolean bidirectional;

  private volatile @Nullable Consumer<WebTransportBuffer> dataConsumer;

  private volatile @Nullable OnCloseListener closeHandler;

  private volatile @Nullable Consumer<Throwable> errorHandler;

  volatile @Nullable Map<String, Object> attributes;

  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<DefaultNettyWebTransportStream, Map>
      ATTRIBUTES_UPDATER =
          AtomicReferenceFieldUpdater.newUpdater(
              DefaultNettyWebTransportStream.class, Map.class, "attributes");

  private static final CompletableFuture<Void> COMPLETED_FUTURE =
      CompletableFuture.completedFuture(null);

  private static @NonNull CompletableFuture<Void> toCompletableFuture(
      @NonNull Future<?> nettyFuture) {
    if (nettyFuture.isDone()) {
      if (nettyFuture.isSuccess()) {
        return COMPLETED_FUTURE;
      } else {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.completeExceptionally(nettyFuture.cause());
        return cf;
      }
    }
    CompletableFuture<Void> cf = new CompletableFuture<>();
    nettyFuture.addListener(
        f -> {
          if (f.isSuccess()) {
            cf.complete(null);
          } else {
            cf.completeExceptionally(f.cause());
          }
        });
    return cf;
  }

  private static final Logger logger =
      LoggerFactory.getLogger(DefaultNettyWebTransportStream.class);

  private final Queue<CompletableFuture<Void>> writableWaiters = new ConcurrentLinkedQueue<>();
  private volatile @Nullable Consumer<Boolean> writabilityListener;
  private volatile boolean lastNotifiedWritable = true;

  /** Default Netty Web Transport Stream. */
  public DefaultNettyWebTransportStream(@NonNull QuicStreamChannel channel, long sessionId) {
    this.streamChannel = Objects.requireNonNull(channel, "channel must not be null");
    this.sessionId = sessionId;
    this.streamId = channel.streamId();
    this.bidirectional = (channel.type() == QuicStreamType.BIDIRECTIONAL);
    this.lastNotifiedWritable = channel.isWritable();
    if (channel.closeFuture() != null) {
      channel.closeFuture().addListener(f -> notifyClosed());
    }
  }

  public long sessionId() {
    return sessionId;
  }

  public long streamId() {
    return streamId;
  }

  public boolean isBidirectional() {
    return bidirectional;
  }

  public @NonNull QuicStreamChannel streamChannel() {
    return streamChannel;
  }

  @Override
  public void setAutoRead(boolean autoRead) {
    if (streamChannel.eventLoop() != null) {
      if (streamChannel.eventLoop().inEventLoop()) {
        streamChannel.config().setAutoRead(autoRead);
      } else {
        streamChannel.eventLoop().execute(() -> streamChannel.config().setAutoRead(autoRead));
      }
    } else {
      streamChannel.config().setAutoRead(autoRead);
    }
  }

  @Override
  public boolean isAutoRead() {
    return streamChannel.config().isAutoRead();
  }

  @Override
  public void read() {
    if (streamChannel.eventLoop() != null) {
      if (streamChannel.eventLoop().inEventLoop()) {
        streamChannel.read();
      } else {
        streamChannel.eventLoop().execute(streamChannel::read);
      }
    } else {
      streamChannel.read();
    }
  }

  /**
   * Registers a callback to be invoked when stream payload data is received.
   *
   * @param consumer the data consumer callback
   */
  public void onData(@NonNull Consumer<WebTransportBuffer> consumer) {
    if (this.dataConsumer != null) {
      throw new IllegalStateException("onData handler already registered");
    }
    this.dataConsumer = consumer;
  }

  /** On Data. */
  public <T> void onData(@NonNull StreamCodec<T> codec, @NonNull Consumer<T> consumer) {
    Consumer<T> autoReleasingConsumer =
        msg -> {
          try {
            consumer.accept(msg);
          } finally {
            codec.release(msg);
          }
        };
    this.onData(
        data -> {
          codec.decode(data, autoReleasingConsumer);
        });
  }

  public void onClose(@NonNull OnCloseListener onCloseListener) {
    this.closeHandler = onCloseListener;
  }

  public void onError(@NonNull Consumer<Throwable> handler) {
    this.errorHandler = handler;
  }

  public @Nullable Consumer<WebTransportBuffer> getDataConsumer() {
    return dataConsumer;
  }

  public @Nullable OnCloseListener getCloseHandler() {
    return closeHandler;
  }

  public @Nullable Consumer<Throwable> getErrorHandler() {
    return errorHandler;
  }

  private @NonNull CompletableFuture<Void> writeOutbound(@NonNull Object msg) {
    if (!streamChannel.isActive()) {
      ReferenceCountUtil.release(msg);
      CompletableFuture<Void> cf = new CompletableFuture<>();
      cf.completeExceptionally(new ClosedChannelException());
      return cf;
    }
    return toCompletableFuture(streamChannel.writeAndFlush(msg));
  }

  /**
   * Notifies the stream that the channel's writability state has changed. Called by {@link
   * WebTransportChunkedWriteHandler} when Netty's {@code channelWritabilityChanged} fires.
   *
   * @param writable true if the channel has become writable
   */
  public void notifyWritabilityChanged(boolean writable) {
    if (writable) {
      drainWritableWaiters();
    }
    notifyWritabilityListener();
  }

  /** Notifies the stream that the channel has closed, failing pending waiters. */
  public void notifyClosed() {
    CompletableFuture<Void> waiter;
    while ((waiter = writableWaiters.poll()) != null) {
      waiter.completeExceptionally(new ClosedChannelException());
    }
    notifyWritabilityListener();
  }

  private void drainWritableWaiters() {
    CompletableFuture<Void> waiter;
    while ((waiter = writableWaiters.poll()) != null) {
      waiter.complete(null);
    }
  }

  private void notifyWritabilityListener() {
    Consumer<Boolean> listener = this.writabilityListener;
    if (listener != null) {
      boolean current = isWritable();
      if (current != lastNotifiedWritable) {
        lastNotifiedWritable = current;
        try {
          listener.accept(current);
        } catch (Throwable t) {
          logger.error("Error in writability listener for stream {}", streamId, t);
        }
      }
    }
  }

  @Override
  public boolean isWritable() {
    return streamChannel.isWritable();
  }

  @Override
  public @NonNull CompletableFuture<Void> waitForWritable() {
    if (isWritable()) {
      return COMPLETED_FUTURE;
    }
    if (!streamChannel.isActive()) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      cf.completeExceptionally(new ClosedChannelException());
      return cf;
    }
    CompletableFuture<Void> cf = new CompletableFuture<>();
    writableWaiters.add(cf);
    // Double-check after enqueue to avoid race
    if (isWritable()) {
      drainWritableWaiters();
    }
    return cf;
  }

  @Override
  public void onWritabilityChanged(@NonNull Consumer<Boolean> listener) {
    this.writabilityListener = Objects.requireNonNull(listener, "listener cannot be null");
  }

  /**
   * Writes and flushes a Netty {@link ByteBuf} directly to the stream.
   *
   * @param buf the Netty ByteBuf to write and flush
   * @return a future that completes when the write operation is done
   */
  @Override
  public @NonNull CompletableFuture<Void> write(@NonNull ByteBuf buf) {
    return writeOutbound(buf);
  }

  /**
   * Writes and flushes a {@link WebTransportBuffer} to the stream.
   *
   * @param data the buffer to write
   * @return a future that completes when the write operation is done
   */
  public @NonNull CompletableFuture<Void> write(@NonNull WebTransportBuffer data) {
    if (data instanceof DefaultNettyWebTransportBuffer) {
      ByteBuf retained = ((DefaultNettyWebTransportBuffer) data).retainedReadableBuffer();
      try {
        return writeOutbound(retained);
      } catch (RuntimeException | Error e) {
        retained.release();
        throw e;
      }
    }
    if (data instanceof FlyweightWebTransportBuffer) {
      ByteBuf del = ((FlyweightWebTransportBuffer) data).delegate();
      if (del != null) {
        int len = del.readableBytes();
        ByteBuf packet = streamChannel.alloc().directBuffer(len);
        packet.writeBytes(del, del.readerIndex(), len);
        return writeOutbound(packet);
      }
    }
    return writeOutbound(Unpooled.wrappedBuffer(data.nioBuffer()));
  }

  /**
   * Writes and flushes a {@link BinarySource} to the stream.
   *
   * <p>This method enables efficient transmission of arbitrary data streams, files, or memory
   * regions by wrapping the source in a chunked input. The underlying stream pipeline handles the
   * fragmentation and asynchronous streaming.
   *
   * <p>Note: The provided {@code BinarySource} will be automatically closed by the underlying
   * pipeline when the streaming is complete or if an error occurs.
   *
   * @param binarySource the binary source to read and stream
   * @return a future that completes when the entire source has been written
   */
  public @NonNull CompletableFuture<Void> write(@NonNull BinarySource binarySource) {
    return toCompletableFuture(
        streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource)));
  }

  public @NonNull CompletableFuture<Void> write(@NonNull BinarySource binarySource, int chunkSize) {
    return toCompletableFuture(
        streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource, chunkSize)));
  }

  /**
   * Writes and flushes a byte array to the stream. This is a zero-copy operation that wraps the
   * byte array in a buffer.
   *
   * <p><strong>Caveat:</strong> The underlying array must not be modified until the returned future
   * completes, as it is read directly by the network transport thread.
   *
   * @param data the byte array to write
   * @return a future that completes when the write operation is done
   */
  public @NonNull CompletableFuture<Void> write(byte @NonNull [] data) {
    return writeOutbound(Unpooled.wrappedBuffer(data));
  }

  /**
   * Writes and flushes a slice of a byte array to the stream. This is a zero-copy operation that
   * wraps the array slice in a buffer.
   *
   * <p><strong>Caveat:</strong> The underlying array must not be modified until the returned future
   * completes, as it is read directly by the network transport thread.
   *
   * @param data the byte array containing the slice
   * @param offset the starting index in the array
   * @param length the number of bytes to write
   * @return a future that completes when the write operation is done
   */
  public @NonNull CompletableFuture<Void> write(byte @NonNull [] data, int offset, int length) {
    return writeOutbound(Unpooled.wrappedBuffer(data, offset, length));
  }

  /**
   * Writes and flushes a NIO {@link ByteBuffer} to the stream. This is a zero-copy operation that
   * wraps the buffer.
   *
   * <p><strong>Caveat:</strong> The underlying buffer must not be modified or written to until the
   * returned future completes.
   *
   * @param data the buffer to write
   * @return a future that completes when the write operation is done
   */
  public @NonNull CompletableFuture<Void> write(@NonNull ByteBuffer data) {
    return writeOutbound(Unpooled.wrappedBuffer(data));
  }

  /**
   * Writes and flushes a text string to the stream encoded as UTF-8.
   *
   * @param text the text to write
   * @return a future that completes when the write operation is done
   */
  public @NonNull CompletableFuture<Void> writeText(@NonNull String text) {
    return writeText(text, CharsetUtil.UTF_8);
  }

  /**
   * Writes and flushes a text string to the stream encoded using the specified charset.
   *
   * @param text the text to write
   * @param charset the character encoding to use
   * @return a future that completes when the write operation is done
   */
  @Override
  public @NonNull CompletableFuture<Void> writeText(
      @NonNull String text, @NonNull Charset charset) {
    ByteBuf buf = ByteBufUtil.encodeString(streamChannel.alloc(), CharBuffer.wrap(text), charset);
    return writeOutbound(buf);
  }

  @Override
  public void writeDirect(@NonNull WebTransportBuffer data) {
    if (!streamChannel.isActive()) {
      return;
    }
    if (data instanceof FlyweightWebTransportBuffer) {
      ByteBuf del = ((FlyweightWebTransportBuffer) data).delegate();
      if (del != null) {
        streamChannel.writeAndFlush(del.retain(), streamChannel.voidPromise());
        return;
      }
    }
    if (data instanceof DefaultNettyWebTransportBuffer) {
      streamChannel.writeAndFlush(
          ((DefaultNettyWebTransportBuffer) data).delegate().retain(), streamChannel.voidPromise());
      return;
    }
    ByteBuf packet = Unpooled.wrappedBuffer(data.nioBuffer());
    streamChannel.writeAndFlush(packet, streamChannel.voidPromise());
  }

  public void close() {
    streamChannel().close();
  }

  public void reset(long appErrorCode) {
    WebTransportUtils.resetStream(streamChannel(), appErrorCode);
  }

  /** Returns whether the given attribute key is present. */
  public boolean hasAttribute(@NonNull String key) {
    return attributes != null && attributes.containsKey(key);
  }

  /** Sets the attribute. */
  public @Nullable Object setAttribute(@NonNull String key, @Nullable Object value) {
    if (value == null) {
      return attributes == null ? null : attributes.remove(key);
    }
    Map<String, Object> attrs = attributes;
    if (attrs == null) {
      Map<String, Object> newMap = new ConcurrentHashMap<>();
      if (ATTRIBUTES_UPDATER.compareAndSet(this, null, newMap)) {
        attrs = newMap;
      } else {
        attrs = attributes;
      }
    }
    return attrs.put(key, value);
  }

  /** Returns the attribute cast to the given type, or null. */
  public <T> @Nullable T getAttribute(@NonNull String key, @NonNull Class<T> type) {
    if (attributes == null) {
      return null;
    }
    Object value = attributes.get(key);
    return value == null ? null : type.cast(value);
  }

  /** Returns the attribute cast to the given type, or the default value. */
  public <T> @Nullable T getAttributeOrDefault(
      @NonNull String key, @NonNull Class<T> type, @NonNull T defaultValue) {
    if (attributes == null) {
      return defaultValue;
    }
    Object value = attributes.get(key);
    return value == null ? defaultValue : type.cast(value);
  }

  /** Removes and returns the attribute associated with the given key. */
  public @Nullable Object removeAttribute(@NonNull String key) {
    return attributes == null ? null : attributes.remove(key);
  }

  /** Clears all attributes. */
  public void clearAttributes() {
    if (attributes != null) {
      attributes.clear();
    }
  }

  public int attributeCount() {
    return attributes == null ? 0 : attributes.size();
  }

  public boolean hasAttributes() {
    return attributes != null && !attributes.isEmpty();
  }

  /** Returns an immutable view of the attribute names. */
  public @NonNull Set<String> attributeNames() {
    return attributes == null
        ? Collections.emptySet()
        : Collections.unmodifiableSet(attributes.keySet());
  }

  public @NonNull Map<String, Object> getAttributes() {
    return attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(attributes);
  }

  @Override
  public boolean isActive() {
    return streamChannel().isActive();
  }

  @Override
  public @NonNull CompletableFuture<Void> shutdown(int error) {
    return toCompletableFuture(streamChannel().shutdown(error, streamChannel().newPromise()));
  }

  @Override
  public @NonNull CompletableFuture<Void> setPriority(@NonNull StreamPriority priority) {
    Objects.requireNonNull(priority, "priority cannot be null");
    return toCompletableFuture(
        streamChannel.updatePriority(
            new QuicStreamPriority(priority.urgency(), priority.isIncremental())));
  }

  @Override
  public @NonNull CompletableFuture<Void> setPriority(int urgency, boolean incremental) {
    return setPriority(StreamPriority.of(urgency, incremental));
  }

  @Override
  public @NonNull StreamPriority getPriority() {
    QuicStreamPriority quicPriority = streamChannel.priority();
    if (quicPriority == null) {
      return StreamPriority.DEFAULT;
    }
    return StreamPriority.of(quicPriority.urgency(), quicPriority.isIncremental());
  }
}
