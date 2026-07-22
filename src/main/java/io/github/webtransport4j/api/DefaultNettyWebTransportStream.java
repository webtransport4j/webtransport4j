package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;

import java.nio.ByteBuffer;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Default Netty-based WebTransport stream implementation. */
public class DefaultNettyWebTransportStream implements NettyWebTransportStream {

  private final @NonNull QuicStreamChannel streamChannel;

  private final long sessionId;

  private final long streamId;

  private final boolean bidirectional;

  private @Nullable Consumer<WebTransportBuffer> dataConsumer;

  private @Nullable OnCloseListener closeHandler;

  private @Nullable Consumer<Throwable> errorHandler;

  private @Nullable Map<String, Object> attributes;

  private static @NonNull CompletableFuture<Void> toCompletableFuture(
      @NonNull Future<?> nettyFuture) {
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

  /** Default Netty Web Transport Stream. */
  public DefaultNettyWebTransportStream(@NonNull QuicStreamChannel channel, long sessionId) {
    this.streamChannel = Objects.requireNonNull(channel, "channel must not be null");
    this.sessionId = sessionId;
    this.streamId = channel.streamId();
    this.bidirectional = (channel.type() == QuicStreamType.BIDIRECTIONAL);
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
        return toCompletableFuture(streamChannel().writeAndFlush(retained));
      } catch (RuntimeException | Error e) {
        retained.release();
        throw e;
      }
    }
    return toCompletableFuture(streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data.nioBuffer())));
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
    return toCompletableFuture(streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource)));
  }

  public @NonNull CompletableFuture<Void> write(@NonNull BinarySource binarySource, int chunkSize) {
    return toCompletableFuture(streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource, chunkSize)));
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
    return toCompletableFuture(streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data)));
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
    return toCompletableFuture(streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data, offset, length)));
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
    return toCompletableFuture(streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data)));
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
  public @NonNull CompletableFuture<Void> writeText(
      @NonNull String text, java.nio.charset.@NonNull Charset charset) {
    return toCompletableFuture(streamChannel().writeAndFlush(Unpooled.copiedBuffer(text, charset)));
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
    if (attributes == null) {
      attributes = new Object2ObjectOpenHashMap<>();
    }
    return attributes.put(key, value);
  }

  public <T> @Nullable T getAttribute(@NonNull String key, @NonNull Class<T> type) {
    if (attributes == null) {
      return null;
    }
    Object value = attributes.get(key);
    return value == null ? null : type.cast(value);
  }

  public <T> @Nullable T getAttributeOrDefault(
      @NonNull String key, @NonNull Class<T> type, @NonNull T defaultValue) {
    if (attributes == null) {
      return defaultValue;
    }
    Object value = attributes.get(key);
    return value == null ? defaultValue : type.cast(value);
  }

  public @Nullable Object removeAttribute(@NonNull String key) {
    return attributes == null ? null : attributes.remove(key);
  }

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

  public @NonNull Set<String> attributeNames() {
    return attributes == null ? Collections.emptySet() : Collections.unmodifiableSet(attributes.keySet());
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
}
