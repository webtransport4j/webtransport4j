package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import io.github.webtransport4j.server.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class DefaultNettyWebTransportStream implements NettyWebTransportStream {
  private final QuicStreamChannel streamChannel;
  private final long sessionId;
  private final long streamId;
  private final boolean bidirectional;
  private Consumer<ByteBuf> dataConsumer;
  private Runnable closeHandler;
  private Consumer<Throwable> errorHandler;
  private final java.util.Map<String, Object> attributes = new java.util.concurrent.ConcurrentHashMap<>();

  public DefaultNettyWebTransportStream(QuicStreamChannel channel, long sessionId) {
    this.streamChannel = channel;
    this.sessionId = sessionId;
    this.streamId = channel.streamId();
    this.bidirectional = (channel.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
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


  public QuicStreamChannel streamChannel() {
    return streamChannel;
  }

  /**
   * Registers a callback to be invoked when stream payload data is received.
   * <p>
   * <strong>Memory Management Warning:</strong> The passed {@link ByteBuf} is owned and
   * automatically released by the network handler after this callback returns. If you process
   * this buffer asynchronously (e.g. offloading to another thread pool or a reactive pipeline),
   * you <strong>must</strong> call {@link ByteBuf#retain()} to increase its reference count, and
   * subsequently release it via {@link ByteBuf#release()} when done.
   *
   * @param consumer the data consumer callback
   */
  public void onData(Consumer<ByteBuf> consumer) {

    if (this.dataConsumer != null) {
        throw new IllegalStateException(
            "onData handler already registered");
    }

    this.dataConsumer = consumer;
  }

  public <T> void onData(
        StreamCodec<T> codec,
        Consumer<T> consumer) {

    // Create the auto-releasing wrapper once per stream instead of once per chunk
    // to prevent unnecessary garbage collection overhead in high-throughput streams.
    Consumer<T> autoReleasingConsumer = msg -> {
        try {
            consumer.accept(msg);
        } finally {
            // safeRelease prevents IllegalReferenceCountExceptions from masking
            // primary exceptions if the user manually released it already.
            ReferenceCountUtil.safeRelease(msg);
        }
    };

    this.onData(data -> {
        codec.decode(data, autoReleasingConsumer);
    });
  }

  public void onClose(Runnable handler) {
    this.closeHandler = handler;
  }

  public void onError(Consumer<Throwable> handler) {
    this.errorHandler = handler;
  }

  public Consumer<ByteBuf> getDataConsumer() {
    return dataConsumer;
  }

  public Runnable getCloseHandler() {
    return closeHandler;
  }

  public Consumer<Throwable> getErrorHandler() {
    return errorHandler;
  }

  /**
   * Writes and flushes a Netty {@link ByteBuf} to the stream.
   * Netty will automatically manage the reference count and release the buffer once sent.
   *
   * @param data the buffer to write
   * @return a future that completes when the write operation is done
   */
  public Future<Void> write(ByteBuf data) {
      return streamChannel.writeAndFlush(data);

  }

  /**
   * Writes and flushes a {@link BinarySource} to the stream.
   * <p>
   * This method enables efficient transmission of arbitrary data streams, files,
   * or memory regions by wrapping the source in a chunked input. The underlying
   * stream pipeline handles the fragmentation and asynchronous streaming.
   * <p>
   * Note: The provided {@code BinarySource} will be automatically closed by the
   * underlying pipeline when the streaming is complete or if an error occurs.
   *
   * @param binarySource the binary source to read and stream
   * @return a future that completes when the entire source has been written
   */
  public Future<Void> write(BinarySource binarySource) {
      return streamChannel.writeAndFlush(new BinarySourceChunkedInput(binarySource));
  }

  public Future<Void> write(BinarySource binarySource, int chunkSize) {
    return streamChannel.writeAndFlush(new BinarySourceChunkedInput(binarySource,chunkSize));
  }

  /**
   * Writes and flushes a byte array to the stream.
   * This is a zero-copy operation that wraps the byte array in a buffer.
   * <p>
   * <strong>Caveat:</strong> The underlying array must not be modified until the returned
   * future completes, as it is read directly by the network transport thread.
   *
   * @param data the byte array to write
   * @return a future that completes when the write operation is done
   */
  public Future<Void> write(byte[] data) {
    return streamChannel.writeAndFlush(Unpooled.wrappedBuffer(data));
  }

  /**
   * Writes and flushes a slice of a byte array to the stream.
   * This is a zero-copy operation that wraps the array slice in a buffer.
   * <p>
   * <strong>Caveat:</strong> The underlying array must not be modified until the returned
   * future completes, as it is read directly by the network transport thread.
   *
   * @param data the byte array containing the slice
   * @param offset the starting index in the array
   * @param length the number of bytes to write
   * @return a future that completes when the write operation is done
   */
  public Future<Void> write(byte[] data, int offset, int length) {
    return streamChannel.writeAndFlush(Unpooled.wrappedBuffer(data, offset, length));
  }

  /**
   * Writes and flushes a NIO {@link ByteBuffer} to the stream.
   * This is a zero-copy operation that wraps the buffer.
   * <p>
   * <strong>Caveat:</strong> The underlying buffer must not be modified or written to until
   * the returned future completes.
   *
   * @param data the buffer to write
   * @return a future that completes when the write operation is done
   */
  public Future<Void> write(ByteBuffer data) {
    return streamChannel.writeAndFlush(Unpooled.wrappedBuffer(data));
  }

  /**
   * Writes and flushes a text string to the stream encoded as UTF-8.
   *
   * @param text the text to write
   * @return a future that completes when the write operation is done
   */
  public Future<Void> writeText(String text) {
    return writeText(text, CharsetUtil.UTF_8);
  }

  /**
   * Writes and flushes a text string to the stream encoded using the specified charset.
   *
   * @param text the text to write
   * @param charset the character encoding to use
   * @return a future that completes when the write operation is done
   */
  public Future<Void> writeText(String text, java.nio.charset.Charset charset) {
    return streamChannel.writeAndFlush(Unpooled.copiedBuffer(text, charset));
  }


  public void close() {
    streamChannel.close();
  }

  public void reset(long appErrorCode) {
    WebTransportUtils.resetStream(streamChannel, appErrorCode);
  }

  public boolean hasAttribute(String key) {
      return attributes.containsKey(key);
  }

  public Object setAttribute(String key, Object value) {
      if (value == null) {
          return attributes.remove(key);
      }
      return attributes.put(key, value);
  }

  public <T> T getAttribute(String key, Class<T> type) {
      Object value = attributes.get(key);
      return value == null ? null : type.cast(value);
  }

  public <T> T getAttributeOrDefault(
          String key,
          Class<T> type,
          T defaultValue) {
      Object value = attributes.get(key);
      return value == null ? defaultValue : type.cast(value);
  }

  public Object removeAttribute(String key) {
      return attributes.remove(key);
  }

  public void clearAttributes() {
      attributes.clear();
  }

  public int attributeCount() {
      return attributes.size();
  }

  public boolean hasAttributes() {
      return !attributes.isEmpty();
  }

  public Set<String> attributeNames() {
      return Collections.unmodifiableSet(attributes.keySet());
  }


  public Map<String, Object> getAttributes() {
      return Collections.unmodifiableMap(attributes);
  }

  @Override
  public boolean isActive() {
    return streamChannel.isActive();
  }
}
