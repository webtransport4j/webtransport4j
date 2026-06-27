package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import io.github.webtransport4j.server.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class DefaultNettyWebTransportStream implements NettyWebTransportStream {

    private final @NonNull QuicStreamChannel streamChannel;

    private final long sessionId;

    private final long streamId;

    private final boolean bidirectional;

    private @Nullable Consumer<ByteBuf> dataConsumer;

    private @Nullable Runnable closeHandler;

    private @Nullable Consumer<Throwable> errorHandler;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

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
     * <p>
     * <strong>Memory Management Warning:</strong> The passed {@link ByteBuf} is owned and
     * automatically released by the network handler after this callback returns. If you process
     * this buffer asynchronously (e.g. offloading to another thread pool or a reactive pipeline),
     * you <strong>must</strong> call {@link ByteBuf#retain()} to increase its reference count, and
     * subsequently release it via {@link ByteBuf#release()} when done.
     *
     * @param consumer the data consumer callback
     */
    public void onData(@NonNull Consumer<ByteBuf> consumer) {
        if (this.dataConsumer != null) {
            throw new IllegalStateException("onData handler already registered");
        }
        this.dataConsumer = consumer;
    }

    public <T> void onData(@NonNull StreamCodec<T> codec, @NonNull Consumer<T> consumer) {
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

    public void onClose(@NonNull Runnable handler) {
        this.closeHandler = handler;
    }

    public void onError(@NonNull Consumer<Throwable> handler) {
        this.errorHandler = handler;
    }

    public @Nullable Consumer<ByteBuf> getDataConsumer() {
        return dataConsumer;
    }

    public @Nullable Runnable getCloseHandler() {
        return closeHandler;
    }

    public @Nullable Consumer<Throwable> getErrorHandler() {
        return errorHandler;
    }

    /**
     * Writes and flushes a Netty {@link ByteBuf} to the stream.
     * Netty will automatically manage the reference count and release the buffer once sent.
     *
     * @param data the buffer to write
     * @return a future that completes when the write operation is done
     */
    public @NonNull Future<Void> write(@NonNull ByteBuf data) {
        return streamChannel().writeAndFlush(data);
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
    public @NonNull Future<Void> write(@NonNull BinarySource binarySource) {
        return streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource));
    }

    public @NonNull Future<Void> write(@NonNull BinarySource binarySource, int chunkSize) {
        return streamChannel().writeAndFlush(new BinarySourceChunkedInput(binarySource, chunkSize));
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
    public @NonNull Future<Void> write(byte @NonNull [] data) {
        return streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data));
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
    public @NonNull Future<Void> write(byte @NonNull [] data, int offset, int length) {
        return streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data, offset, length));
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
    public @NonNull Future<Void> write(@NonNull ByteBuffer data) {
        return streamChannel().writeAndFlush(Unpooled.wrappedBuffer(data));
    }

    /**
     * Writes and flushes a text string to the stream encoded as UTF-8.
     *
     * @param text the text to write
     * @return a future that completes when the write operation is done
     */
    public @NonNull Future<Void> writeText(@NonNull String text) {
        return writeText(text, CharsetUtil.UTF_8);
    }

    /**
     * Writes and flushes a text string to the stream encoded using the specified charset.
     *
     * @param text the text to write
     * @param charset the character encoding to use
     * @return a future that completes when the write operation is done
     */
    public @NonNull Future<Void> writeText(@NonNull String text, java.nio.charset.@NonNull Charset charset) {
        return streamChannel().writeAndFlush(Unpooled.copiedBuffer(text, charset));
    }

    public void close() {
        streamChannel().close();
    }

    public void reset(long appErrorCode) {
        WebTransportUtils.resetStream(streamChannel(), appErrorCode);
    }

    public boolean hasAttribute(@NonNull String key) {
        return attributes.containsKey(key);
    }

    public @Nullable Object setAttribute(@NonNull String key, @Nullable Object value) {
        if (value == null) {
            return attributes.remove(key);
        }
        return attributes.put(key, value);
    }

    public <T> @Nullable T getAttribute(@NonNull String key, @NonNull Class<T> type) {
        Object value = attributes.get(key);
        return value == null ? null : type.cast(value);
    }

    public <T> @Nullable T getAttributeOrDefault(@NonNull String key, @NonNull Class<T> type, @NonNull T defaultValue) {
        Object value = attributes.get(key);
        return value == null ? defaultValue : type.cast(value);
    }

    public @Nullable Object removeAttribute(@NonNull String key) {
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

    public @NonNull Set<String> attributeNames() {
        return Collections.unmodifiableSet(attributes.keySet());
    }

    public @NonNull Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public boolean isActive() {
        return streamChannel().isActive();
    }
}
