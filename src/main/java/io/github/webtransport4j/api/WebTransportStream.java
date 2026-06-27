package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import io.netty.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import java.util.function.Consumer;

public interface WebTransportStream {

    /* ---------- Metadata ---------- */
    long sessionId();

    long streamId();

    boolean isBidirectional();

    /* ---------- Lifecycle ---------- */
    void close();

    void reset(long appErrorCode);

    /* ---------- Callbacks ---------- */
    void onData(@NonNull Consumer<ByteBuf> consumer);

    void onClose(@NonNull Runnable handler);

    void onError(@NonNull Consumer<Throwable> handler);

    @Nullable Consumer<Throwable> getErrorHandler();

    @Nullable Consumer<ByteBuf> getDataConsumer();

    @Nullable Runnable getCloseHandler();

    default <T> void onData(@NonNull StreamCodec<T> codec, @NonNull Consumer<T> consumer) {
        onData(buf -> codec.decode(buf, msg -> {
            try {
                consumer.accept(msg);
            } finally {
                codec.release(msg);
            }
        }));
    }

    /* ---------- Primitive Write ---------- */
    @NonNull Future<Void> write(@NonNull ByteBuf data);

    @NonNull Future<Void> write(@NonNull BinarySource binarySource);

    @NonNull Future<Void> write(@NonNull BinarySource binarySource, int chunkSize);

    /* ---------- Convenience Writes ---------- */
    default @NonNull Future<Void> write(@NonNull byte @NonNull [] data) {
        return write(Unpooled.wrappedBuffer(data));
    }

    default @NonNull Future<Void> write(@NonNull byte @NonNull [] data, int offset, int length) {
        return write(Unpooled.wrappedBuffer(data, offset, length));
    }

    default @NonNull Future<Void> write(@NonNull ByteBuffer buffer) {
        return write(Unpooled.wrappedBuffer(buffer));
    }

    default @NonNull Future<Void> writeText(@NonNull String text) {
        return writeText(text, CharsetUtil.UTF_8);
    }

    default @NonNull Future<Void> writeText(@NonNull String text, @NonNull Charset charset) {
        return write(Unpooled.copiedBuffer(text, charset));
    }

    /* ---------- Attributes ---------- */
    boolean hasAttribute(@NonNull String key);

    @Nullable Object setAttribute(@NonNull String key, @Nullable Object value);

    <T> @Nullable T getAttribute(@NonNull String key, @NonNull Class<T> type);

    <T> @Nullable T getAttributeOrDefault(@NonNull String key, @NonNull Class<T> type, @NonNull T defaultValue);

    @Nullable Object removeAttribute(@NonNull String key);

    void clearAttributes();

    int attributeCount();

    boolean hasAttributes();

    @NonNull Set<String> attributeNames();

    @NonNull Map<String, Object> getAttributes();

    boolean isActive();
}
