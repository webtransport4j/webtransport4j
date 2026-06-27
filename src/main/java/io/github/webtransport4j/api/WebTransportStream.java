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

    void onData(Consumer<ByteBuf> consumer);

    void onClose(Runnable handler);

    void onError(Consumer<Throwable> handler);
    Consumer<Throwable> getErrorHandler();
    Consumer<ByteBuf> getDataConsumer();

    Runnable getCloseHandler();

    default <T> void onData(
            StreamCodec<T> codec,
            Consumer<T> consumer) {

        onData(buf ->
                codec.decode(buf, msg -> {
                    try {
                        consumer.accept(msg);
                    } finally {
                        codec.release(msg);
                    }
                }));
    }


    /* ---------- Primitive Write ---------- */

    Future<Void> write(ByteBuf data);

    Future<Void> write(BinarySource binarySource);

    Future<Void> write(BinarySource binarySource, int chunkSize);

    /* ---------- Convenience Writes ---------- */

    default Future<Void> write(byte[] data) {
        return write(Unpooled.wrappedBuffer(data));
    }

    default Future<Void> write(byte[] data,
                               int offset,
                               int length) {
        return write(Unpooled.wrappedBuffer(data, offset, length));
    }

    default Future<Void> write(ByteBuffer buffer) {
        return write(Unpooled.wrappedBuffer(buffer));
    }

    default Future<Void> writeText(String text) {
        return writeText(text, CharsetUtil.UTF_8);
    }

    default Future<Void> writeText(
            String text,
            Charset charset) {

        return write(
                Unpooled.copiedBuffer(text, charset));
    }


    /* ---------- Attributes ---------- */

    boolean hasAttribute(String key);

    Object setAttribute(String key, Object value);

    <T> T getAttribute(String key,
                       Class<T> type);

    <T> T getAttributeOrDefault(
            String key,
            Class<T> type,
            T defaultValue);

    Object removeAttribute(String key);

    void clearAttributes();

    int attributeCount();

    boolean hasAttributes();

    Set<String> attributeNames();

    Map<String, Object> getAttributes();

    boolean isActive();
}