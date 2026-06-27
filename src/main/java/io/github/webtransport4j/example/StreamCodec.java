package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.jspecify.annotations.NonNull;

public interface StreamCodec<T> {

    @NonNull ByteBuf encode(@NonNull T message);

    void decode(@NonNull ByteBuf incoming,
                @NonNull Consumer<T> consumer);

    default void release(@NonNull T message) {
        ReferenceCountUtil.safeRelease(message);
    }

    default void close() {}
}