package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

public interface StreamCodec<T> {

    ByteBuf encode(T message);

    void decode(ByteBuf incoming,
                Consumer<T> consumer);

    default void release(T message) {
        ReferenceCountUtil.safeRelease(message);
    }

    default void close() {}
}