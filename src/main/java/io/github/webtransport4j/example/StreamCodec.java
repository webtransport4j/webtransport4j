package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;

public interface StreamCodec<T> extends AutoCloseable {

    ByteBuf encode(T message);

    void decode(ByteBuf data, Consumer<T> consumer);

    @Override
    void close();
}