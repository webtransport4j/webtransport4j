package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public interface StreamCodec<T> {

    ByteBuf encode(ByteBufAllocator alloc, T message);

    void decode(ByteBuf data, Consumer<T> consumer);

}