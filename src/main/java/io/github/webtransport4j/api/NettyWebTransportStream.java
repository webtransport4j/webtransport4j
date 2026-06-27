package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicStreamChannel;

import java.util.function.Consumer;

public interface NettyWebTransportStream extends WebTransportStream{
    Consumer<ByteBuf> getDataConsumer();

    Runnable getCloseHandler();

    Consumer<Throwable> getErrorHandler();

    QuicStreamChannel streamChannel();
}
