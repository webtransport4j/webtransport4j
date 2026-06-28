package io.github.webtransport4j.api;

import io.netty.handler.codec.quic.QuicStreamChannel;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

public interface NettyWebTransportStream extends WebTransportStream{
    @Nullable Consumer<WebTransportBuffer> getDataConsumer();

    @Nullable Runnable getCloseHandler();

    @Nullable Consumer<Throwable> getErrorHandler();

    @NonNull QuicStreamChannel streamChannel();
}
