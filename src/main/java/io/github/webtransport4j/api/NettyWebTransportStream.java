package io.github.webtransport4j.api;

import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Netty-based implementation of WebTransport stream. */
public interface NettyWebTransportStream extends WebTransportStream {
  @Nullable Consumer<WebTransportBuffer> getDataConsumer();

  @Nullable OnCloseListener getCloseHandler();

  @Nullable Consumer<Throwable> getErrorHandler();

  @NonNull QuicStreamChannel streamChannel();
}
