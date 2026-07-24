package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Netty-based implementation of WebTransport stream. */
public interface NettyWebTransportStream extends WebTransportStream {
  @Nullable Consumer<WebTransportBuffer> getDataConsumer();

  @Nullable OnCloseListener getCloseHandler();

  @Nullable Consumer<Throwable> getErrorHandler();

  @NonNull QuicStreamChannel streamChannel();

  /**
   * Writes and flushes a Netty {@link ByteBuf} directly to the stream.
   *
   * @param buf the Netty ByteBuf to write and flush
   * @return a future that completes when the write operation is done
   */
  @NonNull CompletableFuture<Void> write(@NonNull ByteBuf buf);
}
