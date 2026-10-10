package io.github.webtransport4j.server;

import io.github.webtransport4j.api.OnCloseListener;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportStream;
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

  /**
   * Power-user escape hatch: registers a direct callback for raw Netty {@link ByteBuf} instances.
   *
   * <p>When registered, incoming stream payloads are passed directly to this consumer without
   * allocating a {@link WebTransportBuffer} wrapper. The callback executes synchronously on the
   * Netty EventLoop. The buffer is automatically released after the consumer returns; if retained
   * asynchronously, caller must explicitly call {@code buf.retain()} and later {@code buf.release()}.
   *
   * @param consumer the raw ByteBuf consumer
   */
  void onRawByteBuf(@NonNull Consumer<ByteBuf> consumer);

  /**
   * Returns the registered raw ByteBuf consumer, if any.
   *
   * @return the raw ByteBuf consumer, or null
   */
  @Nullable Consumer<ByteBuf> getRawByteBufConsumer();

  /**
   * Writes and flushes a Netty {@link ByteBuf} directly using a void promise for zero-allocation.
   *
   * @param buf the buffer to write
   */
  void writeDirect(@NonNull ByteBuf buf);
}
