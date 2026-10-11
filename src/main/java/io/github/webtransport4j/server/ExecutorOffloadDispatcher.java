package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opt-in {@link MessageDispatcher} that runs user handler callbacks on a caller-provided {@link
 * Executor} instead of the connection event loop. It is a safety valve for applications with
 * blocking or slow handlers: the default inline dispatch in {@link DefaultMessageDispatcher} stays
 * zero-GC and remains the recommended mode for non-blocking handlers.
 *
 * <p>Install it per connection via {@link
 * WebTransportServerBuilder#messageDispatcherSupplier(Supplier)}, e.g. {@code
 * builder.messageDispatcherSupplier(() -> new ExecutorOffloadDispatcher(
 * Executors.newSingleThreadExecutor(), true))}. Each dispatch submits one task carrying the payload
 * buffer; the task runs the user callback and releases the buffer when it completes.
 *
 * <p>Ordering is preserved per submission executor: callbacks for a channel are submitted from its
 * event loop in receive order, so a single-thread executor executes them in that same order.
 * {@code onIncomingStream} and {@code onError} callbacks are not covered and still run inline on
 * the event loop.
 */
public class ExecutorOffloadDispatcher extends DefaultMessageDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorOffloadDispatcher.class);

  private final Executor executor;
  private final boolean shutdownExecutorOnClose;

  /**
   * Creates a dispatcher that runs user callbacks on {@code executor}. The executor is never shut
   * down by this dispatcher.
   *
   * @param executor executor running the callbacks; a single-thread executor per connection is
   *     recommended so callbacks keep their receive order
   */
  public ExecutorOffloadDispatcher(@NonNull Executor executor) {
    this(executor, false);
  }

  /**
   * Creates a dispatcher that runs user callbacks on {@code executor} and optionally shuts it down
   * when the channel closes (only when it is an {@link ExecutorService}).
   *
   * @param executor executor running the callbacks
   * @param shutdownExecutorOnClose true to shut the executor down when the channel closes; intended
   *     for per-connection executors, leave false for shared pools
   */
  public ExecutorOffloadDispatcher(@NonNull Executor executor, boolean shutdownExecutorOnClose) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.shutdownExecutorOnClose =
        shutdownExecutorOnClose && executor instanceof ExecutorService;
  }

  @Override
  public void handlerAdded(@NonNull ChannelHandlerContext ctx) {
    if (shutdownExecutorOnClose) {
      ctx.channel().closeFuture().addListener(f -> ((ExecutorService) executor).shutdown());
    }
  }

  @Override
  protected void dispatchStreamData(
      @NonNull WebTransportStream stream, @NonNull ByteBuf content) {
    offload(content, () -> super.dispatchStreamData(stream, content));
  }

  @Override
  protected void dispatchRawStreamData(
      @NonNull Consumer<ByteBuf> rawConsumer, @NonNull ByteBuf content) {
    offload(content, () -> super.dispatchRawStreamData(rawConsumer, content));
  }

  @Override
  protected void dispatchRawDatagramData(
      @NonNull Consumer<ByteBuf> rawDatagramConsumer, @NonNull ByteBuf content) {
    offload(content, () -> super.dispatchRawDatagramData(rawDatagramConsumer, content));
  }

  @Override
  protected void dispatchDatagramData(
      @NonNull WebTransportHandler handler,
      @NonNull WebTransportSession session,
      @NonNull ByteBuf content) {
    offload(content, () -> super.dispatchDatagramData(handler, session, content));
  }

  private void offload(@NonNull ByteBuf content, @NonNull Runnable task) {
    try {
      executor.execute(task);
    } catch (RuntimeException e) {
      logger.error("Failed to offload handler callback to executor; dropping payload", e);
      if (content.refCnt() > 0) {
        content.release();
      }
    }
  }
}
