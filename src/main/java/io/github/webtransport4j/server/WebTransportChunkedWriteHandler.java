package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler supporting chunked writing of payloads with writability notifications. */
public class WebTransportChunkedWriteHandler extends ChunkedWriteHandler {

  private static final Logger logger =
      LoggerFactory.getLogger(WebTransportChunkedWriteHandler.class);

  @Override
  @SuppressWarnings("unchecked")
  public void write(
      @NonNull ChannelHandlerContext ctx, @NonNull Object msg, @NonNull ChannelPromise promise)
      throws Exception {
    if (logger.isDebugEnabled()) {
      logger.debug("ChunkedWriteHandler.write(): {}", msg.getClass());
    }
    if (msg instanceof ChunkedInput && !(msg instanceof FlowControlledChunkedInput)) {
      msg = new FlowControlledChunkedInput<>((ChunkedInput<Object>) msg, ctx);
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void flush(@NonNull ChannelHandlerContext ctx) throws Exception {
    if (logger.isDebugEnabled()) {
      logger.debug("ChunkedWriteHandler.flush()");
    }
    super.flush(ctx);
  }

  @Override
  public void channelWritabilityChanged(@NonNull ChannelHandlerContext ctx) throws Exception {
    super.channelWritabilityChanged(ctx);
    WebTransportStream stream = ctx.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
    if (stream instanceof DefaultNettyWebTransportStream) {
      ((DefaultNettyWebTransportStream) stream)
          .notifyWritabilityChanged(stream.isWritable());
    }
  }

  @Override
  public void channelInactive(@NonNull ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    WebTransportStream stream = ctx.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
    if (stream instanceof DefaultNettyWebTransportStream) {
      ((DefaultNettyWebTransportStream) stream).notifyClosed();
    }
  }

  /**
   * Flow-control aware ChunkedInput decorator that suspends chunk extraction when the WebTransport
   * stream or session is not writable.
   */
  private static final class FlowControlledChunkedInput<T> implements ChunkedInput<T> {
    private final ChunkedInput<T> delegate;
    private final ChannelHandlerContext ctx;

    private FlowControlledChunkedInput(
        @NonNull ChunkedInput<T> delegate, @NonNull ChannelHandlerContext ctx) {
      this.delegate = delegate;
      this.ctx = ctx;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
      return delegate.isEndOfInput();
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }

    @Override
    @Deprecated
    public @Nullable T readChunk(@NonNull ChannelHandlerContext context) throws Exception {
      WebTransportStream stream =
          context.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (stream != null && !stream.isWritable()) {
        return null;
      }
      return delegate.readChunk(context);
    }

    @Override
    public @Nullable T readChunk(@NonNull ByteBufAllocator allocator) throws Exception {
      WebTransportStream stream =
          ctx.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (stream != null && !stream.isWritable()) {
        return null;
      }
      return delegate.readChunk(allocator);
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public long progress() {
      return delegate.progress();
    }
  }
}
