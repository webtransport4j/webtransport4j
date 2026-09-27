package io.github.webtransport4j.api;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler supporting chunked writing of payloads with writability notifications. */
public final class WebTransportChunkedWriteHandler extends ChunkedWriteHandler {

  private static final Logger logger =
      LoggerFactory.getLogger(WebTransportChunkedWriteHandler.class);

  @Override
  public void write(
      @NonNull ChannelHandlerContext ctx, @NonNull Object msg, @NonNull ChannelPromise promise)
      throws Exception {
    if (logger.isDebugEnabled()) {
      logger.debug("ChunkedWriteHandler.write(): {}", msg.getClass());
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
      ((DefaultNettyWebTransportStream) stream).notifyWritabilityChanged(ctx.channel().isWritable());
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
}
