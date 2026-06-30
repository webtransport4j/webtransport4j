package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportChunkedWriteHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Channel initializer for WebTransport streams. */
public final class WebTransportStreamChannelInitializer
    extends ChannelInitializer<QuicStreamChannel> {

  private static final Logger logger =
      LoggerFactory.getLogger(WebTransportStreamChannelInitializer.class);

  @Override
  protected void initChannel(@NonNull QuicStreamChannel stream) {
    WebTransportUtils.addTrafficShapers(stream);
    stream.pipeline().addFirst(new WebTransportDetectorHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportDetectorHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addFirst(new QuicGlobalSniffer("STREAM-" + stream.streamId()));
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added QuicGlobalSniffer (per-stream). Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new RawWebTransportHandler());
    if (logger.isDebugEnabled()) {
      logger.debug("🔧 Added RawWebTransportHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportChunkedWriteHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportChunkedWriteHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportStreamFrameDecoder. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportHeadersHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportHeadersHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new Http3DataToByteBufHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added Http3DataToByteBufHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportCapsuleDecoder());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportCapsuleDecoder. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportCapsuleHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportCapsuleHandler. Pipeline now: {}", stream.pipeline().names());
    }
    Supplier<MessageDispatcher> supplier =
        stream.parent().attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER).get();
    if (supplier != null) {
      stream.pipeline().addLast(supplier.get());
    } else {
      stream.pipeline().addLast(new DefaultMessageDispatcher());
    }
    if (logger.isDebugEnabled()) {
      logger.debug("🔧 Added MessageDispatcher. Pipeline now: {}", stream.pipeline().names());
    }
    stream
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void exceptionCaught(
                  @NonNull ChannelHandlerContext ctx, @NonNull Throwable cause) {
                logger.error("❌ PIPELINE ERROR: {} ", cause.getMessage(), cause);
              }
            });
  }
}
