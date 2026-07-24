package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportChunkedWriteHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.channel.WriteBufferWaterMark;
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
    int lowWaterMark = WebTransportConfig.getInt("webtransport4j.netty.write_buffer.low_water_mark", 32768);
    int highWaterMark = WebTransportConfig.getInt("webtransport4j.netty.write_buffer.high_water_mark", 65536);
    stream.config().setWriteBufferWaterMark(new WriteBufferWaterMark(lowWaterMark, highWaterMark));

    WebTransportUtils.addTrafficShapers(stream);
    stream.pipeline().addFirst(new WebTransportDetectorHandler());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportDetectorHandler. Pipeline now: {}", stream.pipeline().names());
    }
    if (logger.isDebugEnabled()) {
      stream.pipeline().addFirst(new QuicGlobalSniffer("STREAM-" + stream.streamId()));
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
    stream.pipeline().addLast(WebTransportStreamFrameDecoder.INSTANCE);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportStreamFrameDecoder. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(WebTransportHeadersHandler.INSTANCE);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportHeadersHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(Http3DataToByteBufHandler.INSTANCE);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added Http3DataToByteBufHandler. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(new WebTransportCapsuleDecoder());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportCapsuleDecoder. Pipeline now: {}", stream.pipeline().names());
    }
    stream.pipeline().addLast(WebTransportCapsuleHandler.INSTANCE);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔧 Added WebTransportCapsuleHandler. Pipeline now: {}", stream.pipeline().names());
    }
    Supplier<MessageDispatcher> supplier =
        stream.parent().attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER).get();
    if (supplier != null) {
      stream.pipeline().addLast(supplier.get());
    } else {
      stream.pipeline().addLast(DefaultMessageDispatcher.INSTANCE);
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
                if (cause instanceof AssertionError || cause.getClass().getName().contains("Qpack")) {
                  logger.debug("QPACK encoder teardown notice: {}", cause.getMessage());
                  return;
                }
                logger.error("❌ PIPELINE ERROR: {} ", cause.getMessage(), cause);
              }
            });
  }
}
