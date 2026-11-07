package io.github.webtransport4j.client;

import io.github.webtransport4j.server.Http3WtStreamFrameGuard;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebTransport client connection handler enforcing Draft-17 Sections 3.1, 3.2, and 4.3.
 *
 * @author https://github.com/sanjomo
 * @date 03/07/26 4:53 pm
 */
public class WebTransportClientHandler extends Http3ClientConnectionHandler {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportClientHandler.class);

  private static final Http3Settings.NonStandardHttp3SettingsValidator ALLOW_ALL_NON_STANDARD =
      (id, value) -> true;

  /** Creates a client handler with default settings. */
  public WebTransportClientHandler() {
    this(null, true, ALLOW_ALL_NON_STANDARD);
  }

  /** Creates a client handler with the supplied HTTP/3 settings. */
  public WebTransportClientHandler(
      Http3SettingsFrame localSettings, boolean disableQpackDynamicTable) {
    this(localSettings, disableQpackDynamicTable, ALLOW_ALL_NON_STANDARD);
  }

  /** Creates a client handler with the supplied HTTP/3 settings and validator. */
  public WebTransportClientHandler(
      Http3SettingsFrame localSettings,
      boolean disableQpackDynamicTable,
      Http3Settings.NonStandardHttp3SettingsValidator nonStandardSettingsValidator) {
    super(
        new Http3ClientControlStreamHandler(),
        null,
        new UnknownStreamHandlerFactory(),
        localSettings,
        disableQpackDynamicTable,
        nonStandardSettingsValidator != null
            ? nonStandardSettingsValidator
            : ALLOW_ALL_NON_STANDARD);
  }

  @Override
  protected void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel channel) {
    if (logger.isDebugEnabled()) {
      logger.debug("Initializing bidirectional stream {}", channel.streamId());
    }

    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof ByteBuf && logger.isDebugEnabled()) {
                  ByteBuf buf = (ByteBuf) msg;
                  logger.debug("=== BIDI STREAM {} ===", channel.streamId());
                  logger.debug(ByteBufUtil.prettyHexDump(buf));
                  logger.debug(
                      "ASCII: {}",
                      buf.toString(buf.readerIndex(), buf.readableBytes(), CharsetUtil.UTF_8));
                }

                ctx.fireChannelRead(msg);
              }
            });
  }

  @Override
  protected void initUnidirectionalStream(
      ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
    if (logger.isDebugEnabled()) {
      logger.debug("Initializing unidirectional stream {}", streamChannel.streamId());
    }
    streamChannel
        .pipeline()
        .addFirst(
            Http3WtStreamFrameGuard.HANDLER_NAME,
            Http3WtStreamFrameGuard.forUnidirectionalStream());
    super.initUnidirectionalStream(ctx, streamChannel);
  }
}
