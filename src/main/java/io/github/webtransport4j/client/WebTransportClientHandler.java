package io.github.webtransport4j.client;

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
 * WebTransport client connection handler.
 *
 * @author https://github.com/sanjomo
 * @date 03/07/26 4:53 pm
 */
public class WebTransportClientHandler extends Http3ClientConnectionHandler {
  private static final Logger logger = LoggerFactory.getLogger(WebTransportClientHandler.class);

  public WebTransportClientHandler() {
    this(null, true, null);
  }

  /** Creates a client handler with the supplied HTTP/3 settings. */
  public WebTransportClientHandler(
      Http3SettingsFrame localSettings, boolean disableQpackDynamicTable) {
    this(localSettings, disableQpackDynamicTable, null);
  }

  /** Creates a client handler with the supplied HTTP/3 settings and validator. */
  public WebTransportClientHandler(
      Http3SettingsFrame localSettings,
      boolean disableQpackDynamicTable,
      Http3Settings.NonStandardHttp3SettingsValidator nonStandardSettingsValidator) {
    super(
        null,
        null,
        new UnknownStreamHandlerFactory(),
        localSettings,
        disableQpackDynamicTable,
        nonStandardSettingsValidator);
  }

  @Override
  protected void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel channel) {
    logger.info("Initializing bidirectional stream {}", channel.streamId());

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
    logger.info("Initializing unidirectional stream {}", streamChannel.streamId());

    streamChannel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof ByteBuf && logger.isDebugEnabled()) {
                  ByteBuf buf = (ByteBuf) msg;
                  logger.debug("=== UNI STREAM {} ===", streamChannel.streamId());
                  logger.debug(ByteBufUtil.prettyHexDump(buf));
                  logger.debug(
                      "ASCII: {}",
                      buf.toString(buf.readerIndex(), buf.readableBytes(), CharsetUtil.UTF_8));
                }

                ctx.fireChannelRead(msg);
              }
            });
  }
}
