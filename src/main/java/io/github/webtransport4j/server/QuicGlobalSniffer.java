package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

class QuicGlobalSniffer extends ChannelInboundHandlerAdapter {
  private static final Logger logger = Logger.getLogger(QuicGlobalSniffer.class.getName());
  private final String prefix;

  public QuicGlobalSniffer(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof ByteBuf) {
      ByteBuf data = (ByteBuf) msg;
      int len = data.readableBytes();
      String hex = ByteBufUtil.hexDump(data);
      // Formatting for readability
      if (len > 0) {
        logger.debug("👀 [" + prefix + "] ID:" + ctx.channel().id().asShortText() + " LEN:" + len);
        logger.debug("    HEX: " + hex);
      }
    } else {
      logger.debug("👀 [" + prefix + "] MsgType: " + msg.getClass().getSimpleName());
    }
    // Pass it on!
    ctx.fireChannelRead(msg);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    logger.debug("👀 [" + prefix + "] UserEvent: " + evt.getClass().getName() + " -> " + evt);
    ctx.fireUserEventTriggered(evt);
  }
}
