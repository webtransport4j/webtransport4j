package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

class WebTransportDatagramHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger logger =
      Logger.getLogger(WebTransportDatagramHandler.class.getName());

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    if (logger.isDebugEnabled()) {
      logger.debug("☄️ DatagramHandler received data: " + msg.readableBytes() + " bytes");
    }

    long sessionId = WebTransportUtils.readVariableLengthInt(msg);
    if (sessionId == -1) {
      logger.warn("Received malformed datagram with invalid session ID prefix");
      return;
    }

    ByteBuf payload = msg.readRetainedSlice(msg.readableBytes());
    WebTransportDatagramFrame frame = new WebTransportDatagramFrame(sessionId, payload);
    ctx.fireChannelRead(frame);
  }
}
