package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;

import java.util.List;

class WebTransportDatagramDecoder
        extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger logger =
          Logger.getLogger(WebTransportDatagramDecoder.class.getName());
  @Override
  protected void decode(
          ChannelHandlerContext ctx,
          ByteBuf msg,
          List<Object> out) {
    if (logger.isDebugEnabled()) {
      logger.debug("☄️ DatagramHandler received data: " + msg.readableBytes() + " bytes");
    }
    long sessionId = WebTransportUtils.readVariableLengthInt(msg);
    if (sessionId == -1) {
      return;
    }

    ByteBuf payload = msg.readRetainedSlice(msg.readableBytes());

    out.add(new WebTransportDatagramFrame(sessionId, payload));
  }
}