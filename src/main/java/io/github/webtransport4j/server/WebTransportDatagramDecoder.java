package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class WebTransportDatagramDecoder extends MessageToMessageDecoder<ByteBuf> {

  public static final WebTransportDatagramDecoder INSTANCE = new WebTransportDatagramDecoder();

  private static final Logger logger = LoggerFactory.getLogger(WebTransportDatagramDecoder.class);

  @Override
  protected void decode(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf msg, @NonNull List<Object> out) {
    if (logger.isDebugEnabled()) {
      logger.debug("☄️ DatagramHandler received data: {} bytes", msg.readableBytes());
    }
    long quarterSessionId = WebTransportUtils.readVariableLengthInt(msg);
    if (quarterSessionId == -1) {
      return;
    }
    ByteBuf payload = msg.readRetainedSlice(msg.readableBytes());
    out.add(new WebTransportDatagramFrame(quarterSessionId<<2, payload));
  }
}
