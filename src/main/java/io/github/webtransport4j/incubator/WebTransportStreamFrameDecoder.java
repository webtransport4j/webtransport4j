package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.apache.log4j.Logger;

public class WebTransportStreamFrameDecoder extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger logger =
      Logger.getLogger(WebTransportStreamFrameDecoder.class.getName());

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    if (!(ctx.channel() instanceof QuicStreamChannel)) {
      ctx.fireChannelRead(msg.retain());
      return;
    }

    QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();
    Long typeAttr = stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get();
    Long sessId = stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();

    long sessionId = (sessId != null) ? sessId : stream.streamId();
    boolean bidirectional = (typeAttr == null || typeAttr != 0x54);

    if (logger.isDebugEnabled()) {
      logger.debug(
          String.format(
              "🖼️ Framing Stream Data: Session: %d | Stream: %d | Bidi: %b | Bytes: %d",
              sessionId, stream.streamId(), bidirectional, msg.readableBytes()));
    }

    WebTransportStreamFrame frame =
        new WebTransportStreamFrame(sessionId, stream.streamId(), bidirectional, msg.retain());
    ctx.fireChannelRead(frame);
  }
}
