package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.apache.log4j.Logger;

import java.util.List;

public final class WebTransportStreamFrameDecoder
        extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger logger =
          Logger.getLogger(WebTransportStreamFrameDecoder.class.getName());

  @Override
  protected void decode(
          ChannelHandlerContext ctx,
          ByteBuf msg,
          List<Object> out)
          throws Exception {

    if (!(ctx.channel() instanceof QuicStreamChannel)) {
      out.add(msg.retain());
      return;
    }

    QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();

    Long typeAttr =
            stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get();

    Long sessId =
            stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();

    long sessionId =
            (sessId != null) ? sessId : stream.streamId();

    boolean bidirectional =
            (typeAttr == null || typeAttr != 0x54);

    if (logger.isDebugEnabled()) {
      logger.debug(
              String.format(
                      "🖼️ Framing Stream Data: Session: %d | Stream: %d | Bidi: %b | Bytes: %d",
                      sessionId,
                      stream.streamId(),
                      bidirectional,
                      msg.readableBytes()));
    }

    out.add(
            new WebTransportStreamFrame(
                    sessionId,
                    stream.streamId(),
                    bidirectional,
                    msg.retain()));
  }
}
