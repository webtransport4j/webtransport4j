package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decoder for ByteBuf data into stream frames. */
public final class WebTransportStreamFrameDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger logger =
      LoggerFactory.getLogger(WebTransportStreamFrameDecoder.class);

  @Override
  protected void decode(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf msg, @NonNull List<Object> out)
      throws Exception {
    if (!(ctx.channel() instanceof QuicStreamChannel)) {
      out.add(msg.retain());
      return;
    }
    QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();
    Long typeAttr = stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get();
    Long sessId = stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
    long sessionId = (sessId != null) ? sessId : stream.streamId();
    boolean bidirectional = (typeAttr == null || typeAttr == WebTransportUtils.BI_STREAM_TYPE);
    if (logger.isDebugEnabled()) {
      logger.debug("🖼️ Framing Stream Data: Session: {} | Stream: {} | Bidi: {} | Bytes: {}",
              sessionId, stream.streamId(), bidirectional, msg.readableBytes());
    }
    out.add(new WebTransportStreamFrame(sessionId, stream.streamId(), bidirectional, msg.retain()));
  }
}
