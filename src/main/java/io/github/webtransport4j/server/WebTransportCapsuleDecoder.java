package io.github.webtransport4j.server;

import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes WebTransport capsule protocol messages.
 *
 * @author https://github.com/sanjomo
 * @date 24/06/26 1:08 pm
 */
final class WebTransportCapsuleDecoder extends ByteToMessageDecoder {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportCapsuleDecoder.class);

  private long cachedSessionId = -1L;

  @Override
  protected void decode(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf in, @NonNull List<Object> out) {
    while (true) {
      in.markReaderIndex();
      long capType = readVariableLengthInt(in);
      if (capType == -1) {
        in.resetReaderIndex();
        return;
      }
      long capLen = readVariableLengthInt(in);
      if (capLen == -1 || in.readableBytes() < capLen) {
        in.resetReaderIndex();
        return;
      }
      ByteBuf capVal = in.readRetainedSlice((int) capLen);
      if (cachedSessionId == -1L) {
        Long sessId = ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
        cachedSessionId = (sessId != null) ? sessId : ((QuicStreamChannel) ctx.channel()).streamId();
      }
      if (logger.isTraceEnabled()) {
        logger.trace("💊 Received Capsule | Type: 0x{} | Length: {} | Hex: {}",
                capType, capLen, ByteBufUtil.hexDump(capVal));
      }
      out.add(new WebTransportCapsule(cachedSessionId, capType, capVal));
    }
  }
}
