package io.github.webtransport4j.server;

import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.quic.QuicChannel;
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
public final class WebTransportCapsuleDecoder extends ByteToMessageDecoder {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportCapsuleDecoder.class);

  public static final int DEFAULT_MAX_CAPSULE_LENGTH = 65536;

  private final int maxCapsuleLength;
  private long cachedSessionId = -1L;

  public WebTransportCapsuleDecoder() {
    this(WebTransportConfig.getInt("webtransport4j.capsule.max_length", DEFAULT_MAX_CAPSULE_LENGTH));
  }

  public WebTransportCapsuleDecoder(int maxCapsuleLength) {
    this.maxCapsuleLength = maxCapsuleLength > 0 ? maxCapsuleLength : DEFAULT_MAX_CAPSULE_LENGTH;
  }

  public int maxCapsuleLength() {
    return maxCapsuleLength;
  }

  @Override
  public void handlerAdded(@NonNull ChannelHandlerContext ctx) {
    Long sessId = ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
    if (sessId != null) {
      this.cachedSessionId = sessId;
    } else if (ctx.channel() instanceof QuicStreamChannel) {
      this.cachedSessionId = ((QuicStreamChannel) ctx.channel()).streamId();
    }
  }

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
      if (capLen == -1) {
        in.resetReaderIndex();
        return;
      }
      if (capLen > maxCapsuleLength || capLen < 0) {
        logger.warn(
            "❌ WebTransport capsule length {} exceeds maximum allowed ({}). Closing stream.",
            capLen,
            maxCapsuleLength);
        in.skipBytes(in.readableBytes());
        if (ctx.channel() instanceof QuicStreamChannel) {
          QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
          QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
          if (quic != null && quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR) != null) {
            WebTransportSessionManager mgr =
                quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
            if (mgr != null) {
              mgr.unregister(streamChannel);
            }
          }
          streamChannel.shutdown(0x010e, streamChannel.newPromise());
        } else {
          ctx.close();
        }
        return;
      }
      if (in.readableBytes() < capLen) {
        in.resetReaderIndex();
        return;
      }
      ByteBuf capVal = in.readRetainedSlice((int) capLen);
      if (cachedSessionId == -1L) {
        Long sessId = ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
        cachedSessionId =
            (sessId != null) ? sessId : ((QuicStreamChannel) ctx.channel()).streamId();
      }
      if (logger.isTraceEnabled()) {
        logger.trace(
            "💊 Received Capsule | Type: 0x{} | Length: {} | Hex: {}",
            capType,
            capLen,
            ByteBufUtil.hexDump(capVal));
      }
      out.add(new WebTransportCapsule(cachedSessionId, capType, capVal));
    }
  }
}
