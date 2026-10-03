package io.github.webtransport4j.server;

import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * Decodes WebTransport unidirectional stream headers.
 *
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:02 pm
 */
public class WebTransportUniStreamHeaderDecoder extends ByteToMessageDecoder {

  private boolean sessionHeaderRead = false;

  private final long streamType;

  public WebTransportUniStreamHeaderDecoder(long streamType) {
    this.streamType = streamType;
  }

  @Override
  protected void decode(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf in, @NonNull List<Object> out)
      throws Exception {
    if (!sessionHeaderRead) {
      in.markReaderIndex();
      long sessionId = readVariableLengthInt(in);
      if (sessionId == -1) {
        in.resetReaderIndex();
        return;
      }
      // Write all stream-invariant attributes exactly once
      ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);
      ctx.channel().attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set(this.streamType);
      String savedPath = null;
      if (ctx.channel().parent() != null) {
        io.netty.handler.codec.quic.QuicChannel parentQuic =
            (io.netty.handler.codec.quic.QuicChannel) ctx.channel().parent();
        io.netty.util.Attribute<WebTransportSessionManager> mgrAttr =
            parentQuic.attr(WebTransportAttributeKeys.WT_SESSION_MGR);
        WebTransportSessionManager mgr = mgrAttr != null ? mgrAttr.get() : null;
        if (mgr != null) {
          io.github.webtransport4j.api.WebTransportSession session = mgr.get(sessionId);
          if (session != null) {
            savedPath = session.path();
          }
        }
        if (savedPath == null
            && parentQuic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY) != null) {
          savedPath = parentQuic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).get();
        }
      }
      ctx.channel().attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set(savedPath);
      sessionHeaderRead = true;
    }
    if (!in.isReadable()) {
      return;
    }
    out.add(in.readRetainedSlice(in.readableBytes()));
  }
}
