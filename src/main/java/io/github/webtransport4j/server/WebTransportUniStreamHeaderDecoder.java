package io.github.webtransport4j.server;

import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
      if (!WebTransportUtils.initializeClientStream(ctx, this.streamType, sessionId)) {
        in.skipBytes(in.readableBytes());
        return;
      }
      sessionHeaderRead = true;
    }
    if (!in.isReadable()) {
      return;
    }
    int payloadBytes = in.readableBytes();
    if (!WebTransportUtils.recordStreamDataReceived(ctx, payloadBytes)) {
      in.skipBytes(in.readableBytes());
      return;
    }
    WebTransportServer server =
        (ctx.channel() != null && ctx.channel().parent() != null)
            ? ctx.channel().parent().attr(WebTransportAttributeKeys.SERVER_KEY).get()
            : null;
    boolean zeroGc = server != null && server.isZeroGc();
    if (zeroGc) {
      ctx.pipeline().replace(this, "zeroGcUniHandler", ZeroGcUniStreamHandler.INSTANCE);
    }
    out.add(in.readRetainedSlice(in.readableBytes()));
  }

  /**
   * Lightweight zero-GC handler replacing the decoder after the unidirectional stream header
   * has been consumed.
   */
  @ChannelHandler.Sharable
  public static final class ZeroGcUniStreamHandler extends ChannelInboundHandlerAdapter {

    public static final ZeroGcUniStreamHandler INSTANCE = new ZeroGcUniStreamHandler();

    @Override
    public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg)
        throws Exception {
      if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        int payloadBytes = buf.readableBytes();
        if (payloadBytes > 0) {
          if (!WebTransportUtils.recordStreamDataReceived(ctx, payloadBytes)) {
            buf.release();
            return;
          }
        }
        ctx.fireChannelRead(buf);
        return;
      }
      ctx.fireChannelRead(msg);
    }
  }
}
