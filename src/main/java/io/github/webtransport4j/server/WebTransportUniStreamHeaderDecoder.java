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
    out.add(in.readRetainedSlice(in.readableBytes()));
  }
}
