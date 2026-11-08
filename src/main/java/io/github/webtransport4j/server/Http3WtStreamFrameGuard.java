package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http3.Http3UnknownFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.ReferenceCountUtil;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zero-allocation, zero-copy HTTP/3 frame boundary guard enforcing Draft-17 Section 4.3.
 *
 * <p>Per {@code draft-ietf-webtrans-http3-17} Section 4.3, endpoints MUST NOT send {@code
 * WT_STREAM} ({@code 0x41}) as a frame type on HTTP/3 streams other than as the very first bytes
 * sent by the initiator of a bidirectional stream. Receiving {@code 0x41} in any other circumstance
 * (such as on an HTTP/3 control stream, push stream, after initial {@code HEADERS} on a request or
 * {@code CONNECT} stream, or from the responder of a request stream) MUST be treated as a
 * connection error of type {@code H3_FRAME_UNEXPECTED} ({@code 0x0105}).
 *
 * <p>Because {@code WT_STREAM} has no HTTP/3 {@code Length} field, inspecting raw frame-type
 * boundaries ahead of Netty's {@code Http3FrameCodec} guarantees immediate {@code 0x0105}
 * termination without stalling on missing length bytes or misreporting {@code H3_FRAME_ERROR}
 * ({@code 0x0106}) at stream EOF.
 */
public final class Http3WtStreamFrameGuard extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(Http3WtStreamFrameGuard.class);

  /** Handler name used when attaching to stream pipelines. */
  public static final String HANDLER_NAME = "http3WtStreamFrameGuard";

  /** Shared connection-level installer for inbound unidirectional streams. */
  public static final UnidirectionalStreamInstaller UNIDIRECTIONAL_INSTALLER =
      new UnidirectionalStreamInstaller();

  private static final byte STATE_UNI_STREAM_TYPE = 0;
  private static final byte STATE_PUSH_ID = 1;
  private static final byte STATE_FRAME_TYPE = 2;
  private static final byte STATE_FRAME_LENGTH = 3;
  private static final byte STATE_FRAME_PAYLOAD = 4;
  private static final byte STATE_BYPASS = 5;
  private static final byte STATE_REJECTED = 6;

  private static final BiConsumer<ChannelHandlerContext, Integer> DEFAULT_CONNECTION_CLOSER =
      (ctx, errorCode) -> {
        QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
        if (quic != null) {
          quic.close(true, errorCode, Unpooled.EMPTY_BUFFER);
        } else {
          ctx.close();
        }
      };

  private final BiConsumer<ChannelHandlerContext, Integer> connectionErrorCloser;
  private byte state;
  private int varIntExpectedBytes;
  private int varIntReadBytes;
  private long varIntAccumulator;
  private long remainingPayloadBytes;

  private Http3WtStreamFrameGuard(
      byte initialState,
      @NonNull BiConsumer<ChannelHandlerContext, Integer> connectionErrorCloser) {
    this.state = initialState;
    this.connectionErrorCloser =
        Objects.requireNonNull(connectionErrorCloser, "connectionErrorCloser must not be null");
  }

  /**
   * Creates a guard positioned at the start of HTTP/3 frames on a bidirectional request stream.
   *
   * @return frame guard starting at HTTP/3 frame-type boundary
   */
  public static @NonNull Http3WtStreamFrameGuard forRequestStream() {
    return new Http3WtStreamFrameGuard(STATE_FRAME_TYPE, DEFAULT_CONNECTION_CLOSER);
  }

  /**
   * Creates a guard for a bidirectional request stream with a custom connection-error callback.
   *
   * @param connectionErrorCloser callback invoked with {@code H3_FRAME_UNEXPECTED} ({@code 0x0105})
   * @return frame guard starting at HTTP/3 frame-type boundary
   */
  public static @NonNull Http3WtStreamFrameGuard forRequestStream(
      @NonNull BiConsumer<ChannelHandlerContext, Integer> connectionErrorCloser) {
    return new Http3WtStreamFrameGuard(STATE_FRAME_TYPE, connectionErrorCloser);
  }

  /**
   * Creates a guard positioned at the initial stream-type varint of an inbound unidirectional
   * stream.
   *
   * <p>Monitors HTTP/3 control ({@code 0x00}) and push ({@code 0x01}) streams and removes itself
   * with zero further overhead on QPACK ({@code 0x02}/{@code 0x03}) or WebTransport unidirectional
   * ({@code 0x54}) streams.
   *
   * @return frame guard starting at unidirectional stream-type header
   */
  public static @NonNull Http3WtStreamFrameGuard forUnidirectionalStream() {
    return new Http3WtStreamFrameGuard(STATE_UNI_STREAM_TYPE, DEFAULT_CONNECTION_CLOSER);
  }

  /**
   * Creates a guard for an inbound unidirectional stream with a custom connection-error callback.
   *
   * @param connectionErrorCloser callback invoked with {@code H3_FRAME_UNEXPECTED} ({@code 0x0105})
   * @return frame guard starting at unidirectional stream-type header
   */
  public static @NonNull Http3WtStreamFrameGuard forUnidirectionalStream(
      @NonNull BiConsumer<ChannelHandlerContext, Integer> connectionErrorCloser) {
    return new Http3WtStreamFrameGuard(STATE_UNI_STREAM_TYPE, connectionErrorCloser);
  }

  /**
   * Closes the parent QUIC connection (or channel context) with {@code H3_FRAME_UNEXPECTED} ({@code
   * 0x0105}).
   *
   * @param ctx channel handler context where unexpected {@code WT_STREAM} was observed
   */
  public static void closeWithFrameUnexpected(@NonNull ChannelHandlerContext ctx) {
    DEFAULT_CONNECTION_CLOSER.accept(ctx, WebTransportProtocolSpec.H3_FRAME_UNEXPECTED);
  }

  @Override
  public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg) {
    if (msg instanceof Http3UnknownFrame) {
      Http3UnknownFrame unknown = (Http3UnknownFrame) msg;
      if (unknown.type() == WebTransportUtils.BI_STREAM_TYPE) {
        ReferenceCountUtil.release(unknown);
        rejectUnexpectedWtStream(ctx);
        return;
      }
      ctx.fireChannelRead(msg);
      return;
    }

    if (!(msg instanceof ByteBuf)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ByteBuf buf = (ByteBuf) msg;
    if (state == STATE_REJECTED) {
      buf.release();
      return;
    }
    if (state == STATE_BYPASS) {
      ctx.fireChannelRead(buf);
      return;
    }

    int cursor = buf.readerIndex();
    final int limit = buf.writerIndex();

    while (cursor < limit) {
      if (state == STATE_FRAME_PAYLOAD) {
        cursor = consumePayload(cursor, limit);
        if (cursor == limit) {
          break;
        }
        continue;
      }

      cursor = readVarInt(buf, cursor, limit);
      if (varIntReadBytes < varIntExpectedBytes) {
        break;
      }

      long decoded = completeVarInt();
      if (!processDecodedVarInt(ctx, buf, decoded)) {
        return;
      }
    }

    ctx.fireChannelRead(buf);
  }

  private int consumePayload(int cursor, int limit) {
    long available = limit - cursor;
    if (available <= remainingPayloadBytes) {
      remainingPayloadBytes -= available;
      if (remainingPayloadBytes == 0L) {
        state = STATE_FRAME_TYPE;
      }
      return limit;
    }
    int newCursor = cursor + (int) remainingPayloadBytes;
    remainingPayloadBytes = 0L;
    state = STATE_FRAME_TYPE;
    return newCursor;
  }

  private int readVarInt(@NonNull ByteBuf buf, int cursor, int limit) {
    int cur = cursor;
    if (varIntExpectedBytes == 0) {
      int first = buf.getUnsignedByte(cur++);
      varIntExpectedBytes = 1 << (first >>> 6);
      varIntAccumulator = first & 0x3FL;
      varIntReadBytes = 1;
    }
    while (varIntReadBytes < varIntExpectedBytes && cur < limit) {
      varIntAccumulator = (varIntAccumulator << 8) | buf.getUnsignedByte(cur++);
      varIntReadBytes++;
    }
    return cur;
  }

  private long completeVarInt() {
    final long decoded = varIntAccumulator;
    varIntExpectedBytes = 0;
    varIntReadBytes = 0;
    varIntAccumulator = 0L;
    return decoded;
  }

  private boolean processDecodedVarInt(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf buf, long decoded) {
    switch (state) {
      case STATE_UNI_STREAM_TYPE:
        return handleUniStreamType(ctx, buf, decoded);
      case STATE_PUSH_ID:
        state = STATE_FRAME_TYPE;
        return true;
      case STATE_FRAME_TYPE:
        return handleFrameType(ctx, buf, decoded);
      case STATE_FRAME_LENGTH:
        handleFrameLength(decoded);
        return true;
      default:
        return true;
    }
  }

  private boolean handleUniStreamType(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf buf, long streamType) {
    if (streamType == WebTransportProtocolSpec.H3_STREAM_TYPE_CONTROL) {
      state = STATE_FRAME_TYPE;
      return true;
    }
    if (streamType == WebTransportProtocolSpec.H3_STREAM_TYPE_PUSH) {
      state = STATE_PUSH_ID;
      return true;
    }
    state = STATE_BYPASS;
    if (ctx.pipeline().context(this) != null) {
      ctx.pipeline().remove(this);
    }
    ctx.fireChannelRead(buf);
    return false;
  }

  private boolean handleFrameType(
      @NonNull ChannelHandlerContext ctx, @NonNull ByteBuf buf, long frameType) {
    if (frameType == WebTransportUtils.BI_STREAM_TYPE) {
      buf.release();
      rejectUnexpectedWtStream(ctx);
      return false;
    }
    state = STATE_FRAME_LENGTH;
    return true;
  }

  private void handleFrameLength(long length) {
    if (length == 0L) {
      state = STATE_FRAME_TYPE;
    } else {
      remainingPayloadBytes = length;
      state = STATE_FRAME_PAYLOAD;
    }
  }

  private void rejectUnexpectedWtStream(@NonNull ChannelHandlerContext ctx) {
    state = STATE_REJECTED;
    logger.warn(
        "Received unexpected WT_STREAM (0x41) frame on HTTP/3 stream. Closing connection with"
            + " H3_FRAME_UNEXPECTED (0x0105).");
    connectionErrorCloser.accept(ctx, WebTransportProtocolSpec.H3_FRAME_UNEXPECTED);
  }

  /**
   * Sharable {@link QuicChannel} handler that prepends {@link
   * Http3WtStreamFrameGuard#forUnidirectionalStream()} onto newly accepted peer unidirectional
   * streams before {@code Http3ConnectionHandler} attaches its codec.
   */
  @Sharable
  public static final class UnidirectionalStreamInstaller extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg) {
      if (msg instanceof QuicStreamChannel) {
        QuicStreamChannel stream = (QuicStreamChannel) msg;
        if (stream.type() == QuicStreamType.UNIDIRECTIONAL
            && stream.pipeline().get(Http3WtStreamFrameGuard.class) == null) {
          stream.pipeline().addFirst(HANDLER_NAME, forUnidirectionalStream());
        }
      }
      ctx.fireChannelRead(msg);
    }
  }
}

