package io.github.webtransport4j.incubator;

import static io.github.webtransport4j.incubator.WebTransportUtils.readVariableLengthInt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.apache.log4j.Logger;

/**
 * Handler responsible for parsing WebTransport capsules carried within HTTP/3 DATA frames on the
 * CONNECT control stream.
 *
 * <p>Because capsules may be fragmented or merged across TCP/QUIC stream packets, this handler uses
 * a stateful {@code cumulation} buffer. It accumulates incoming bytes and parses complete capsules
 * in a rollback-safe manner using {@code markReaderIndex()} and {@code resetReaderIndex()}.
 *
 * <h3>Pictorial Flow of Fragmentation</h3>
 *
 * <pre>
 *                   ┌───────────────── Capsule 1 (3 Bytes) ───────────────┐
 * Wire Data:        │  Type (0x68 0x43)  │  Length (0x00)  │  Value (empty)  │
 *                   └────────────────────┴─────────────────┴─────────────────┘
 *                                      \                 /
 *                                       \               /
 * Split across packets:                  \             /
 *                                   ┌─────▼───────────▼───┐     ┌───▼───┐
 *                                   │      Packet A       │     │Packet │
 *                                   │    [ 0x68, 0x43 ]   │     │   B   │
 *                                   └─────────────────────┘     │[0x00] │
 *                                                               └───────┘
 * </pre>
 *
 * <h3>Examples of Buffering Scenarios</h3>
 *
 * <p><strong>Case 1: Single Complete Capsule</strong>
 *
 * <ul>
 *   <li>Capsule: {@code [0x68, 0x43, 0x00]} (CLOSE_SESSION, 3 bytes)
 *   <li>Packet 1: {@code [0x68, 0x43, 0x00]} (3 bytes)
 *   <li>Trace:
 *       <ol>
 *         <li>Bytes written to {@code cumulation} (contains 3 bytes).
 *         <li>{@code markReaderIndex()} saves position {@code 0}.
 *         <li>{@code capType} parses to {@code 0x2843} (consumes 2 bytes, ReaderIndex = 2).
 *         <li>{@code capLen} parses to {@code 0} (consumes 1 byte, ReaderIndex = 3).
 *         <li>{@code readableBytes() < capLen} is {@code 0 < 0} (false).
 *         <li>Empty capsule payload read. Capsule processed successfully.
 *         <li>Loop terminates, {@code cumulation} is empty, released, and set to null.
 *       </ol>
 * </ul>
 *
 * <p><strong>Case 2: Fragmented Capsule</strong>
 *
 * <ul>
 *   <li>Capsule: {@code [0x68, 0x43, 0x00]} (CLOSE_SESSION, 3 bytes)
 *   <li>Packet A: {@code [0x68, 0x43]} (2 bytes)
 *   <li>Trace (Packet A):
 *       <pre>
 *     cumulation:
 *     ┌──────────┬──────────┐
 *     │   0x68   │   0x43   │
 *     └──────────┴──────────┘
 *     ▲                     ▲
 *     ReaderIndex           WriterIndex (Length = 2)
 *     </pre>
 *       <ol>
 *         <li>Bytes written to {@code cumulation} (contains 2 bytes).
 *         <li>{@code markReaderIndex()} saves position {@code 0}.
 *         <li>{@code capType} parses to {@code 0x2843} (ReaderIndex = 2).
 *         <li>{@code capLen} read fails (returns -1) due to no bytes left in buffer.
 *         <li>{@code resetReaderIndex()} rolls ReaderIndex back to {@code 0}. Loop exits.
 *       </ol>
 *   <li>Packet B: {@code [0x00]} (1 byte)
 *   <li>Trace (Packet B):
 *       <pre>
 *     cumulation:
 *     ┌──────────┬──────────┬──────────┐
 *     │   0x68   │   0x43   │   0x00   │
 *     └──────────┴──────────┴──────────┘
 *     ▲                                ▲
 *     ReaderIndex                      WriterIndex (Length = 3)
 *     </pre>
 *       <ol>
 *         <li>{@code cumulation} accumulates {@code [0x00]} (contains 3 bytes: {@code [0x68, 0x43,
 *             0x00]}).
 *         <li>{@code markReaderIndex()} saves position {@code 0}.
 *         <li>{@code capType} parses to {@code 0x2843} (ReaderIndex = 2).
 *         <li>{@code capLen} parses to {@code 0} (ReaderIndex = 3).
 *         <li>Capsule processed successfully.
 *         <li>{@code cumulation} released and set to null.
 *       </ol>
 * </ul>
 *
 * <p><strong>Case 3: Multiple Capsules in One Packet</strong>
 *
 * <ul>
 *   <li>Capsules: two {@code [0x68, 0x43, 0x00]} (6 bytes total)
 *   <li>Packet 1: {@code [0x68, 0x43, 0x00, 0x68, 0x43, 0x00]} (6 bytes)
 *   <li>Trace:
 *       <ol>
 *         <li>Bytes written to {@code cumulation} (contains 6 bytes).
 *         <li>Loop iteration 1: parses and executes first capsule (ReaderIndex = 3).
 *         <li>Loop iteration 2: {@code markReaderIndex()} saves position {@code 3}. Parses and
 *             executes second capsule (ReaderIndex = 6).
 *         <li>Loop terminates, buffer released.
 *       </ol>
 * </ul>
 *
 * <p><strong>Case 4: Multiple Capsules + Trailing Partial Capsule</strong>
 *
 * <ul>
 *   <li>Capsules: {@code [0x68, 0x43, 0x00]} (Complete, 3 bytes) + {@code [0x68]} (Partial, 1 byte)
 *   <li>Packet 1: {@code [0x68, 0x43, 0x00, 0x68]} (4 bytes)
 *   <li>Trace (Packet 1):
 *       <pre>
 *     cumulation:
 *     ┌──────────┬──────────┬──────────┬──────────┐
 *     │   0x68   │   0x43   │   0x00   │   0x68   │
 *     └──────────┴──────────┴──────────┴──────────┘
 *                                      ▲          ▲
 *                                      ReaderIndex WriterIndex (Length = 4)
 *                                      (Marked at 3)
 *     </pre>
 *       <ol>
 *         <li>Bytes written to {@code cumulation} (contains 4 bytes).
 *         <li>Loop iteration 1: parses first capsule (ReaderIndex = 3).
 *         <li>Loop iteration 2: {@code markReaderIndex()} saves position {@code 3}.
 *         <li>{@code capType} parsing fails (needs 2 bytes for the varint prefix, but only 1 byte
 *             {@code 0x68} is left). Returns -1.
 *         <li>{@code resetReaderIndex()} rolls ReaderIndex back to {@code 3}. Loop exits.
 *         <li>Buffer retains {@code [0x68]} for the next packet.
 *       </ol>
 * </ul>
 */
public class WebTransportDataHandler extends Http3RequestStreamInboundHandler {
  private static final Logger logger = Logger.getLogger(WebTransportDataHandler.class.getName());
  private ByteBuf cumulation;

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (cumulation != null) {
      cumulation.release();
      cumulation = null;
    }
    super.handlerRemoved(ctx);
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame) {
    ByteBuf payload = frame.content();
    if (payload == null || !payload.isReadable()) {
      ReferenceCountUtil.release(frame);
      return;
    }
    try {
      if (cumulation == null) {
        cumulation = ctx.alloc().buffer();
      }
      cumulation.writeBytes(payload);

      while (cumulation.isReadable()) {
        cumulation.markReaderIndex();
        long capType = readVariableLengthInt(cumulation);
        if (capType == -1) {
          cumulation.resetReaderIndex();
          break;
        }
        long capLen = readVariableLengthInt(cumulation);
        if (capLen == -1 || cumulation.readableBytes() < capLen) {
          cumulation.resetReaderIndex();
          break;
        }

        ByteBuf capVal = cumulation.readSlice((int) capLen);
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  "💊 Received Capsule | Type: 0x%X | Length: %d | Hex: %s",
                  capType, capLen, io.netty.buffer.ByteBufUtil.hexDump(capVal)));
        }

        Long sessId = ctx.channel().attr(WebTransportUtils.SESSION_ID_KEY).get();
        long sessionId =
            (sessId != null)
                ? sessId
                : ((io.netty.handler.codec.quic.QuicStreamChannel) ctx.channel()).streamId();
        WebTransportCapsule capsule = new WebTransportCapsule(sessionId, capType, capVal.retain());
        ctx.fireChannelRead(capsule);

        if (capType == 0x2843) {
          break;
        }
      }

      if (cumulation != null && !cumulation.isReadable()) {
        cumulation.release();
        cumulation = null;
      }
    } catch (Exception e) {
      logger.error("Error parsing capsules on CONNECT stream", e);
      if (cumulation != null) {
        cumulation.release();
        cumulation = null;
      }
      ctx.close();
    } finally {
      ReferenceCountUtil.release(frame);
    }
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame) {
    ctx.fireChannelRead(frame);
  }

  @Override
  protected void channelInputClosed(ChannelHandlerContext ctx) {
    logger.debug("🔒 Stream Closed: " + ctx.channel().id());
    ctx.close();
  }
}
