package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * <h1>Engine.IO / Socket.IO WebTransport Message Frame Decoder</h1>
 *
 * <h2>First Principles: Stream-oriented vs. Message-oriented Transports</h2>
 * <p>
 * Individual QUIC streams (both bidirectional and unidirectional) are raw byte-stream abstractions.
 * Like TCP connections, they guarantee in-order delivery of bytes, but they <b>do not preserve application-layer
 * message boundaries</b>. If a client writes two separate messages to a stream, the network may fragment,
 * coalesce, or deliver them to the server as a single contiguous block of bytes.
 * </p>
 * <p>
 * To reconstruct distinct application messages (e.g. Engine.IO packet types and JSON payloads) from this continuous
 * stream, we require a <i>Framing Layer</i>.
 * </p>
 *
 * <h2>Engine.IO WebTransport Framing Protocol Specification</h2>
 * <p>
 * This decoder implements the WebSocket-like framing format adopted by the Engine.IO WebTransport specification
 * (mirroring the length-prefix rules of RFC 6455):
 * </p>
 * <ul>
 *   <li>
 *     <b>Byte 0 (Header indicator)</b>:
 *     <ul>
 *       <li>The Most Significant Bit (MSB, mask {@code 0x80}) indicates binary vs. text encoding.</li>
 *       <li>The lower 7 bits (mask {@code 0x7F}) represent the length indicator {@code lenType}.</li>
 *     </ul>
 *   </li>
 *   <li>
 *     <b>Variable-length payload decoding</b>:
 *     <ul>
 *       <li>If {@code lenType < 126}: The payload length is exactly {@code lenType} (1-byte header total).</li>
 *       <li>If {@code lenType == 126}: The next 2 bytes (16-bit unsigned integer) define the payload length (3-byte header total).</li>
 *       <li>If {@code lenType == 127}: The next 8 bytes (64-bit integer) define the payload length (9-byte header total).</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h2>Netty Pipeline Processing</h2>
 * <p>
 * Extends {@link ByteToMessageDecoder}. It maintains an internal accumulation buffer. As bytes arrive,
 * it attempts to parse the header, checks if the complete payload has been received, and if so, slices
 * and passes the clean payload (without headers) down the pipeline to {@link MessageDispatcher}.
 * If the payload is incomplete, it rewinds the reader index and yields back to wait for more packets.
 * </p>
 */
public class EngineIoFrameDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 1) {
            return;
        }

        in.markReaderIndex();
        int firstByte = in.readUnsignedByte();
        int lenType = firstByte & 0x7F;

        int payloadLen;
        if (lenType < 126) {
            payloadLen = lenType;
        } else if (lenType == 126) {
            if (in.readableBytes() < 2) {
                in.resetReaderIndex();
                return;
            }
            payloadLen = in.readUnsignedShort();
        } else { // lenType == 127
            if (in.readableBytes() < 8) {
                in.resetReaderIndex();
                return;
            }
            payloadLen = (int) in.readLong();
        }

        if (in.readableBytes() < payloadLen) {
            in.resetReaderIndex();
            return;
        }

        ByteBuf payload = in.readRetainedSlice(payloadLen);
        out.add(payload);
    }
}
