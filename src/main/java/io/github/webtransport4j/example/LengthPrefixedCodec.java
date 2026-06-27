package io.github.webtransport4j.example;

import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.function.Consumer;

/**
 * A simple length-prefixed framing codec using QUIC variable-length integer encoding.
 * <p>
 * This encoding saves bandwidth by compressing the frame length prefix into 1, 2, 4, 
 * or 8 bytes depending on the payload size. The first two bits of the encoded integer 
 * determine its total byte length:
 * <ul>
 *   <li><b>00</b>: 1 byte  (Payloads up to 63 bytes)</li>
 *   <li><b>01</b>: 2 bytes (Payloads up to 16,383 bytes)</li>
 *   <li><b>10</b>: 4 bytes (Payloads up to 1,073,741,823 bytes)</li>
 *   <li><b>11</b>: 8 bytes (Payloads up to 4,611,686,018,427,387,903 bytes)</li>
 * </ul>
 *
 * Frame format:
 * +--------------+-------------------+
 * | QUIC Var Int |     Payload       |
 * +--------------+-------------------+
 * | Length (N)   | N bytes           |
 * +--------------+-------------------+
 */
public final class LengthPrefixedCodec
        implements StreamCodec<ByteBuf> {

    /**
     * Default maximum frame size (16 MiB).
     */
    public static final int DEFAULT_MAX_FRAME_SIZE =
            16 * 1024 * 1024;

    /**
     * Maximum allowed payload size.
     */
    private final int maxFrameSize;

    /**
     * Accumulates fragmented incoming bytes until complete
     * frames are available.
     */
    private final ByteBuf accumulator;

    /**
     * Creates a codec using the default maximum frame size.
     */
    public LengthPrefixedCodec() {
        this(DEFAULT_MAX_FRAME_SIZE);
    }

    /**
     * Creates a codec with a custom maximum frame size.
     *
     * @param maxFrameSize maximum payload size in bytes
     */
    public LengthPrefixedCodec(int maxFrameSize) {

        if (maxFrameSize <= 0) {
            throw new IllegalArgumentException(
                    "maxFrameSize must be greater than zero");
        }

        this.maxFrameSize = maxFrameSize;
        this.accumulator = Unpooled.buffer();
    }

    /**
     * Returns the configured maximum frame size.
     */
    public int maxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public ByteBuf encode(ByteBuf message) {

        int length = message.readableBytes();

        if (length > maxFrameSize) {
            throw new IllegalArgumentException(
                    "Frame size " + length
                            + " exceeds configured maximum of "
                            + maxFrameSize + " bytes");
        }

        ByteBuf out = Unpooled.buffer(8 + length, 8 + length);

        WebTransportUtils.writeVarInt(out, length);

        out.writeBytes(
                message,
                message.readerIndex(),
                length);

        return out;
    }

    @Override
    public void decode(
            ByteBuf incoming,
            Consumer<ByteBuf> consumer) {

        accumulator.writeBytes(incoming);

        while (true) {

            if (!accumulator.isReadable()) {
                return;
            }

            accumulator.markReaderIndex();

            long lengthLong = WebTransportUtils.readVariableLengthInt(accumulator);
            
            if (lengthLong == -1) {
                accumulator.resetReaderIndex();
                return;
            }

            if (lengthLong < 0 || lengthLong > maxFrameSize) {
                throw new IllegalArgumentException(
                        "Invalid frame size: "
                                + lengthLong
                                + " (maximum "
                                + maxFrameSize
                                + " bytes)");
            }
            
            int length = (int) lengthLong;

            if (accumulator.readableBytes() < length) {
                accumulator.resetReaderIndex();
                return;
            }

            consumer.accept(
                    accumulator.readRetainedSlice(length));
        }
    }

    @Override
    public void close() {

        if (accumulator.refCnt() > 0) {
            accumulator.release();
        }
    }
}