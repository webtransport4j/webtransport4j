package io.github.webtransport4j.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.function.Consumer;

/**
 * A simple length-prefixed framing codec.
 *
 * Frame format:
 * +------------+-------------------+
 * | 4-byte int |     Payload       |
 * +------------+-------------------+
 * | Length (N) | N bytes           |
 * +------------+-------------------+
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

        ByteBuf out = Unpooled.buffer(
                Integer.BYTES + length,
                Integer.BYTES + length);

        out.writeInt(length);

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

            if (accumulator.readableBytes() < Integer.BYTES) {
                return;
            }

            accumulator.markReaderIndex();

            int length = accumulator.readInt();

            if (length < 0 || length > maxFrameSize) {
                throw new IllegalArgumentException(
                        "Invalid frame size: "
                                + length
                                + " (maximum "
                                + maxFrameSize
                                + " bytes)");
            }

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