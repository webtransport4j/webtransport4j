package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public final class LengthPrefixedCodec
    implements StreamCodec<ByteBuf> {

    private static final int MAX_FRAME_SIZE =
        16 * 1024 * 1024;

    private final ByteBufAllocator alloc;
    private final ByteBuf accumulator;

    public LengthPrefixedCodec(ByteBufAllocator alloc) {
        this.alloc = alloc;
        this.accumulator = alloc.buffer();
    }

    @Override
    public ByteBuf encode(ByteBuf message) {

        ByteBuf out =
            alloc.buffer(4 + message.readableBytes());

        out.writeInt(message.readableBytes());
        out.writeBytes(
            message,
            message.readerIndex(),
            message.readableBytes());

        return out;
    }

    @Override
    public void decode(
            ByteBuf incoming,
            Consumer<ByteBuf> consumer) {

        accumulator.writeBytes(incoming);

        while (true) {

            if (accumulator.readableBytes() < 4) {
                break;
            }

            accumulator.markReaderIndex();

            int length = accumulator.readInt();

            if (length < 0 ||
                length > MAX_FRAME_SIZE) {
                throw new IllegalArgumentException(
                    "Invalid frame size: " + length);
            }

            if (accumulator.readableBytes() < length) {
                accumulator.resetReaderIndex();
                break;
            }

            consumer.accept(
                accumulator.readRetainedSlice(length));
        }

        if (!accumulator.isReadable()) {
            accumulator.clear();
        } else if (accumulator.readerIndex() > 4096) {
            accumulator.discardReadBytes();
        }
    }

    @Override
    public void close() {
        accumulator.release();
    }
}