package io.github.webtransport4j.example;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class LengthPrefixedCodec
    implements StreamCodec<ByteBuf> {

    private final ByteBuf accumulator;

    public LengthPrefixedCodec(ByteBufAllocator alloc) {
        this.accumulator = alloc.buffer();
    }

    @Override
    public ByteBuf encode(
            ByteBufAllocator alloc,
            ByteBuf message) {

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
                return;
            }

            accumulator.markReaderIndex();

            int length = accumulator.readInt();

            if (accumulator.readableBytes() < length) {
                accumulator.resetReaderIndex();
                return;
            }

            ByteBuf frame =
                accumulator.readRetainedSlice(length);

            consumer.accept(frame);
        }
    }
}