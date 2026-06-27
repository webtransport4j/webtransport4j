package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link BinarySource} backed by a Netty {@link ByteBuf}.
 * <p>
 * Reading from this source will advance the reader index of the underlying
 * buffer. The source considers the initial {@link ByteBuf#readableBytes()}
 * as its total size.
 */
final class ByteBufBinarySource implements BinarySource {

    private final ByteBuf buffer;
    private final int size;

    ByteBufBinarySource(@NotNull ByteBuf buffer) {
        this.buffer = buffer;
        this.size = buffer.readableBytes();
    }

    @Override
    public int read(@NotNull ByteBuffer dst) throws IOException {
        if (!buffer.isReadable()) {
            return -1;
        }

        int bytes = Math.min(dst.remaining(), buffer.readableBytes());
        
        int oldLimit = dst.limit();
        dst.limit(dst.position() + bytes);
        buffer.readBytes(dst);
        dst.limit(oldLimit);
        
        return bytes;
    }

    @Override
    public long size() {
        return size;
    }
}