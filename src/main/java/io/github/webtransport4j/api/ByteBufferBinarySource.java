package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;

import java.nio.ByteBuffer;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link BinarySource} backed by a {@link ByteBuffer}.
 * <p>
 * Reading from this source will advance the position of the underlying
 * buffer. The source considers the initial {@link ByteBuffer#remaining()}
 * as its size.
 */
final class ByteBufferBinarySource implements BinarySource {

    private final ByteBuffer buffer;
    private final int size;

    ByteBufferBinarySource(@NonNull ByteBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.size = buffer.remaining();
    }

    @Override
    public int read(@NonNull ByteBuffer dst) throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        }

        int bytes = Math.min(dst.remaining(), buffer.remaining());

        ByteBuffer slice = buffer.slice();
        slice.limit(bytes);

        dst.put(slice);
        buffer.position(buffer.position() + bytes);

        return bytes;
    }

    @Override
    public long size() {
        return size;
    }
}