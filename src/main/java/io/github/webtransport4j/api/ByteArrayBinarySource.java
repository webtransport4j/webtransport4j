package io.github.webtransport4j.api;

/**
 * @author https://github.com/sanjomo
 * @date 27/06/26 2:18 pm
 */
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * A {@link BinarySource} backed by a byte array.
 * <p>
 * This implementation does not create a defensive copy of the array.
 * Modifications to the array will be reflected in the source.
 */
final class ByteArrayBinarySource implements BinarySource {

    private final byte[] data;
    private final int end;
    private final int size;
    private int position;

    ByteArrayBinarySource(byte @NotNull [] data) {
        this(data, 0, data.length);
    }

    ByteArrayBinarySource(byte @NotNull [] data, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException();
        }

        this.data = data;
        this.position = offset;
        this.end = offset + length;
        this.size = length;
    }

    @Override
    public int read(@NotNull ByteBuffer dst) {
        if (position >= end) {
            return -1;
        }

        int bytes = Math.min(dst.remaining(), end - position);
        dst.put(data, position, bytes);
        position += bytes;
        return bytes;
    }

    @Override
    public long size() {
        return size;
    }
}