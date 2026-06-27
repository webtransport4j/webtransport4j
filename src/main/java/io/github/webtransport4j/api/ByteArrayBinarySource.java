package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;

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

    ByteArrayBinarySource(byte @NonNull [] data) {
        this(data, 0, data.length);
    }

    ByteArrayBinarySource(byte @NonNull [] data, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException();
        }

        this.data = data;
        this.position = offset;
        this.end = offset + length;
        this.size = length;
    }

    @Override
    public int read(@NonNull ByteBuffer dst) {
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