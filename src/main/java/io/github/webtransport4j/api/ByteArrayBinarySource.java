package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A {@link BinarySource} backed by a byte array.
 *
 * <p>This implementation does not create a defensive copy of the array. Modifications to the array
 * will be reflected in the source.
 */
final class ByteArrayBinarySource implements ZeroCopyBinarySource {

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
  public @Nullable ByteBuf readRetainedChunk(int maxBytes) {
    if (position >= end) {
      return null;
    }
    int bytes = Math.min(maxBytes, end - position);
    ByteBuf chunk = Unpooled.wrappedBuffer(data, position, bytes);
    position += bytes;
    return chunk;
  }

  @Override
  public long size() {
    return size;
  }
}
