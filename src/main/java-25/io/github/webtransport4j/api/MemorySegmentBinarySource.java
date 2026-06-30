package io.github.webtransport4j.api;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * A {@link BinarySource} backed by a {@link MemorySegment}.
 *
 * <p>This implementation natively supports segments larger than 2GB by avoiding {@code
 * asByteBuffer()} on the source segment and using {@link MemorySegment#copy} to copy data directly
 * to the destination.
 */
final class MemorySegmentBinarySource implements BinarySource {

  private final MemorySegment segment;
  private final long size;
  private long offset;

  MemorySegmentBinarySource(@NonNull MemorySegment segment) {
    this.segment = Objects.requireNonNull(segment, "segment");
    this.size = segment.byteSize();
    this.offset = 0;
  }

  @Override
  public int read(@NonNull ByteBuffer dst) {
    if (offset >= size) {
      return -1;
    }

    long remaining = size - offset;
    int bytesToRead = (int) Math.min(dst.remaining(), remaining);

    MemorySegment dstSegment = MemorySegment.ofBuffer(dst);
    MemorySegment.copy(segment, offset, dstSegment, dst.position(), bytesToRead);

    dst.position(dst.position() + bytesToRead);
    offset += bytesToRead;

    return bytesToRead;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public void close() {
    // No-op. The MemorySegment lifetime is managed by its Arena.
  }
}
