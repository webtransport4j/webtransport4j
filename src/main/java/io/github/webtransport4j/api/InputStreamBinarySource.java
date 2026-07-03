package io.github.webtransport4j.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * A {@link BinarySource} backed by an {@link InputStream}.
 *
 * <p>This source optimizes reads when the destination {@link ByteBuffer} is array-backed. Calling
 * {@link #close()} will close the underlying {@code InputStream}.
 */
final class InputStreamBinarySource implements BinarySource {

  private final InputStream in;

  private byte[] scratch;

  InputStreamBinarySource(@NonNull InputStream in) {
    this.in = Objects.requireNonNull(in, "in must not be null");
  }

  @Override
  public int read(@NonNull ByteBuffer dst) throws IOException {
    if (!dst.hasRemaining()) {
      return 0;
    }

    int bytesRead;

    if (dst.hasArray()) {
      int offset = dst.arrayOffset() + dst.position();
      bytesRead = in.read(dst.array(), offset, dst.remaining());

      if (bytesRead > 0) {
        dst.position(dst.position() + bytesRead);
      }
    } else {
      int scratchSize = Math.min(dst.remaining(), 8192);
      if (scratch == null || scratch.length < scratchSize) {
        scratch = new byte[scratchSize];
      }
      bytesRead = in.read(scratch, 0, scratchSize);

      if (bytesRead > 0) {
        dst.put(scratch, 0, bytesRead);
      }
    }

    return bytesRead;
  }

  @Override
  public long size() {
    return -1;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
