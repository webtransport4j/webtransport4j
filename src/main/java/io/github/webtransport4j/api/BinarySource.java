package io.github.webtransport4j.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.jspecify.annotations.NonNull;

/**
 * An interface representing a source of binary data that can be read in chunks.
 * Implementations of this interface should provide a way to read bytes from the source
 * and optionally provide the total size of the source if known.
 */
public interface BinarySource extends AutoCloseable {

  /**
   * Reads bytes into the destination buffer.
   *
   * <p>The implementation should write into {@code dst} starting at its
   * current position and advance the position by the number of bytes read.
   *
   * @param dst destination buffer
   * @return the number of bytes read, or {@code -1} if the end of the source
   *         has been reached.
   * @throws IOException if an I/O error occurs
   */
  int read(@NonNull ByteBuffer dst) throws IOException;

  /**
   * Returns the total size of the source if known.
   *
   * @return total size in bytes, or {@code -1} if unknown.
   */
  default long size() throws IOException {
    return -1;
  }

  default boolean hasKnownSize() throws IOException {
    return size() >= 0;
  }

  @Override
  default void close() throws IOException {
  }
}
