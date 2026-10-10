package io.github.webtransport4j.api;

import java.nio.ByteBuffer;
import org.jspecify.annotations.NonNull;

/**
 * A neutral buffer abstraction for WebTransport payloads. This hides the underlying transport
 * buffer implementation (e.g., Netty's ByteBuf) while preserving zero-copy extraction and
 * reference-counting for async processing.
 */
public interface WebTransportBuffer extends AutoCloseable {

  /**
   * Returns the number of readable bytes in this buffer.
   *
   * @return the number of readable bytes.
   */
  int readableBytes();

  /**
   * Exposes this buffer's readable bytes as a zero-copy NIO {@link ByteBuffer}. The returned buffer
   * shares the same memory as this buffer.
   *
   * @return a zero-copy ByteBuffer view.
   */
  ByteBuffer nioBuffer();

  ByteBuffer skipBytes(int bytes);

  /**
   * Copies the readable bytes from this buffer into a newly allocated byte array.
   *
   * @return a byte array containing a copy of the data.
   */
  byte[] readBytes();

  /**
   * Retains this buffer, increasing its reference count. You MUST call this if you intend to
   * process the buffer asynchronously (e.g., passing it to another thread).
   *
   * @return this buffer.
   */
  WebTransportBuffer retain();

  /**
   * Gets a byte at the specified relative index (0 to readableBytes() - 1) without modifying the
   * reader index.
   *
   * @param index relative index from reader index
   * @return byte value
   */
  default byte getByte(int index) {
    return nioBuffer().get(index);
  }

  /**
   * Reads a byte from this buffer, incrementing the reader index.
   *
   * @return byte value
   */
  default byte readByte() {
    return nioBuffer().get();
  }

  /**
   * Gets a 32-bit integer at the specified relative index without modifying the reader index.
   *
   * @param index relative index from reader index
   * @return 32-bit int
   */
  default int getInt(int index) {
    return nioBuffer().getInt(index);
  }

  /**
   * Gets a 64-bit long at the specified relative index without modifying the reader index.
   *
   * @param index relative index from reader index
   * @return 64-bit long
   */
  default long getLong(int index) {
    return nioBuffer().getLong(index);
  }

  /**
   * Compares the readable contents of this buffer against the given byte array without allocating any
   * intermediate objects or byte arrays on the heap.
   *
   * @param expected expected byte array to compare against
   * @return true if the buffer has the same readable length and identical bytes, false otherwise
   */
  default boolean equalsBytes(@NonNull byte[] expected) {
    if (readableBytes() != expected.length) {
      return false;
    }
    for (int i = 0; i < expected.length; i++) {
      if (getByte(i) != expected[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether this buffer starts with the specified prefix bytes without allocating any
   * intermediate objects on the heap.
   *
   * @param prefix prefix byte array to check
   * @return true if this buffer starts with prefix, false otherwise
   */
  default boolean startsWith(@NonNull byte[] prefix) {
    if (readableBytes() < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (getByte(i) != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  /** Releases this buffer, decreasing its reference count. */
  void release();

  /** Closes the buffer (equivalent to release()). */
  @Override
  default void close() {
    release();
  }
}
