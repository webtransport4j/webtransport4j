package io.github.webtransport4j.server;

import io.github.webtransport4j.api.ZeroCopyBinarySource;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A {@link io.github.webtransport4j.api.BinarySource} backed by a Netty {@link ByteBuf}.
 *
 * <p>Reading from this source will advance the reader index of the underlying buffer. The source
 * considers the initial {@link ByteBuf#readableBytes()} as its total size.
 */
public final class ByteBufBinarySource implements ZeroCopyBinarySource {

  private final ByteBuf buffer;
  private final int size;

  /**
   * Constructs a new {@link ByteBufBinarySource} wrapping the given buffer.
   *
   * @param buffer the underlying buffer
   */
  public ByteBufBinarySource(@NonNull ByteBuf buffer) {
    this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
    this.size = buffer.readableBytes();
  }

  @Override
  public int read(@NonNull ByteBuffer dst) throws IOException {
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
  public @Nullable ByteBuffer readZeroCopyChunk(int maxBytes) {
    if (!buffer.isReadable()) {
      return null;
    }
    int bytes = Math.min(maxBytes, buffer.readableBytes());
    ByteBuffer nio = buffer.nioBuffer(buffer.readerIndex(), bytes);
    buffer.skipBytes(bytes);
    return nio;
  }

  /**
   * Reads a retained slice from the underlying buffer.
   *
   * @param maxBytes the maximum number of bytes to read
   * @return a retained slice of the readable bytes, or null if not readable
   */
  public @Nullable ByteBuf readRetainedChunk(int maxBytes) {
    if (!buffer.isReadable()) {
      return null;
    }
    return buffer.readRetainedSlice(Math.min(maxBytes, buffer.readableBytes()));
  }

  @Override
  public long size() {
    return size;
  }
}
