package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import org.jspecify.annotations.NonNull;

/** Factory methods for creating {@link BinarySource} instances. */
public final class BinarySources {

  private BinarySources() {}

  public static @NonNull BinarySource fromByteArray(byte @NonNull [] data) {
    return new ByteArrayBinarySource(data);
  }

  public static @NonNull BinarySource fromByteArray(byte @NonNull [] data, int offset, int length) {
    return new ByteArrayBinarySource(data, offset, length);
  }

  public static @NonNull BinarySource fromByteBuffer(@NonNull ByteBuffer buffer) {
    return new ByteBufferBinarySource(buffer);
  }

  public static @NonNull BinarySource fromByteBuf(@NonNull ByteBuf buffer) {
    return new ByteBufBinarySource(buffer);
  }

  /** Java 25 Foreign Memory API. */
  public static @NonNull BinarySource fromMemorySegment(@NonNull MemorySegment segment) {
    return new MemorySegmentBinarySource(segment);
  }

  public static @NonNull BinarySource fromPath(@NonNull Path path) throws IOException {
    return new PathBinarySource(path);
  }

  public static @NonNull BinarySource fromFile(@NonNull File file) throws IOException {
    return new PathBinarySource(file.toPath());
  }

  public static @NonNull BinarySource fromInputStream(@NonNull InputStream in) throws IOException {
    return new InputStreamBinarySource(in);
  }

  public static @NonNull BinarySource fromReadableByteChannel(@NonNull ReadableByteChannel channel)
      throws IOException {
    return new ChannelBinarySource(channel);
  }
}
