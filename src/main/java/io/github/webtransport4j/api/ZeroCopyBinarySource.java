package io.github.webtransport4j.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.jspecify.annotations.Nullable;

/** Binary source supporting zero-copy chunk reads as NIO ByteBuffers. */
public interface ZeroCopyBinarySource extends BinarySource {

  @Nullable ByteBuffer readZeroCopyChunk(int maxBytes) throws IOException;
}
