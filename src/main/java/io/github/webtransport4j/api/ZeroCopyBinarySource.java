package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.jspecify.annotations.Nullable;

/**
 * Binary source supporting zero-copy retained chunk reads.
 */
public interface ZeroCopyBinarySource extends BinarySource {

  @Nullable ByteBuf readRetainedChunk(int maxBytes) throws IOException;
}