package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.jspecify.annotations.Nullable;

public interface ZeroCopyBinarySource extends BinarySource {

  @Nullable ByteBuf readRetainedChunk(int maxBytes) throws IOException;
}