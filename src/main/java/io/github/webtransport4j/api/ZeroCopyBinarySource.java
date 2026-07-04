package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.jspecify.annotations.Nullable;

/** Internal extension for binary sources that can expose owned Netty chunks without copying. */
interface ZeroCopyBinarySource extends BinarySource {

  @Nullable ByteBuf readRetainedChunk(int maxBytes) throws IOException;
}