package io.github.webtransport4j.server;

import io.github.webtransport4j.api.BinarySource;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.api.ZeroCopyBinarySource;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A chunked input that reads from a binary source. */
public final class BinarySourceChunkedInput implements ChunkedInput<ByteBuf> {
  private static final Logger logger = LoggerFactory.getLogger(BinarySourceChunkedInput.class);
  private static final int DEFAULT_CHUNK_SIZE =
      Integer.getInteger("webtransport4j.chunked.input.size", 16 * 1024);

  private final BinarySource source;
  private final int chunkSize;
  private final @Nullable Supplier<Boolean> writabilityChecker;

  private boolean eof;
  private long progress;

  /**
   * Creates a new instance with the default chunk size.
   *
   * @param source the binary source to read from
   */
  public BinarySourceChunkedInput(@NonNull BinarySource source) {
    this(Objects.requireNonNull(source, "source must not be null"), DEFAULT_CHUNK_SIZE, (Supplier<Boolean>) null);
  }

  /**
   * Creates a new instance with the specified chunk size.
   *
   * @param source the binary source to read from
   * @param chunkSize the size of each chunk to read
   */
  public BinarySourceChunkedInput(@NonNull BinarySource source, int chunkSize) {
    this(source, chunkSize, (Supplier<Boolean>) null);
  }

  /**
   * Creates a new instance bounded by the writability of the specified WebTransport stream.
   *
   * @param source the binary source to read from
   * @param stream the stream whose flow-control writability gates chunk extraction
   */
  public BinarySourceChunkedInput(
      @NonNull BinarySource source, @Nullable WebTransportStream stream) {
    this(source, DEFAULT_CHUNK_SIZE, stream != null ? stream::isWritable : null);
  }

  /**
   * Creates a new instance with the specified chunk size bounded by stream writability.
   *
   * @param source the binary source to read from
   * @param chunkSize the size of each chunk to read
   * @param stream the stream whose flow-control writability gates chunk extraction
   */
  public BinarySourceChunkedInput(
      @NonNull BinarySource source, int chunkSize, @Nullable WebTransportStream stream) {
    this(source, chunkSize, stream != null ? stream::isWritable : null);
  }

  /**
   * Creates a new instance with the specified chunk size and writability checker.
   *
   * @param source the binary source to read from
   * @param chunkSize the size of each chunk to read
   * @param writabilityChecker supplier returning true if chunk reading is currently permitted
   */
  public BinarySourceChunkedInput(
      @NonNull BinarySource source, int chunkSize, @Nullable Supplier<Boolean> writabilityChecker) {
    this.source = Objects.requireNonNull(source, "source must not be null");
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunk size must be greater than 0");
    }
    this.chunkSize = chunkSize;
    this.writabilityChecker = writabilityChecker;
  }

  @Override
  public boolean isEndOfInput() {
    return eof;
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  @Override
  public @Nullable ByteBuf readChunk(@NonNull ChannelHandlerContext ctx) throws Exception {
    WebTransportStream stream = ctx.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
    if (stream != null && !stream.isWritable()) {
      return null;
    }
    return readChunk(ctx.alloc());
  }

  @Override
  public @Nullable ByteBuf readChunk(@NonNull ByteBufAllocator alloc) throws Exception {
    if (eof) {
      return null;
    }
    if (writabilityChecker != null && !Boolean.TRUE.equals(writabilityChecker.get())) {
      return null;
    }

    int currentChunkSize = chunkSize;
    if (source.hasKnownSize()) {
      long remaining = source.size() - progress;
      if (remaining <= 0) {
        eof = true;
        close();
        return null;
      } else if (remaining < currentChunkSize) {
        currentChunkSize = (int) remaining;
      }
    }

    if (source instanceof ByteBufBinarySource) {
      return readByteBufChunk((ByteBufBinarySource) source, currentChunkSize);
    }

    if (source instanceof ZeroCopyBinarySource) {
      return readZeroCopyChunk((ZeroCopyBinarySource) source, currentChunkSize);
    }

    ByteBuf buf = alloc.buffer(currentChunkSize);
    int totalRead = 0;

    try {
      ByteBuffer nio = buf.nioBuffer(0, currentChunkSize);

      while (totalRead < currentChunkSize) {
        int read = source.read(nio);

        if (read < 0) {
          eof = true;
          break;
        }

        if (read == 0) {
          break;
        }

        totalRead += read;
      }

      if (totalRead == 0) {
        buf.release();
        if (eof) {
          close();
        }
        return null;
      }

      buf.writerIndex(totalRead);
      progress += totalRead;

      if (eof) {
        close();
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Read chunk of {} bytes. Total progress: {} bytes", totalRead, progress);
      }
      return buf;
    } catch (Throwable t) {
      buf.release();
      close();
      throw t;
    }
  }

  private @Nullable ByteBuf readZeroCopyChunk(
      @NonNull ZeroCopyBinarySource zeroCopySource, int currentChunkSize) throws IOException {
    ByteBuffer nioChunk = zeroCopySource.readZeroCopyChunk(currentChunkSize);
    if (nioChunk == null) {
      eof = true;
      close();
      return null;
    }
    int totalRead = nioChunk.remaining();
    if (totalRead == 0) {
      return null;
    }
    ByteBuf chunk = Unpooled.wrappedBuffer(nioChunk);
    try {
      progress += totalRead;
      if (source.hasKnownSize() && progress >= source.size()) {
        eof = true;
        close();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Read zero-copy chunk of {} bytes. Total progress: {} bytes", totalRead, progress);
      }
      return chunk;
    } catch (Throwable t) {
      chunk.release();
      close();
      throw t;
    }
  }

  private @Nullable ByteBuf readByteBufChunk(
      @NonNull ByteBufBinarySource byteBufSource, int currentChunkSize) throws IOException {
    ByteBuf chunk = byteBufSource.readRetainedChunk(currentChunkSize);
    if (chunk == null) {
      eof = true;
      close();
      return null;
    }
    int totalRead = chunk.readableBytes();
    if (totalRead == 0) {
      chunk.release();
      return null;
    }
    try {
      progress += totalRead;
      if (source.hasKnownSize() && progress >= source.size()) {
        eof = true;
        close();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Read zero-copy chunk of {} bytes. Total progress: {} bytes", totalRead, progress);
      }
      return chunk;
    } catch (Throwable t) {
      chunk.release();
      close();
      throw t;
    }
  }

  @Override
  public long length() {
    try {
      return source.hasKnownSize() ? source.size() : -1;
    } catch (IOException e) {
      logger.error("Error occurred while fetching source size", e);
      return -1;
    }
  }

  @Override
  public long progress() {
    return progress;
  }
}
