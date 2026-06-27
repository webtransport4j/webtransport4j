package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class BinarySourceChunkedInput
        implements ChunkedInput<ByteBuf> {
    private static final Logger logger = LoggerFactory.getLogger(BinarySourceChunkedInput.class);
    private static final int DEFAULT_CHUNK_SIZE = Integer.getInteger("webtransport4j.chunked.input.size", 16 * 1024);

    private final BinarySource source;
    private final int chunkSize;

    private boolean eof;
    private long progress;

    public BinarySourceChunkedInput(@NonNull BinarySource source) {
        this(Objects.requireNonNull(source,"source must not be null"), DEFAULT_CHUNK_SIZE);
    }

    public BinarySourceChunkedInput(BinarySource source, int chunkSize) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        if(chunkSize <= 0) {
            throw new IllegalArgumentException("chunk size must be greater than 0");
        }
        this.chunkSize = chunkSize;
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
        return readChunk(ctx.alloc());
    }

    @Override
    public @Nullable ByteBuf readChunk(@NonNull ByteBufAllocator alloc) throws Exception {
        if (eof) {
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

    @Override
    public long length() {
        try {
            return source.hasKnownSize() ? source.size() : -1;
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public long progress() {
        return progress;
    }
}