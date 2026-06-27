package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;

/**
 * Factory methods for creating {@link BinarySource} instances.
 */
public final class BinarySources {

    private BinarySources() {
    }
    public static @NotNull BinarySource from(@NotNull byte[] data) {
        return new ByteArrayBinarySource(data);
    }

    public static @NotNull BinarySource from(@NotNull byte[] data, int offset, int length) {
        return new ByteArrayBinarySource(data, offset, length);
    }

    public static @NotNull BinarySource from(@NotNull ByteBuffer buffer) {
        return new ByteBufferBinarySource(buffer);
    }

    public static @NotNull BinarySource from(@NotNull ByteBuf buffer) {
        return new ByteBufBinarySource(buffer);
    }
    /**
     * Java 25 Foreign Memory API.
     */
    public static @NotNull BinarySource from(@NotNull MemorySegment segment) {
        return new MemorySegmentBinarySource(segment);
    }

    public static @NotNull BinarySource from(@NotNull Path path) throws IOException {
        return new PathBinarySource(path);
    }

    public static @NotNull BinarySource from(@NotNull File file) throws IOException {
        return new PathBinarySource(file.toPath());
    }

    public static @NotNull BinarySource from(@NotNull InputStream in) throws IOException {
        return new InputStreamBinarySource(in);
    }

    public static @NotNull BinarySource from(@NotNull ReadableByteChannel channel) throws IOException {
        return new ChannelBinarySource(channel);
    }
}