package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.jspecify.annotations.NonNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;

/**
 * Factory methods for creating {@link BinarySource} instances.
 * <p>
 * This utility class provides convenient wrappers for various I/O and memory
 * abstractions to be used seamlessly over WebTransport streams.
 */
public final class BinarySources {

    private BinarySources() {
    }

    /**
     * Creates a {@link BinarySource} backed by the given byte array.
     *
     * @param data the byte array to read from
     * @return a new binary source wrapping the entire array
     */
    public static @NonNull BinarySource fromByteArray(byte @NonNull [] data) {
        return new ByteArrayBinarySource(data);
    }

    /**
     * Creates a {@link BinarySource} backed by a specific region of a byte array.
     *
     * @param data   the byte array to read from
     * @param offset the starting offset within the array
     * @param length the number of bytes to read
     * @return a new binary source wrapping the array region
     * @throws IndexOutOfBoundsException if offset or length are out of bounds
     */
    public static @NonNull BinarySource fromByteArray(byte @NonNull [] data, int offset, int length) {
        return new ByteArrayBinarySource(data, offset, length);
    }

    /**
     * Creates a {@link BinarySource} backed by a {@link ByteBuffer}.
     * <p>
     * Note: Reading from this source will advance the position of the underlying
     * {@code ByteBuffer}.
     *
     * @param buffer the buffer to read from
     * @return a new binary source wrapping the buffer
     */
    public static @NonNull BinarySource fromByteBuffer(@NonNull ByteBuffer buffer) {
        return new ByteBufferBinarySource(buffer);
    }

    /**
     * Creates a {@link BinarySource} backed by a Netty {@link ByteBuf}.
     * <p>
     * Note: Reading from this source will advance the reader index of the
     * underlying {@code ByteBuf}.
     *
     * @param buffer the buffer to read from
     * @return a new binary source wrapping the buffer
     */
    public static @NonNull BinarySource fromByteBuf(@NonNull ByteBuf buffer) {
        return new ByteBufBinarySource(buffer);
    }

    /**
     * Creates a {@link BinarySource} backed by a file at the specified {@link Path}.
     *
     * @param path the path to the file
     * @return a new binary source wrapping the file
     * @throws IOException if an I/O error occurs opening the file
     */
    public static @NonNull BinarySource fromPath(@NonNull Path path) throws IOException {
        return new PathBinarySource(path);
    }

    /**
     * Creates a {@link BinarySource} backed by the specified {@link File}.
     *
     * @param file the file to read from
     * @return a new binary source wrapping the file
     * @throws IOException if an I/O error occurs opening the file
     */
    public static @NonNull BinarySource fromFile(@NonNull File file) throws IOException {
        return new PathBinarySource(file.toPath());
    }

    /**
     * Creates a {@link BinarySource} backed by an {@link InputStream}.
     * <p>
     * Note: The underlying input stream will be closed when the source is closed.
     *
     * @param in the input stream to read from
     * @return a new binary source wrapping the stream
     * @throws IOException if an I/O error occurs
     */
    public static @NonNull BinarySource fromInputStream(@NonNull InputStream in) throws IOException {
        return new InputStreamBinarySource(in);
    }

    /**
     * Creates a {@link BinarySource} backed by a {@link ReadableByteChannel}.
     * <p>
     * Note: The underlying channel will be closed when the source is closed.
     *
     * @param channel the channel to read from
     * @return a new binary source wrapping the channel
     * @throws IOException if an I/O error occurs
     */
    public static @NonNull BinarySource fromReadableByteChannel(@NonNull ReadableByteChannel channel) throws IOException {
        return new ChannelBinarySource(channel);
    }
}
