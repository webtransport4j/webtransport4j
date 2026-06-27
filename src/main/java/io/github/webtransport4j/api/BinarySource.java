package io.github.webtransport4j.api;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author <a href="https://github.com/sanjomo">Santhosh Mohan</a>
 * @date 25/06/26 8:02 pm
 */
public interface BinarySource extends AutoCloseable {

    /**
     * Reads bytes into the destination buffer.
     *
     * <p>The implementation should write into {@code dst} starting at its
     * current position and advance the position by the number of bytes read.
     *
     * @param dst destination buffer
     * @return the number of bytes read, or {@code -1} if the end of the source
     *         has been reached.
     * @throws IOException if an I/O error occurs
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Returns the total size of the source if known.
     *
     * @return total size in bytes, or {@code -1} if unknown.
     */
    default long size() throws IOException {
        return -1;
    }

    default boolean hasKnownSize() throws IOException {
        return size() >= 0;
    }

    @Override
    default void close() throws IOException {
    }
}