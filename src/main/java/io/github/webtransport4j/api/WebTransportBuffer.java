package io.github.webtransport4j.api;

import java.nio.ByteBuffer;

/**
 * A neutral buffer abstraction for WebTransport payloads.
 * This hides the underlying transport buffer implementation (e.g., Netty's ByteBuf)
 * while preserving zero-copy extraction and reference-counting for async processing.
 */
public interface WebTransportBuffer extends AutoCloseable {

    /**
     * Returns the number of readable bytes in this buffer.
     *
     * @return the number of readable bytes.
     */
    int readableBytes();

    /**
     * Exposes this buffer's readable bytes as a zero-copy NIO {@link ByteBuffer}.
     * The returned buffer shares the same memory as this buffer.
     *
     * @return a zero-copy ByteBuffer view.
     */
    ByteBuffer nioBuffer();

    /**
     * Copies the readable bytes from this buffer into a newly allocated byte array.
     *
     * @return a byte array containing a copy of the data.
     */
    byte[] readBytes();

    /**
     * Retains this buffer, increasing its reference count.
     * You MUST call this if you intend to process the buffer asynchronously
     * (e.g., passing it to another thread).
     *
     * @return this buffer.
     */
    WebTransportBuffer retain();

    /**
     * Releases this buffer, decreasing its reference count.
     */
    void release();

    /**
     * Closes the buffer (equivalent to release()).
     */
    @Override
    default void close() {
        release();
    }
}
