package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

/**
 * Netty-backed implementation of {@link WebTransportBuffer} wrapping a {@link ByteBuf}.
 *
 * <p>This wrapper is immutable in its delegate reference and thread-safe.
 * Calling {@link #retain()} increments the underlying buffer's reference count.
 * Calling {@link #release()} safely decrements the reference count if still retained.
 */
public class DefaultNettyWebTransportBuffer implements WebTransportBuffer {

  private final @NonNull ByteBuf delegate;
  private final AtomicInteger refCnt;

  public DefaultNettyWebTransportBuffer(@NonNull ByteBuf delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.refCnt = new AtomicInteger(1);
  }

  /**
   * Returns a retained slice of the readable bytes for outbound writing.
   *
   * @return retained readable slice
   */
  public @NonNull ByteBuf retainedReadableBuffer() {
    return delegate.retainedSlice(delegate.readerIndex(), delegate.readableBytes());
  }

  /**
   * Returns the underlying ByteBuf delegate.
   *
   * @return the delegate ByteBuf
   */
  public @NonNull ByteBuf delegate() {
    return delegate;
  }

  /**
   * Returns the current reference count of this buffer wrapper.
   *
   * @return reference count
   */
  public int refCnt() {
    return refCnt.get();
  }

  @Override
  public int readableBytes() {
    return delegate.readableBytes();
  }

  @Override
  public ByteBuffer nioBuffer() {
    return delegate.nioBuffer();
  }

  @Override
  public ByteBuffer skipBytes(int length) {
    return delegate.skipBytes(length).nioBuffer();
  }

  @Override
  public byte[] readBytes() {
    byte[] bytes = new byte[delegate.readableBytes()];
    delegate.readBytes(bytes);
    return bytes;
  }

  @Override
  public WebTransportBuffer retain() {
    refCnt.incrementAndGet();
    delegate.retain();
    return this;
  }

  /**
   * Retains the buffer with a custom increment.
   *
   * @param increment the amount to increment reference count
   * @return this buffer
   */
  public WebTransportBuffer retain(int increment) {
    if (increment <= 0) {
      throw new IllegalArgumentException("increment must be positive: " + increment);
    }
    refCnt.addAndGet(increment);
    delegate.retain(increment);
    return this;
  }

  @Override
  public void release() {
    for (;;) {
      int current = refCnt.get();
      if (current <= 0) {
        return;
      }
      if (refCnt.compareAndSet(current, current - 1)) {
        if (delegate.refCnt() > 0) {
          delegate.release();
        }
        return;
      }
    }
  }

  @Override
  public void close() {
    release();
  }
}
