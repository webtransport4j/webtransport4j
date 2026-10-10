package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.jspecify.annotations.NonNull;

/**
 * Netty-backed buffer with an atomic wrapper lifetime and no monitor locking.
 *
 * <p>Construction transfers ownership of exactly one existing delegate reference to this wrapper;
 * it does not retain the delegate. Wrapper references are counted independently. The owned delegate
 * reference is released exactly once, when the last wrapper reference is released.
 *
 * <p>A caller must own a live wrapper reference throughout each operation, including use of
 * returned borrowed views. Retain before handing the wrapper to asynchronous work, and release that
 * reference when the work finishes. Reference counting is thread-safe; content access, reader-index
 * changes and compound operations require confinement or external coordination.
 */
public class DefaultNettyWebTransportBuffer implements WebTransportBuffer {

  private static final IntHandle<DefaultNettyWebTransportBuffer> REF_CNT_HANDLE =
      Handles.newIntHandle(
          DefaultNettyWebTransportBuffer.class,
          "refCnt",
          MethodHandles.lookup(),
          () ->
              AtomicIntegerFieldUpdater.newUpdater(
                  DefaultNettyWebTransportBuffer.class, "refCnt"));

  private final @NonNull ByteBuf delegate;
  private volatile int refCnt = 1;

  /**
   * Wraps a byte array into a {@link WebTransportBuffer}.
   *
   * @param data payload byte array
   * @return a new WebTransportBuffer wrapping the byte array
   */
  public static @NonNull DefaultNettyWebTransportBuffer wrap(byte @NonNull [] data) {
    return new DefaultNettyWebTransportBuffer(Unpooled.wrappedBuffer(data));
  }

  /**
   * Wraps an existing Netty {@link ByteBuf} into a {@link WebTransportBuffer}.
   *
   * @param buf delegate ByteBuf
   * @return a new WebTransportBuffer wrapping the ByteBuf
   */
  public static @NonNull DefaultNettyWebTransportBuffer wrap(@NonNull ByteBuf buf) {
    return new DefaultNettyWebTransportBuffer(buf);
  }

  /**
   * Wraps an existing NIO {@link ByteBuffer} into a {@link WebTransportBuffer}.
   *
   * @param buffer delegate ByteBuffer
   * @return a new WebTransportBuffer wrapping the ByteBuffer
   */
  public static @NonNull DefaultNettyWebTransportBuffer wrap(@NonNull ByteBuffer buffer) {
    return new DefaultNettyWebTransportBuffer(Unpooled.wrappedBuffer(buffer));
  }

  /**
   * Takes ownership of one existing delegate reference on successful construction.
   *
   * @param delegate a live buffer reference whose ownership is transferred to this wrapper
   */
  public DefaultNettyWebTransportBuffer(@NonNull ByteBuf delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    if (delegate.refCnt() <= 0) {
      throw new IllegalArgumentException("delegate has already been released");
    }
  }

  /**
   * Creates an independently retained slice of the readable bytes. The caller must release it or
   * transfer ownership to a Netty write. It remains valid after this wrapper is released.
   *
   * @return an independently owned readable slice
   */
  public @NonNull ByteBuf retainedReadableBuffer() {
    ensureAccessible();
    return delegate.retainedSlice(delegate.readerIndex(), delegate.readableBytes());
  }

  /**
   * Returns a borrowed delegate. Do not release the reference owned by this wrapper. To keep it
   * independently, explicitly acquire and later release an additional delegate reference.
   *
   * @return the borrowed delegate
   */
  public @NonNull ByteBuf delegate() {
    ensureAccessible();
    return delegate;
  }

  /**
   * Returns the wrapper reference count, not the delegate count. Independent slices are excluded.
   *
   * @return the current wrapper reference count
   */
  public int refCnt() {
    return refCnt;
  }

  @Override
  public int readableBytes() {
    ensureAccessible();
    return delegate.readableBytes();
  }

  /** Returns a borrowed view; a live reference must be held until use of the view finishes. */
  @Override
  public ByteBuffer nioBuffer() {
    ensureAccessible();
    return delegate.nioBuffer();
  }

  @Override
  public ByteBuffer skipBytes(int length) {
    ensureAccessible();
    return delegate.skipBytes(length).nioBuffer();
  }

  @Override
  public byte getByte(int index) {
    ensureAccessible();
    return delegate.getByte(delegate.readerIndex() + index);
  }

  @Override
  public byte readByte() {
    ensureAccessible();
    return delegate.readByte();
  }

  @Override
  public int getInt(int index) {
    ensureAccessible();
    return delegate.getInt(delegate.readerIndex() + index);
  }

  @Override
  public long getLong(int index) {
    ensureAccessible();
    return delegate.getLong(delegate.readerIndex() + index);
  }

  @Override
  public boolean equalsBytes(byte @NonNull [] expected) {
    ensureAccessible();
    if (delegate.readableBytes() != expected.length) {
      return false;
    }
    int readerIdx = delegate.readerIndex();
    for (int i = 0; i < expected.length; i++) {
      if (delegate.getByte(readerIdx + i) != expected[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean startsWith(byte @NonNull [] prefix) {
    ensureAccessible();
    if (delegate.readableBytes() < prefix.length) {
      return false;
    }
    int readerIdx = delegate.readerIndex();
    for (int i = 0; i < prefix.length; i++) {
      if (delegate.getByte(readerIdx + i) != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] readBytes() {
    ensureAccessible();
    byte[] bytes = new byte[delegate.readableBytes()];
    delegate.readBytes(bytes);
    return bytes;
  }

  @Override
  public WebTransportBuffer retain() {
    return retain(1);
  }

  /**
   * Acquires additional wrapper references without changing the delegate reference count.
   *
   * @param increment positive number of references to acquire
   * @return this buffer
   * @throws IllegalStateException if released or the wrapper reference count would overflow
   */
  public WebTransportBuffer retain(int increment) {
    if (increment <= 0) {
      throw new IllegalArgumentException("increment must be positive: " + increment);
    }
    for (; ; ) {
      int current = refCnt;
      if (current == 0) {
        throw new IllegalStateException("buffer has already been released");
      }
      if (increment > Integer.MAX_VALUE - current) {
        throw new IllegalStateException("reference count overflow");
      }
      if (REF_CNT_HANDLE.compareAndSet(this, current, current + increment)) {
        return this;
      }
    }
  }

  /**
   * Releases one wrapper reference. Calls after zero are no-ops, preserving the previous behavior.
   * The thread performing the transition to zero releases the single owned delegate reference.
   */
  @Override
  public void release() {
    for (; ; ) {
      int current = refCnt;
      if (current == 0) {
        return;
      }
      if (REF_CNT_HANDLE.compareAndSet(this, current, current - 1)) {
        if (current == 1) {
          // Never silently swallow invalid delegate ownership or retry this final release.
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

  private void ensureAccessible() {
    if (refCnt == 0) {
      throw new IllegalStateException("buffer has already been released");
    }
  }
}
