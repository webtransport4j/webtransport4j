package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thread-confined borrowed buffer view over Netty {@link ByteBuf} instances.
 *
 * <p>Avoids Java heap wrapper object allocations by reusing a thread-confined flyweight instance
 * attached directly to incoming network buffers. Retaining promotes this same object to owned
 * storage and permanently disables recycling. Server callbacks use independent owned buffers.
 */
public class FlyweightWebTransportBuffer implements WebTransportBuffer {

  private @Nullable ByteBuf delegate;
  private final boolean flyweight;
  private @Nullable DefaultNettyWebTransportBuffer owned;

  /**
   * Creates an uninitialized reusable thread-confined flyweight buffer instance.
   *
   * @return a new flyweight buffer instance
   */
  public static @NonNull FlyweightWebTransportBuffer createFlyweight() {
    return new FlyweightWebTransportBuffer(null, true);
  }

  /**
   * Wraps an existing Netty {@link ByteBuf} as a non-flyweight buffer.
   *
   * @param buf delegate ByteBuf
   * @return a new FlyweightWebTransportBuffer
   */
  public static @NonNull FlyweightWebTransportBuffer wrap(@NonNull ByteBuf buf) {
    return new FlyweightWebTransportBuffer(Objects.requireNonNull(buf, "buf must not be null"), false);
  }

  protected FlyweightWebTransportBuffer(@Nullable ByteBuf delegate, boolean flyweight) {
    this.delegate = delegate;
    this.flyweight = flyweight;
    if (!flyweight) {
      this.owned = new DefaultNettyWebTransportBuffer(Objects.requireNonNull(delegate));
    }
  }

  /**
   * Configures this flyweight to point to the provided {@link ByteBuf} without allocating memory.
   *
   * @param buf the delegate ByteBuf
   * @return this flyweight instance
   */
  public @NonNull FlyweightWebTransportBuffer attach(@NonNull ByteBuf buf) {
    if (!flyweight) {
      throw new IllegalStateException("Only flyweight instances can be re-attached");
    }
    if (delegate != null || owned != null) {
      throw new IllegalStateException("Attached or promoted buffers cannot be re-attached");
    }
    this.delegate = buf;
    return this;
  }

  /**
   * Detaches a borrowed view. Retained ownership remains live until released.
   */
  public void detach() {
    if (owned == null) {
      this.delegate = null;
    }
  }

  /**
   * Returns the delegate {@link ByteBuf}.
   *
   * @return delegate ByteBuf
   */
  public @Nullable ByteBuf delegate() {
    return delegate;
  }

  @Override
  public int readableBytes() {
    ensureAccessible();
    return Objects.requireNonNull(delegate).readableBytes();
  }

  @Override
  public ByteBuffer nioBuffer() {
    ensureAccessible();
    return Objects.requireNonNull(delegate).nioBuffer();
  }

  @Override
  public ByteBuffer skipBytes(int bytes) {
    ensureAccessible();
    return Objects.requireNonNull(delegate).skipBytes(bytes).nioBuffer();
  }

  @Override
  public byte getByte(int index) {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    return d.getByte(d.readerIndex() + index);
  }

  @Override
  public byte readByte() {
    ensureAccessible();
    return Objects.requireNonNull(delegate).readByte();
  }

  @Override
  public int getInt(int index) {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    return d.getInt(d.readerIndex() + index);
  }

  @Override
  public long getLong(int index) {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    return d.getLong(d.readerIndex() + index);
  }

  @Override
  public boolean equalsBytes(byte @NonNull [] expected) {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    if (d.readableBytes() != expected.length) {
      return false;
    }
    int readerIdx = d.readerIndex();
    for (int i = 0; i < expected.length; i++) {
      if (d.getByte(readerIdx + i) != expected[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean startsWith(byte @NonNull [] prefix) {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    if (d.readableBytes() < prefix.length) {
      return false;
    }
    int readerIdx = d.readerIndex();
    for (int i = 0; i < prefix.length; i++) {
      if (d.getByte(readerIdx + i) != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] readBytes() {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    byte[] arr = new byte[d.readableBytes()];
    d.readBytes(arr);
    return arr;
  }

  @Override
  public WebTransportBuffer retain() {
    ensureAccessible();
    ByteBuf d = Objects.requireNonNull(delegate);
    if (owned == null) {
      // Preserve identity; promoted objects must never be recycled after the callback.
      owned = DefaultNettyWebTransportBuffer.wrap(d.retainedSlice());
      delegate = owned.delegate();
    } else {
      owned.retain();
    }
    return this;
  }

  @Override
  public void release() {
    if (owned == null) {
      detach();
      return;
    }
    owned.release();
    if (owned.refCnt() == 0) {
      delegate = null;
    }
  }

  private void ensureAccessible() {
    if (delegate == null) {
      throw new IllegalStateException("Buffer is detached or already released");
    }
  }
}
