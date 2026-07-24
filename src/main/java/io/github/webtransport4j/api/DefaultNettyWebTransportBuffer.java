package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/** Internal implementation of {@link WebTransportBuffer} that wraps a Netty {@link ByteBuf}. */
public class DefaultNettyWebTransportBuffer implements WebTransportBuffer {

  private @NonNull ByteBuf delegate;

  public DefaultNettyWebTransportBuffer(@NonNull ByteBuf delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
  }

  public @NonNull DefaultNettyWebTransportBuffer wrap(@NonNull ByteBuf delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    return this;
  }

  @NonNull ByteBuf retainedReadableBuffer() {
    return delegate.retainedSlice(delegate.readerIndex(), delegate.readableBytes());
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
    return new DefaultNettyWebTransportBuffer(delegate.retainedSlice());
  }

  @Override
  public void release() {
    delegate.release();
  }
}
