package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/** Internal implementation of {@link WebTransportBuffer} that wraps a Netty {@link ByteBuf}. */
public class DefaultNettyWebTransportBuffer implements WebTransportBuffer {

  private final @NonNull ByteBuf delegate;

  public DefaultNettyWebTransportBuffer(@NonNull ByteBuf delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
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
  public byte[] readBytes() {
    byte[] bytes = new byte[delegate.readableBytes()];
    delegate.readBytes(bytes);
    return bytes;
  }

  @Override
  public WebTransportBuffer retain() {
    delegate.retain();
    return this;
  }

  @Override
  public void release() {
    delegate.release();
  }
}
