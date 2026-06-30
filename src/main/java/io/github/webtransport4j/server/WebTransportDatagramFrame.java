package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import org.jspecify.annotations.NonNull;

/** WebTransport datagram frame representation. */
public class WebTransportDatagramFrame extends DefaultByteBufHolder implements WebTransportFrame {

  private final long sessionId;

  public WebTransportDatagramFrame(long sessionId, ByteBuf data) {
    super(data);
    this.sessionId = sessionId;
  }

  @Override
  public long sessionId() {
    return sessionId;
  }

  @Override
  public @NonNull WebTransportDatagramFrame copy() {
    return new WebTransportDatagramFrame(sessionId, content().copy());
  }

  @Override
  public @NonNull WebTransportDatagramFrame duplicate() {
    return new WebTransportDatagramFrame(sessionId, content().duplicate());
  }

  @Override
  public @NonNull WebTransportDatagramFrame retainedDuplicate() {
    return new WebTransportDatagramFrame(sessionId, content().retainedDuplicate());
  }

  @Override
  public @NonNull WebTransportDatagramFrame replace(@NonNull ByteBuf content) {
    return new WebTransportDatagramFrame(sessionId, content);
  }

  @Override
  public @NonNull WebTransportDatagramFrame retain() {
    super.retain();
    return this;
  }

  @Override
  public @NonNull WebTransportDatagramFrame retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public @NonNull WebTransportDatagramFrame touch() {
    super.touch();
    return this;
  }

  @Override
  public @NonNull WebTransportDatagramFrame touch(@NonNull Object hint) {
    super.touch(hint);
    return this;
  }

  @Override
  public @NonNull String toString() {
    return "WebTransportDatagramFrame(sessionId=" + sessionId + ", content=" + content() + ")";
  }
}
