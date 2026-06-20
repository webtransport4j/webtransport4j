package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

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
  public WebTransportDatagramFrame copy() {
    return new WebTransportDatagramFrame(sessionId, content().copy());
  }

  @Override
  public WebTransportDatagramFrame duplicate() {
    return new WebTransportDatagramFrame(sessionId, content().duplicate());
  }

  @Override
  public WebTransportDatagramFrame retainedDuplicate() {
    return new WebTransportDatagramFrame(sessionId, content().retainedDuplicate());
  }

  @Override
  public WebTransportDatagramFrame replace(ByteBuf content) {
    return new WebTransportDatagramFrame(sessionId, content);
  }

  @Override
  public WebTransportDatagramFrame retain() {
    super.retain();
    return this;
  }

  @Override
  public WebTransportDatagramFrame retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public WebTransportDatagramFrame touch() {
    super.touch();
    return this;
  }

  @Override
  public WebTransportDatagramFrame touch(Object hint) {
    super.touch(hint);
    return this;
  }

  @Override
  public String toString() {
    return "WebTransportDatagramFrame(sessionId=" + sessionId + ", content=" + content() + ")";
  }
}
