package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

public class WebTransportCapsule extends DefaultByteBufHolder implements WebTransportFrame {
  private final long sessionId;
  private final long capsuleType;

  public WebTransportCapsule(long sessionId, long capsuleType, ByteBuf data) {
    super(data);
    this.sessionId = sessionId;
    this.capsuleType = capsuleType;
  }

  @Override
  public long sessionId() {
    return sessionId;
  }

  public long capsuleType() {
    return capsuleType;
  }

  @Override
  public WebTransportCapsule copy() {
    return new WebTransportCapsule(sessionId, capsuleType, content().copy());
  }

  @Override
  public WebTransportCapsule duplicate() {
    return new WebTransportCapsule(sessionId, capsuleType, content().duplicate());
  }

  @Override
  public WebTransportCapsule retainedDuplicate() {
    return new WebTransportCapsule(sessionId, capsuleType, content().retainedDuplicate());
  }

  @Override
  public WebTransportCapsule replace(ByteBuf content) {
    return new WebTransportCapsule(sessionId, capsuleType, content);
  }

  @Override
  public WebTransportCapsule retain() {
    super.retain();
    return this;
  }

  @Override
  public WebTransportCapsule retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public WebTransportCapsule touch() {
    super.touch();
    return this;
  }

  @Override
  public WebTransportCapsule touch(Object hint) {
    super.touch(hint);
    return this;
  }

  @Override
  public String toString() {
    return "WebTransportCapsule(sessionId="
        + sessionId
        + ", capsuleType=0x"
        + Long.toHexString(capsuleType)
        + ", content="
        + content()
        + ")";
  }
}
