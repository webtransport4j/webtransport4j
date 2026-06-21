package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;

public interface WebTransportHandler {
  void onSessionReady(WebTransportSession session);
  void onSessionClosed(WebTransportSession session);
  void onIncomingStream(WebTransportSession session, WebTransportStream stream);
  void onDatagramReceived(WebTransportSession session, ByteBuf data);
}
