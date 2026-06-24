package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface WebTransportHandler {
  Logger logger = LoggerFactory.getLogger(WebTransportHandler.class);
  default void onSessionReady(WebTransportSession session){
    logger.debug("🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: {} | Session Stream ID: {}", session.path(), session.getSessionStreamId());
  }
  default void onSessionClosed(WebTransportSession session){
    logger.debug("🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}", session.path(), session.getSessionStreamId());

  }
  default void onIncomingStream(WebTransportSession session, WebTransportStream stream){
    logger.debug("📥 [DEFAULT HANDLER] New client-initiated stream received. ID: {} | Type: {}", stream.streamId(), (stream.isBidirectional() ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));
        stream.onData(data -> {
          logger.debug("📥 [DEFAULT HANDLER] Data received on stream :{} of size :{}", stream.streamId(), data.readableBytes());
        } );
  }
  default void onDatagramReceived(WebTransportSession session, ByteBuf data){
    logger.debug("☄️ [DEFAULT HANDLER] Received Datagram of size :{}", data.readableBytes());
  }
}
