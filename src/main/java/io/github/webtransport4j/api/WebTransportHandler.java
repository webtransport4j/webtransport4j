package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;

public interface WebTransportHandler {
  Logger logger = Logger.getLogger(WebTransportHandler.class.getName());
  default void onSessionReady(WebTransportSession session){
    logger.debug("🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
  }
  default void onSessionClosed(WebTransportSession session){
    logger.debug("🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
    
  }
  default void onIncomingStream(WebTransportSession session, WebTransportStream stream){
    logger.debug("📥 [DEFAULT HANDLER] New client-initiated stream received. ID: "
        + stream.streamId() + " | Type: " + (stream.isBidirectional() ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));
        stream.onData(data -> {
          logger.debug("📥 [DEFAULT HANDLER] Data received on stream :" + stream.streamId() + " of size :" + data.readableBytes());
        } );
  }
  default void onDatagramReceived(WebTransportSession session, ByteBuf data){
    logger.debug("☄️ [DEFAULT HANDLER] Received Datagram of size :" + data.readableBytes());
  }
}
