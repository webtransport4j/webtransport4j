package io.github.webtransport4j.api;

import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;

public interface WebTransportHandler {
  Logger logger = Logger.getLogger(WebTransportHandler.class.getName());
  default void onSessionReady(WebTransportSession session){
    logger.info("🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
  }
  default void onSessionClosed(WebTransportSession session){
    logger.info("🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
    
  }
  default void onIncomingStream(WebTransportSession session, WebTransportStream stream){
    logger.info("📥 [DEFAULT HANDLER] New client-initiated stream received. ID: "
        + stream.streamId() + " | Type: " + (stream.isBidirectional() ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));
        stream.onData(data -> {
          logger.info("📥 [DEFAULT HANDLER] Data received on stream :" + stream.streamId() + " of size :" + data.readableBytes());
        } );
  }
  default void onDatagramReceived(WebTransportSession session, ByteBuf data){
    logger.info("☄️ [DEFAULT HANDLER] Received Datagram of size :" + data.readableBytes());
  }
}
