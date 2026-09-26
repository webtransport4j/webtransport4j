package io.github.webtransport4j.api;

import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler interface for WebTransport events. */
public interface WebTransportHandler {
  Logger logger = LoggerFactory.getLogger(WebTransportHandler.class);

  /**
   * Selects an application subprotocol from the list provided by the client
   * via the WT-Available-Protocols header as per draft-16 Section 3.3.
   *
   * @param availableProtocols list of protocols advertised by the client
   * @return the selected protocol, or null if no subprotocol is selected
   */
  default @Nullable String selectSubprotocol(@NonNull List<String> availableProtocols) {
    return null;
  }

  /** On Session Ready. */
  default void onSessionReady(@NonNull WebTransportSession session) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: {} | Session Stream ID: {}",
          session.path(),
          session.getSessionStreamId());
    }
  }

  /** On Session Closed. */
  default void onSessionClosed(@NonNull WebTransportSession session) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}",
          session.path(),
          session.getSessionStreamId());
    }
  }

  /** On Incoming Stream. */
  default void onIncomingStream(
      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "📥 [DEFAULT HANDLER] New client-initiated stream received. ID: {} | Type: {}",
          stream.streamId(),
          (stream.isBidirectional() ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));
    }
    stream.onData(
        data -> {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "📥 [DEFAULT HANDLER] Data received on stream :{} of size :{}",
                stream.streamId(),
                data.readableBytes());
          }
        });
  }

  /** On Datagram Received. */
  default void onDatagramReceived(
      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
    if (logger.isDebugEnabled()) {
      logger.debug("☄️ [DEFAULT HANDLER] Received Datagram of size :{}", data.readableBytes());
    }
  }
}
