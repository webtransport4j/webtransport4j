package io.github.webtransport4j.api;

import java.net.SocketAddress;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler interface for WebTransport events. */
public interface WebTransportHandler {
  Logger logger = LoggerFactory.getLogger(WebTransportHandler.class);

  /**
   * Evaluates and authorizes an incoming WebTransport extended CONNECT session request prior to admission.
   *
   * <p>Handlers can inspect request headers (e.g. {@code Authorization}, {@code Cookie}), query
   * parameters, or client origin, and return {@code false} to reject the session with HTTP 403 Forbidden.
   *
   * @param request the incoming session request context
   * @return {@code true} to admit the session; {@code false} to reject with 403 Forbidden
   */
  default boolean onSessionRequest(@NonNull SessionRequestContext request) {
    return true;
  }

  /**
   * Selects an application subprotocol from the list provided by the client via the
   * WT-Available-Protocols header as per draft-16 Section 3.3.
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

  /** Signals graceful shutdown; existing streams and datagrams remain usable until closure. */
  default void onSessionDraining(@NonNull WebTransportSession session) {}

  /** On Session Closed. */
  default void onSessionClosed(@NonNull WebTransportSession session) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}",
          session.path(),
          session.getSessionStreamId());
    }
  }

  /**
   * On Session Closed with termination status code and diagnostic reason phrase per draft-16 Section 6.
   * By default, delegates to {@link #onSessionClosed(WebTransportSession)}.
   *
   * @param session the closed WebTransport session
   * @param closeCode the session termination status code (0 = graceful close)
   * @param reason the optional session closure reason, or null
   */
  default void onSessionClosed(
      @NonNull WebTransportSession session, int closeCode, @Nullable String reason) {
    onSessionClosed(session);
  }

  /**
   * Invoked when an unhandled transport, decoding, or execution error occurs on the session.
   *
   * @param session the affected WebTransport session
   * @param cause the error cause
   */
  default void onError(@NonNull WebTransportSession session, @NonNull Throwable cause) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "⚠️ [DEFAULT HANDLER] WebTransport Session Error. Path: {} | Session Stream ID: {}",
          session.path(),
          session.getSessionStreamId(),
          cause);
    }
  }

  /**
   * Invoked when the client peer migrates to a new network path (IP address or port change).
   *
   * <p>In QUIC and WebTransport, connection migration is handled transparently by the transport
   * layer, and existing streams and datagrams continue without interruption. Applications only
   * need to override this if they require custom auditing, IP tracking, or geo-location updates.
   *
   * @param session the WebTransport session
   * @param oldAddress the previous remote client socket address
   * @param newAddress the new remote client socket address
   */
  default void onConnectionMigration(
      @NonNull WebTransportSession session,
      @NonNull SocketAddress oldAddress,
      @NonNull SocketAddress newAddress) {}

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
