package io.github.webtransport4j.example;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default handler for WebTransport paths. */
public class DefaultPathHandler implements WebTransportHandler {
  private static final Logger logger = LoggerFactory.getLogger(DefaultPathHandler.class);

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    logger.info(
        "🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: {} | Session Stream ID: {}",
        session.path(),
        session.getSessionStreamId());
  }

  @Override
  public void onSessionClosed(@NonNull WebTransportSession session) {
    logger.info(
        "🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}",
        session.path(),
        session.getSessionStreamId());
  }

  @Override
  public void onIncomingStream(
      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    boolean isBidi = stream.isBidirectional();
    logger.info(
        "📥 [DEFAULT HANDLER] New client-initiated stream received. ID: {} | Type: {}",
        stream.streamId(),
        (isBidi ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));

    stream.onClose(() -> logger.info("🔒 [DEFAULT HANDLER] Stream {} closed.", stream.streamId()));
    stream.onError(
        err -> logger.error("❌ [DEFAULT HANDLER] Stream {} error", stream.streamId(), err));

    stream.onData(
        buffer -> {
          logger.debug("📨 [DEFAULT HANDLER] Stream [{}] received {} bytes", stream.streamId(), buffer.readableBytes());
          stream.write(buffer);
        });
  }

  @Override
  public void onDatagramReceived(
      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
    String content = new String(data.readBytes(), StandardCharsets.UTF_8);
    logger.info("☄️ [DEFAULT HANDLER] Received Datagram: {}", content);
    String replyText = "DEFAULT ACK DG: " + content;
    session.sendDatagram(replyText.getBytes(StandardCharsets.UTF_8));
  }
}
