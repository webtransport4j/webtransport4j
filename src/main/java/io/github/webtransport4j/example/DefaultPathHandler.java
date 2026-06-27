package io.github.webtransport4j.example;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPathHandler implements WebTransportHandler {
  private static final Logger logger = LoggerFactory.getLogger(DefaultPathHandler.class);

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    logger.info("🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: {} | Session Stream ID: {}", session.path(), session.getSessionStreamId());
  }

  @Override
  public void onSessionClosed(@NonNull WebTransportSession session) {
    logger.info("🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}", session.path(), session.getSessionStreamId());
  }

  @Override
  public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    boolean isBidi = stream.isBidirectional();
    logger.info("📥 [DEFAULT HANDLER] New client-initiated stream received. ID: {} | Type: {}", stream.streamId(), (isBidi ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));

    stream.onClose(() -> logger.info("🔒 [DEFAULT HANDLER] Stream {} closed.", stream.streamId()));
    stream.onError(err -> logger.error("❌ [DEFAULT HANDLER] Stream {} error", stream.streamId(), err));

    LengthPrefixedCodec codec =
    new LengthPrefixedCodec();
    stream.onData(
        codec,
        message -> {
            System.out.println(
                "      ["+ stream.streamId() + "] Received data: "+message.toString(java.nio.charset.StandardCharsets.UTF_8));

            ByteBuf payload =
    Unpooled.copiedBuffer(
        "hello",
        StandardCharsets.UTF_8);

stream.write(codec.encode(payload));
        }
    );
  }

  @Override
  public void onDatagramReceived(@NonNull WebTransportSession session, @NonNull ByteBuf data) {
    String content = data.toString(java.nio.charset.StandardCharsets.UTF_8);
    logger.info("☄️ [DEFAULT HANDLER] Received Datagram: {}", content);
    String replyText = "DEFAULT ACK DG: " + content;
    session.sendDatagram(replyText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
