package io.github.webtransport4j.example;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public class DefaultPathHandler implements WebTransportHandler {
  private static final Logger logger = Logger.getLogger(DefaultPathHandler.class.getName());

  @Override
  public void onSessionReady(WebTransportSession session) {
    logger.info("🟢 [DEFAULT HANDLER] WebTransport Session Ready. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
  }

  @Override
  public void onSessionClosed(WebTransportSession session) {
    logger.info("🔴 [DEFAULT HANDLER] WebTransport Session Closed. Path: " + session.path()
        + " | Session Stream ID: " + session.getSessionStreamId());
  }

  @Override
  public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
    boolean isBidi = stream.isBidirectional();
    logger.info("📥 [DEFAULT HANDLER] New client-initiated stream received. ID: "
        + stream.streamId() + " | Type: " + (isBidi ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));

    stream.onClose(() -> logger.info("🔒 [DEFAULT HANDLER] Stream " + stream.streamId() + " closed."));
    stream.onError(err -> logger.error("❌ [DEFAULT HANDLER] Stream " + stream.streamId() + " error: ", err));

    LengthPrefixedCodec codec =
    new LengthPrefixedCodec(
        stream.streamChannel().alloc());
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
  public void onDatagramReceived(WebTransportSession session, ByteBuf data) {
    String content = data.toString(java.nio.charset.StandardCharsets.UTF_8);
    logger.info("☄️ [DEFAULT HANDLER] Received Datagram: " + content);
    String replyText = "DEFAULT ACK DG: " + content;
    session.sendDatagram(replyText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
