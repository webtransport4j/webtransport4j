package io.github.webtransport4j.example;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
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

    
    stream.onData(
        new LengthPrefixedCodec(
            stream.channel().alloc()),
        message -> {
            System.out.println(
                "✅ ["+ stream.streamId() + "] Received data: "+message.toString(java.nio.charset.StandardCharsets.UTF_8));

            ByteBuf msgBuf = io.netty.buffer.Unpooled.copiedBuffer("dsdsds", java.nio.charset.StandardCharsets.UTF_8);
            ByteBuf encodedBuf = new LengthPrefixedCodec(stream.channel().alloc()).encode(stream.channel().alloc(), msgBuf);
            stream.write(encodedBuf);
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
