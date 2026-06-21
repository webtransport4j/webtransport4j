package io.github.webtransport4j.example;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;

/**
 * A comprehensive test and demonstration handler for WebTransport features.
 * This handler illustrates:
 * <ul>
 *   <li>Handling session lifecycle events (ready, closed)</li>
 *   <li>Initiating server-side bidirectional and unidirectional streams</li>
 *   <li>Handling client-initiated bidirectional and unidirectional streams</li>
 *   <li>Sending and receiving session-level datagrams (echo back)</li>
 * </ul>
 */
public class WebTransportTestHandler implements WebTransportHandler {

  private static final Logger logger = Logger.getLogger(WebTransportTestHandler.class.getName());

  @Override
  public void onSessionReady(WebTransportSession session) {
    logger.info("🟢 [TEST HANDLER] WebTransport Session Ready. Path: " + session.path() 
        + " | Session Stream ID: " + session.getSessionStreamId());

      // 1. Initiate a Server-to-Client Unidirectional Stream
      logger.info("🚀 [TEST HANDLER] Creating server-initiated unidirectional stream...");
      session.createUniStream().addListener((Future<WebTransportStream> f) -> {
        if (f.isSuccess()) {
          WebTransportStream stream = f.getNow();
          logger.info("   👉 Unidirectional stream created successfully. ID: " + stream.streamId());
          stream.writeText("Hello from Server-Initiated Unidirectional Stream! [ID: " + stream.streamId() + "]")
              .addListener(wf -> {
                if (wf.isSuccess()) {
                  logger.info("   ✅ Sent data over server uni stream " + stream.streamId());
                } else {
                  logger.error("   ❌ Failed to write to server uni stream", wf.cause());
                }
              });
        } else {
          logger.error("   ❌ Failed to create server-initiated unidirectional stream", f.cause());
        }
      });

      // 2. Initiate a Server-to-Client Bidirectional Stream
      logger.info("🚀 [TEST HANDLER] Creating server-initiated bidirectional stream...");
      session.createBiStream().addListener((Future<WebTransportStream> f) -> {
        if (f.isSuccess()) {
          WebTransportStream stream = f.getNow();
          logger.info("   👉 Bidirectional stream created successfully. ID: " + stream.streamId());
          
          // Listen to client responses on this stream
          stream.onData(data -> {
            String content = data.toString(StandardCharsets.UTF_8);
            logger.info("   📩 Received response on server-initiated bidi stream " 
                + stream.streamId() + ": " + content);
          });

          stream.onClose(() -> logger.info("   🔒 Server-initiated bidi stream " + stream.streamId() + " closed."));
          stream.onError(err -> logger.error("   ❌ Server-initiated bidi stream " + stream.streamId() + " error: ", err));

          // Write test greeting
          stream.writeText("Hello from Server-Initiated Bidirectional Stream! [ID: " + stream.streamId() + "]")
              .addListener(wf -> {
                if (wf.isSuccess()) {
                  logger.info("   ✅ Sent greeting on server bidi stream " + stream.streamId());
                } else {
                  logger.error("   ❌ Failed to send greeting on server bidi stream", wf.cause());
                }
              });
        } else {
          logger.error("   ❌ Failed to create server-initiated bidirectional stream", f.cause());
        }
      });

  }

  @Override
  public void onSessionClosed(WebTransportSession session) {
    logger.info("🔴 [TEST HANDLER] WebTransport Session Closed. Path: " + session.path() 
        + " | Session Stream ID: " + session.getSessionStreamId());
  }

  @Override
  public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
    boolean isBidi = stream.isBidirectional();
    logger.info("📥 [TEST HANDLER] New client-initiated stream received. ID: " 
        + stream.streamId() + " | Type: " + (isBidi ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));

    // Register callbacks
    stream.onClose(() -> logger.info("🔒 Client-initiated stream " + stream.streamId() + " closed."));
    stream.onError(err -> logger.error("❌ Client-initiated stream " + stream.streamId() + " error: ", err));

    stream.onData(data -> {
      String content = data.toString(StandardCharsets.UTF_8);
      logger.info("📩 Received data on stream " + stream.streamId() + ": " + content);

      if (isBidi) {
        // Echo back data with prefix for bidirectional streams
        String replyText = "ACK BI: Server received from " + session.path() + ": " + content;
        stream.writeText(replyText).addListener(f -> {
          if (f.isSuccess()) {
            logger.info("✅ Echoed response to client on bidi stream " + stream.streamId());
          } else {
            logger.error("❌ Failed to echo to client on bidi stream " + stream.streamId(), f.cause());
          }
        });
      } else {
        // Unidirectional streams from client are write-only from client's perspective,
        // so the server cannot write back. We just print/log it.
        logger.info("ℹ️ Unidirectional stream data processed (no echo possible on write-only stream).");
      }
    });
  }

  @Override
  public void onDatagramReceived(WebTransportSession session, ByteBuf data) {
    String content = data.toString(StandardCharsets.UTF_8);
    logger.info("☄️ [TEST HANDLER] Received Datagram: " + content);

    // Echo back the datagram package to the client
    String replyText = "ACK DG: Server received datagram: " + content;
    byte[] bytes = replyText.getBytes(StandardCharsets.UTF_8);
    
    session.sendDatagram(bytes).addListener(f -> {
      if (f.isSuccess()) {
        logger.info("✅ Echoed datagram response back to client.");
      } else {
        logger.error("❌ Failed to send datagram response", f.cause());
      }
    });
  }
}
