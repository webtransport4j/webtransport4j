package io.github.webtransport4j.example;

import io.github.webtransport4j.api.BinarySources;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A comprehensive test and demonstration handler for WebTransport features. This handler
 * illustrates:
 *
 * <ul>
 *   <li>Handling session lifecycle events (ready, closed)
 *   <li>Initiating server-side bidirectional and unidirectional streams
 *   <li>Handling client-initiated bidirectional and unidirectional streams
 *   <li>Sending and receiving session-level datagrams (echo back)
 * </ul>
 */
public class WebTransportTestHandler implements WebTransportHandler {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportTestHandler.class);

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    logger.info(
        "🟢 [TEST HANDLER] WebTransport Session Ready. Path: {} | Session Stream ID: {}",
        session.path(),
        session.getSessionStreamId());

    // 1. Initiate a Server-to-Client Unidirectional Stream
    logger.info("🚀 [TEST HANDLER] Creating server-initiated unidirectional stream...");
    session
        .createUniStream()
        .addListener(
            (Future<WebTransportStream> f) -> {
              if (f.isSuccess()) {
                WebTransportStream stream = f.getNow();
                logger.info(
                    "   👉 Unidirectional stream created successfully. ID: {}", stream.streamId());
                stream
                    .writeText(
                        "Hello from Server-Initiated Unidirectional Stream! [ID: "
                            + stream.streamId()
                            + "]")
                    .addListener(
                        wf -> {
                          if (wf.isSuccess()) {
                            logger.info(
                                "   ✅ Sent data over server uni stream {}", stream.streamId());
                          } else {
                            logger.error("   ❌ Failed to write to server uni stream", wf.cause());
                          }
                        });
                stream
                    .write(BinarySources.fromFile(new File("/Users/sam/Downloads/images.zip")))
                    .addListener(
                        wf -> {
                          if (wf.isSuccess()) {
                            logger.info(
                                "   ✅ Sent file data on server bidi stream {}", stream.streamId());
                          } else {
                            logger.error(
                                "   ❌ Failed to send file data on server bidi stream", wf.cause());
                          }
                        });
              } else {
                logger.error(
                    "   ❌ Failed to create server-initiated unidirectional stream", f.cause());
              }
            });

    // 2. Initiate a Server-to-Client Bidirectional Stream
    logger.info("🚀 [TEST HANDLER] Creating server-initiated bidirectional stream...");
    session
        .createBiStream()
        .addListener(
            (Future<WebTransportStream> f) -> {
              if (f.isSuccess()) {
                WebTransportStream stream = f.getNow();
                logger.info(
                    "   👉 Bidirectional stream created successfully. ID: {}", stream.streamId());

                // Listen to client responses on this stream
                stream.onData(
                    data -> {
                      String content = new String(data.readBytes(), StandardCharsets.UTF_8);
                      logger.info(
                          "   📩 Received response on server-initiated bidi stream {}: {}",
                          stream.streamId(),
                          content);
                    });

                stream.onClose(
                    () ->
                        logger.info(
                            "   🔒 Server-initiated bidi stream {} closed.", stream.streamId()));
                stream.onError(
                    err ->
                        logger.error(
                            "   ❌ Server-initiated bidi stream {} error", stream.streamId(), err));

                // Write test greeting
                stream
                    .writeText(
                        "Hello from Server-Initiated Bidirectional Stream! [ID: "
                            + stream.streamId()
                            + "]")
                    .addListener(
                        wf -> {
                          if (wf.isSuccess()) {
                            logger.info(
                                "   ✅ Sent greeting on server bidi stream {}", stream.streamId());
                          } else {
                            logger.error(
                                "   ❌ Failed to send greeting on server bidi stream", wf.cause());
                          }
                        });
                stream
                    .write(BinarySources.fromFile(new File("/Users/sam/Downloads/images.zip")))
                    .addListener(
                        wf -> {
                          if (wf.isSuccess()) {
                            logger.info(
                                "   ✅ Sent file data on server bidi stream {}", stream.streamId());
                          } else {
                            logger.error(
                                "   ❌ Failed to send file data on server bidi stream", wf.cause());
                          }
                        });
              } else {
                logger.error(
                    "   ❌ Failed to create server-initiated bidirectional stream", f.cause());
              }
            });
  }

  @Override
  public void onSessionClosed(@NonNull WebTransportSession session) {
    logger.info(
        "🔴 [TEST HANDLER] WebTransport Session Closed. Path: {} | Session Stream ID: {}",
        session.path(),
        session.getSessionStreamId());
  }

  @Override
  public void onIncomingStream(
      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    boolean isBidi = stream.isBidirectional();
    logger.info(
        "📥 [TEST HANDLER] New client-initiated stream received. ID: {} | Type: {}",
        stream.streamId(),
        (isBidi ? "BIDIRECTIONAL" : "UNIDIRECTIONAL"));

    // Register callbacks
    stream.onClose(() -> logger.info("🔒 Client-initiated stream {} closed.", stream.streamId()));
    stream.onError(
        err -> logger.error("❌ Client-initiated stream {} error", stream.streamId(), err));

    stream.onData(
        data -> {
          byte[] bytes = data.readBytes();

          String prefixCheck =
              new String(bytes, 0, Math.min(bytes.length, 20), StandardCharsets.UTF_8);
          if (prefixCheck.startsWith("SleepServer_")) {
            logger.info(
                "😴 Server received Sleep command on stream {}. Simulating heavy blocking task...",
                stream.streamId());
            try {
              Thread.sleep(3000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            logger.info("⏰ Server woke up on stream {} after sleeping.", stream.streamId());
          }

          if (isBidi) {
            if (!stream.hasAttribute("prefixed")) {
              stream.setAttribute("prefixed", true);
              byte[] prefixBytes = "ACK BI: ".getBytes(StandardCharsets.UTF_8);
              byte[] outBytes = new byte[prefixBytes.length + bytes.length];
              System.arraycopy(prefixBytes, 0, outBytes, 0, prefixBytes.length);
              System.arraycopy(bytes, 0, outBytes, prefixBytes.length, bytes.length);
              stream
                  .write(outBytes)
                  .addListener(
                      f -> {
                        if (f.isSuccess()) {
                          logger.info(
                              "✅ Echoed response to client on bidi stream {}", stream.streamId());
                        } else {
                          logger.error(
                              "❌ Failed to echo to client on bidi stream {}",
                              stream.streamId(),
                              f.cause());
                        }
                      });
            } else {
              // Already prefixed this stream, just echo the raw chunk
              stream.write(bytes);
            }
          } else {
            // Echo an ACK back via a NEW Server-to-Client Unidirectional stream so Python can
            // assert it
            session
                .createUniStream()
                .addListener(
                    (Future<WebTransportStream> f) -> {
                      if (f.isSuccess()) {
                        WebTransportStream ackStream = f.getNow();
                        byte[] prefixBytes = "ACK UNI: ".getBytes(StandardCharsets.UTF_8);
                        byte[] outBytes = new byte[prefixBytes.length + bytes.length];
                        System.arraycopy(prefixBytes, 0, outBytes, 0, prefixBytes.length);
                        System.arraycopy(bytes, 0, outBytes, prefixBytes.length, bytes.length);
                        ackStream
                            .write(outBytes)
                            .addListener(
                                wf -> {
                                  ackStream.close();
                                });
                      }
                    });
          }
        });
  }

  @Override
  public void onDatagramReceived(
      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
    String content = new String(data.readBytes(), StandardCharsets.UTF_8);
    logger.info("☄️ [TEST HANDLER] Received Datagram: {}", content);

    // Echo back the datagram package to the client via a Uni Stream
    String replyText = "ACK DG: " + content;
    session
        .createUniStream()
        .addListener(
            f -> {
              if (f.isSuccess()) {
                WebTransportStream ackStream = (WebTransportStream) f.getNow();
                ackStream
                    .writeText(replyText)
                    .addListener(
                        wf -> {
                          ackStream.close();
                        });
              } else {
                logger.error("❌ Failed to send datagram response via uni stream", f.cause());
              }
            });
  }
}
