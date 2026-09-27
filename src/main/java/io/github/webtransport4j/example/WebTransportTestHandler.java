package io.github.webtransport4j.example;

import io.github.webtransport4j.api.BinarySources;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.server.WebTransportConfig;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler for testing WebTransport protocol features. */
public class WebTransportTestHandler implements WebTransportHandler {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportTestHandler.class);

  private static final int DEFAULT_MAX_DELAYED_TASKS = 64;
  private static final ScheduledExecutorService SHARED_DELAY_SCHEDULER =
      Executors.newScheduledThreadPool(
          Math.min(2, Math.max(1, Runtime.getRuntime().availableProcessors())),
          r -> {
            Thread t = new Thread(r, "wt-test-delay-worker");
            t.setDaemon(true);
            return t;
          });

  private final ScheduledExecutorService delayScheduler;
  private final Semaphore delayAdmissionSemaphore;
  private final int maxDelayedTasks;

  /** Default constructor using shared scheduler and configured max delayed tasks. */
  public WebTransportTestHandler() {
    this(
        SHARED_DELAY_SCHEDULER,
        WebTransportConfig.getInt(
            "webtransport4j.test.handler.max_delayed_tasks", DEFAULT_MAX_DELAYED_TASKS));
  }

  /**
   * Constructor with custom scheduler and admission capacity.
   *
   * @param delayScheduler dedicated scheduler for delayed tasks
   * @param maxDelayedTasks maximum concurrent delayed tasks admitted
   */
  public WebTransportTestHandler(ScheduledExecutorService delayScheduler, int maxDelayedTasks) {
    this.delayScheduler = delayScheduler;
    this.maxDelayedTasks = maxDelayedTasks;
    this.delayAdmissionSemaphore = new Semaphore(maxDelayedTasks);
  }

  /** Returns the number of currently available delayed task admission permits. */
  public int getAvailableDelayPermits() {
    return delayAdmissionSemaphore.availablePermits();
  }

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    logger.info(
        "🟢 [TEST HANDLER] New WebTransport Session Ready. Path: {} | Session Stream ID: {}",
        session.path(),
        session.getSessionStreamId());

    // 1. Initiate a Server-to-Client Unidirectional Stream
    logger.info("🚀 [TEST HANDLER] Creating server-initiated unidirectional stream...");
    session
        .createUniStream()
        .whenComplete((stream, err) -> {
          if (err == null) {
            logger.info("   👉 Unidirectional stream created successfully. ID: {}", stream.streamId());
            stream
                .writeText("Hello from Server-Initiated Unidirectional Stream! [ID: " + stream.streamId() + "]")
                .whenComplete((res, writeErr) -> {
                  if (writeErr == null) {
                    logger.info("   ✅ Sent data over server uni stream {}", stream.streamId());
                  } else {
                    logger.error("   ❌ Failed to write to server uni stream", writeErr);
                  }
                });
            File sampleFile = new File("/Users/sam/Downloads/images.zip");
            if (sampleFile.exists()) {
              try {
                stream
                    .write(BinarySources.fromFile(sampleFile))
                    .whenComplete((res, writeErr) -> {
                      if (writeErr == null) {
                        logger.info("   ✅ Sent file data on server bidi stream {}", stream.streamId());
                      } else {
                        logger.error("   ❌ Failed to send file data on server bidi stream", writeErr);
                      }
                    });
              } catch (Exception e) {
                logger.error("Error reading file", e);
              }
            }
          } else {
            logger.error("   ❌ Failed to create server-initiated unidirectional stream", err);
          }
        });

    // 2. Initiate a Server-to-Client Bidirectional Stream
    logger.info("🚀 [TEST HANDLER] Creating server-initiated bidirectional stream...");
    session
        .createBiStream()
        .whenComplete((stream, err) -> {
          if (err == null) {
            logger.info("   👉 Bidirectional stream created successfully. ID: {}", stream.streamId());

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
                () -> logger.info("   🔒 Server-initiated bidi stream {} closed.", stream.streamId()));
            stream.onError(
                e -> logger.error("   ❌ Server-initiated bidi stream {} error", stream.streamId(), e));

            // Write test greeting
            stream
                .writeText("Hello from Server-Initiated Bidirectional Stream! [ID: " + stream.streamId() + "]")
                .whenComplete((res, writeErr) -> {
                  if (writeErr == null) {
                    logger.info("   ✅ Sent greeting on server bidi stream {}", stream.streamId());
                  } else {
                    logger.error("   ❌ Failed to send greeting on server bidi stream", writeErr);
                  }
                });
            File sampleFile = new File("/Users/sam/Downloads/images.zip");
            if (sampleFile.exists()) {
              try {
                stream
                    .write(BinarySources.fromFile(sampleFile))
                    .whenComplete((res, writeErr) -> {
                      if (writeErr == null) {
                        logger.info("   ✅ Sent file data on server bidi stream {}", stream.streamId());
                      } else {
                        logger.error("   ❌ Failed to send file data on server bidi stream", writeErr);
                      }
                    });
              } catch (Exception e) {
                logger.error("Error reading file", e);
              }
            }
          } else {
            logger.error("   ❌ Failed to create server-initiated bidirectional stream", err);
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
          Runnable process = () -> {
            if (isBidi) {
              if (!stream.hasAttribute("prefixed")) {
                stream.setAttribute("prefixed", true);
                byte[] prefixBytes = "ACK BI: ".getBytes(StandardCharsets.UTF_8);
                byte[] outBytes = new byte[prefixBytes.length + bytes.length];
                System.arraycopy(prefixBytes, 0, outBytes, 0, prefixBytes.length);
                System.arraycopy(bytes, 0, outBytes, prefixBytes.length, bytes.length);
                stream
                    .write(outBytes)
                    .whenComplete((res, err) -> {
                      if (err == null) {
                        logger.info("✅ Echoed response to client on bidi stream {}", stream.streamId());
                      } else {
                        logger.error("❌ Failed to echo to client on bidi stream {}", stream.streamId(), err);
                      }
                    });
              } else {
                // Already prefixed this stream, just echo the raw chunk
                stream.write(bytes);
              }
            } else {
              // Echo an ACK back via a NEW Server-to-Client Unidirectional stream
              session
                  .createUniStream()
                  .thenAccept(ackStream -> {
                    byte[] prefixBytes = "ACK UNI: ".getBytes(StandardCharsets.UTF_8);
                    byte[] outBytes = new byte[prefixBytes.length + bytes.length];
                    System.arraycopy(prefixBytes, 0, outBytes, 0, prefixBytes.length);
                    System.arraycopy(bytes, 0, outBytes, prefixBytes.length, bytes.length);
                    ackStream.write(outBytes).thenRun(ackStream::close);
                  });
            }
          };

          if (prefixCheck.startsWith("SleepServer_")) {
            if (!delayAdmissionSemaphore.tryAcquire()) {
              logger.warn(
                  "⚠️ Delay admission limit reached (max {}). Dropping delayed chunk on stream {}.",
                  maxDelayedTasks,
                  stream.streamId());
              stream.close();
              return;
            }
            logger.info(
                "😴 Server received Sleep command on stream {}. Scheduling non-blocking delayed task...",
                stream.streamId());
            try {
              delayScheduler.schedule(
                  () -> {
                    try {
                      logger.info(
                          "⏰ Server executing delayed response on stream {}.", stream.streamId());
                      process.run();
                    } catch (Throwable t) {
                      logger.error(
                          "❌ Error executing delayed response on stream {}", stream.streamId(), t);
                    } finally {
                      delayAdmissionSemaphore.release();
                    }
                  },
                  3000,
                  TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
              delayAdmissionSemaphore.release();
              logger.error(
                  "❌ Delayed task rejected by scheduler on stream {}", stream.streamId(), e);
              stream.close();
            }
          } else {
            process.run();
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
        .thenAccept(ackStream -> {
          ackStream.writeText(replyText).thenRun(ackStream::close);
        });
  }
}
