package io.github.webtransport4j.example.quarkus;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientTestSuite;
import io.github.webtransport4j.quarkus.QuarkusWebTransportManager;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Quarkus / CDI application demonstrating WebTransport4J endpoint discovery (@WebTransportEndpoint),
 * non-blocking Quarkus lifecycle management (StartupEvent / ShutdownEvent), and automated client interactions.
 *
 * <p>Run in a single command:
 * <pre>
 * mvn compile exec:java -Dexec.mainClass="io.github.webtransport4j.example.quarkus.QuarkusWebTransportSampleApp"
 * </pre>
 */
public class QuarkusWebTransportSampleApp {

  private static final Logger logger = LoggerFactory.getLogger(QuarkusWebTransportSampleApp.class);

  /** CDI Endpoint annotated with @WebTransportEndpoint. */
  @WebTransportEndpoint(path = "/quarkus-echo")
  public static class QuarkusEchoEndpoint implements WebTransportHandler {

    @Override
    public void onSessionReady(WebTransportSession session) {
      logger.info("⚡ [Quarkus Endpoint] Client session established: {}", session.path());

      // Initiate server-to-client unidirectional stream
      session.createUniStream().thenAccept(stream -> {
        stream.writeText("Hello from Server-Initiated Unidirectional Stream! [ID: " + stream.streamId() + "]");
      });

      // Initiate server-to-client bidirectional stream
      session.createBiStream().thenAccept(stream -> {
        stream.onData(data -> {
          logger.info("  📩 Response on server-initiated bidi stream {}: {}",
              stream.streamId(), new String(data.readBytes(), StandardCharsets.UTF_8));
        });
        stream.writeText("Hello from Server-Initiated Bidirectional Stream! [ID: " + stream.streamId() + "]");
      });
    }

    @Override
    public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
      boolean isBidi = stream.isBidirectional();
      stream.onData(buffer -> {
        byte[] bytes = buffer.readBytes();
        String content = new String(bytes, StandardCharsets.UTF_8);
        logger.info("⚡ [Quarkus Endpoint] Incoming stream data: {}", content);

        if (content.startsWith("SleepServer_")) {
          logger.info("😴 Server sleeping on stream {}...", stream.streamId());
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          logger.info("⏰ Server awake on stream {}!", stream.streamId());
        }

        if (isBidi) {
          if (!stream.hasAttribute("prefixed")) {
            stream.setAttribute("prefixed", true);
            stream.writeText("ACK BI: " + content);
          } else {
            stream.write(bytes);
          }
        } else {
          session.createUniStream().thenAccept(ackStream -> {
            ackStream.writeText("ACK UNI: " + content).thenRun(ackStream::close);
          });
        }
      });
    }

    @Override
    public void onDatagramReceived(WebTransportSession session, WebTransportBuffer data) {
      byte[] payload = data.readBytes();
      String content = new String(payload, StandardCharsets.UTF_8);
      logger.info("⚡ [Quarkus Endpoint] Datagram payload: {}", content);
      session.createUniStream().thenAccept(ackStream -> {
        ackStream.writeText("ACK DG: " + content).thenRun(ackStream::close);
      });
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("==========================================================");
    logger.info("⚡ Starting Quarkus / CDI WebTransport Sample Application...");
    logger.info("==========================================================");

    File keyFile = new File("localhost-key.pem");
    File certFile = new File("localhost.pem");

    String keyPath;
    String certPath;
    if (keyFile.exists() && certFile.exists()) {
      keyPath = keyFile.getAbsolutePath();
      certPath = certFile.getAbsolutePath();
    } else {
      SelfSignedCertificate cert = new SelfSignedCertificate();
      keyPath = cert.privateKey().getAbsolutePath();
      certPath = cert.certificate().getAbsolutePath();
    }

    // Simulate CDI discovery of @WebTransportEndpoint annotated beans
    QuarkusEchoEndpoint endpointBean = new QuarkusEchoEndpoint();

    WebTransportServerBuilder builder = WebTransportServer.builder()
        .port(8444)
        .ssl(keyPath, certPath)
        .allowedOrigins("*")
        .transportType("auto");

    QuarkusWebTransportManager manager = QuarkusWebTransportManager.create(
        builder,
        Collections.singletonList(endpointBean)
    );

    // Simulate Quarkus StartupEvent observer
    manager.onStartup();

    WebTransportServer server = manager.getServer();
    logger.info("✅ Quarkus WebTransport Server listening on port {}", server.getPort());
    logger.info("📡 Endpoint registered for path: /quarkus-echo");

    Thread.sleep(500);

    // Run WebTransport Client test suite against the Quarkus server endpoint
    String clientUrl = "https://127.0.0.1:" + server.getPort() + "/quarkus-echo";
    logger.info("==========================================================");
    logger.info("🧪 Launching WebTransport Client Connecting to {}", clientUrl);
    logger.info("==========================================================");
    WebTransportClientTestSuite.main(clientUrl);

    logger.info("==========================================================");
    logger.info("🎉 Quarkus WebTransport Server + Client verification completed successfully!");
    logger.info("==========================================================");

    // Simulate Quarkus ShutdownEvent observer
    manager.onShutdown();
    logger.info("👋 Quarkus WebTransport Manager shut down cleanly.");
    System.exit(0);
  }
}
