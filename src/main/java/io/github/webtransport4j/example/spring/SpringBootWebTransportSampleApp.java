package io.github.webtransport4j.example.spring;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientTestSuite;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.spring.WebTransportAutoConfiguration;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Sample Spring Boot application demonstrating WebTransport4J server auto-configuration,
 * declarative endpoint mapping (@WebTransportEndpoint), and automated client interactions.
 *
 * <p>Run in a single command:
 * <pre>
 * mvn compile exec:java -Dexec.mainClass="io.github.webtransport4j.example.spring.SpringBootWebTransportSampleApp"
 * </pre>
 */
@SpringBootApplication
@Import(WebTransportAutoConfiguration.class)
public class SpringBootWebTransportSampleApp {

  private static final Logger logger = LoggerFactory.getLogger(SpringBootWebTransportSampleApp.class);

  /** Declarative WebTransport Endpoint Bean registered in Spring Context. */
  @WebTransportEndpoint(path = "/spring-echo")
  @Component
  public static class SpringEchoEndpoint implements WebTransportHandler {

    @Override
    public void onSessionReady(WebTransportSession session) {
      logger.info("🌱 [Spring Endpoint] Client connected to path: {}", session.path());

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
        logger.info("🌱 [Spring Endpoint] Received on stream {}: {}", stream.streamId(), content);

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
      logger.info("🌱 [Spring Endpoint] Datagram received: {}", content);
      session.createUniStream().thenAccept(ackStream -> {
        ackStream.writeText("ACK DG: " + content).thenRun(ackStream::close);
      });
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("==========================================================");
    logger.info("🚀 Starting Spring Boot WebTransport Sample Application...");
    logger.info("==========================================================");

    File keyFile = new File("localhost-key.pem");
    File certFile = new File("localhost.pem");
    if (keyFile.exists() && certFile.exists()) {
      System.setProperty("webtransport4j.ssl-key-path", keyFile.getAbsolutePath());
      System.setProperty("webtransport4j.ssl-cert-path", certFile.getAbsolutePath());
    } else {
      SelfSignedCertificate cert = new SelfSignedCertificate();
      System.setProperty("webtransport4j.ssl-key-path", cert.privateKey().getAbsolutePath());
      System.setProperty("webtransport4j.ssl-cert-path", cert.certificate().getAbsolutePath());
    }

    System.setProperty("webtransport4j.port", "8443");
    System.setProperty("webtransport4j.allowed-origins", "*");
    System.setProperty("webtransport4j.transport", "auto");

    // Launch Spring Application Context
    ConfigurableApplicationContext context = SpringApplication.run(SpringBootWebTransportSampleApp.class, args);

    WebTransportServer server = context.getBean(WebTransportServer.class);
    logger.info("✅ Spring Boot WebTransport Server running on port {}", server.getPort());
    logger.info("📡 Registered Path Handler for: /spring-echo");

    Thread.sleep(500);

    // Run WebTransport Client test suite against the Spring Boot server endpoint
    String clientUrl = "https://127.0.0.1:" + server.getPort() + "/spring-echo";
    logger.info("==========================================================");
    logger.info("🧪 Launching WebTransport Client Connecting to {}", clientUrl);
    logger.info("==========================================================");
    WebTransportClientTestSuite.main(clientUrl);

    logger.info("==========================================================");
    logger.info("🎉 Spring Boot Server + Client verification completed successfully!");
    logger.info("==========================================================");

    // Gracefully shutdown application context
    context.close();
    logger.info("👋 Spring Boot WebTransport Sample Application shut down cleanly.");
    System.exit(0);
  }
}
