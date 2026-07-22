package io.github.webtransport4j.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientTestSuite;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end unmocked integration test verifying live TLS certificate hot-reloading:
 * 1. Starts a real WebTransportServer using cert1.
 * 2. Overwrites cert files on disk with fresh cert2 while server is running.
 * 3. Verifies watcher reloads active QuicSslContext.
 * 4. Runs full WebTransport client test suite against the hot-reloaded server to verify end-to-end handshake & stream/datagram data exchange.
 */
public class TlsHotReloadIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(TlsHotReloadIntegrationTest.class);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  public static class TestEchoEndpoint implements WebTransportHandler {
    @Override
    public void onSessionReady(WebTransportSession session) {
      session.createUniStream().thenAccept(stream -> {
        stream.writeText("Hello from Server-Initiated Unidirectional Stream! [ID: " + stream.streamId() + "]");
      });
      session.createBiStream().thenAccept(stream -> {
        stream.onData(data -> {});
        stream.writeText("Hello from Server-Initiated Bidirectional Stream! [ID: " + stream.streamId() + "]");
      });
    }

    @Override
    public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
      boolean isBidi = stream.isBidirectional();
      stream.onData(buffer -> {
        byte[] bytes = buffer.readBytes();
        String content = new String(bytes, StandardCharsets.UTF_8);
        if (content.startsWith("SleepServer_")) {
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
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
      session.createUniStream().thenAccept(ackStream -> {
        ackStream.writeText("ACK DG: " + content).thenRun(ackStream::close);
      });
    }
  }

  @Test
  public void testUnmockedRealLifeTlsHotReloadIntegration() throws Exception {
    // Step 1: Generate initial certificate 1
    SelfSignedCertificate cert1 = new SelfSignedCertificate("localhost");
    File keyFile = tempFolder.newFile("key.pem");
    File certFile = tempFolder.newFile("cert.pem");

    Files.copy(cert1.privateKey().toPath(), keyFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(cert1.certificate().toPath(), certFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    WebTransportServer server = WebTransportServer.builder()
        .port(0) // dynamic ephemeral port
        .ssl(keyFile.getAbsolutePath(), certFile.getAbsolutePath())
        .allowedOrigins("*")
        .handler("/", new TestEchoEndpoint())
        .build();

    server.start();
    int boundPort = server.getPort();
    QuicSslContext initialSslCtx = server.getActiveSslContext();
    assertNotNull(initialSslCtx);
    logger.info("✅ Server running on port {}", boundPort);

    try {
      // Step 2: Overwrite cert files on disk with cert2 while server is active
      Thread.sleep(1100);

      SelfSignedCertificate cert2 = new SelfSignedCertificate("localhost");
      Files.copy(cert2.privateKey().toPath(), keyFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.copy(cert2.certificate().toPath(), certFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      keyFile.setLastModified(System.currentTimeMillis());
      certFile.setLastModified(System.currentTimeMillis());

      // Wait for watcher or trigger check to pick up and reload QuicSslContext
      boolean reloaded = false;
      for (int i = 0; i < 30; i++) {
        if (server.checkAndReloadTlsCertificates() || server.getActiveSslContext() != initialSslCtx) {
          reloaded = true;
          break;
        }
        Thread.sleep(200);
      }
      assertTrue(reloaded);
      logger.info("✅ Verified active QuicSslContext was swapped in memory.");

      // Step 3: Run full unmocked WebTransport client test suite against the live hot-reloaded server
      String serverUrl = "https://127.0.0.1:" + boundPort + "/";
      logger.info("🧪 Launching WebTransport client against hot-reloaded server: {}", serverUrl);
      WebTransportClientTestSuite.main(serverUrl);

      logger.info("🎉 Unmocked TLS Hot-Reload Integration Test PASSED 100%!");
    } finally {
      server.stop();
    }
  }
}
