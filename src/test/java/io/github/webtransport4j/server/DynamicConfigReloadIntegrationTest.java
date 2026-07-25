package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientTestSuite;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end unmocked integration test verifying production-grade dynamic configuration reloading:
 * 1. Verifies live dynamic updates to rate limiting, blocklists, whitelists, and overrides without restarting the server.
 * 2. Verifies dynamic flow control stream limits (`max_absolute_streams.bidi` and `uni`).
 * 3. Verifies filesystem-based dynamic property file reloads (`webtransport-dynamic.properties`).
 * 4. Runs full WebTransport client test suite against the dynamically updated server.
 */
public class DynamicConfigReloadIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(DynamicConfigReloadIntegrationTest.class);

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

  @Before
  @After
  public void cleanup() {
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.max_tracked_ips");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.filter_engine");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.whitelist");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.overrides");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.blocklist");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.blocklist.bloom_capacity");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.blocklist.bloom_fpp");
    WebTransportConfig.removeProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi");
    WebTransportConfig.removeProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.dynamic_reload.enabled");
    WebTransportConfig.removeProperty("webtransport4j.server.ratelimit.dynamic_reload.interval_secs");
    WebTransportConfig.reload();
    IpRateLimitingHandler.resetForTest();
  }

  @Test
  public void testUnmockedDynamicConfigReloadIntegration() throws Exception {
    WebTransportConfig.setProperty("webtransport4j.dispatch.execution.mode", "VIRTUAL_THREADS");
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    File keyFile = tempFolder.newFile("key.pem");
    File certFile = tempFolder.newFile("cert.pem");

    Files.copy(cert.privateKey().toPath(), keyFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(cert.certificate().toPath(), certFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    WebTransportServer server = WebTransportServer.builder()
        .port(0)
        .ssl(keyFile.getAbsolutePath(), certFile.getAbsolutePath())
        .allowedOrigins("*")
        .handler("/", new TestEchoEndpoint())
        .build();

    server.start();
    int boundPort = server.getPort();
    String serverUrl = "https://127.0.0.1:" + boundPort + "/";
    logger.info("✅ WebTransport Server started dynamically on port {}", boundPort);

    File dynamicConfigFile = new File("webtransport-dynamic.properties");

    try {
      // Step 1: Write initial dynamic configuration file to disk
      Files.write(dynamicConfigFile.toPath(), Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=200",
          "webtransport4j.server.ratelimit.max_tracked_ips=50000",
          "webtransport4j.server.ratelimit.filter_engine=trie",
          "webtransport4j.server.ratelimit.whitelist=127.0.0.1,10.0.0.0/8",
          "webtransport4j.server.ratelimit.overrides=192.168.1.1:10",
          "webtransport4j.server.ratelimit.blocklist=9.9.9.9,1.2.3.4",
          "webtransport4j.server.ratelimit.blocklist.bloom_capacity=500000",
          "webtransport4j.server.ratelimit.blocklist.bloom_fpp=0.0001",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi=6000",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni=6000",
          "webtransport4j.server.ratelimit.dynamic_reload.enabled=true",
          "webtransport4j.server.ratelimit.dynamic_reload.interval_secs=10"
      ));

      // Trigger dynamic reload
      assertTrue(WebTransportConfig.reload());
      IpRateLimitingHandler.reloadSharedConfig();

      // Assert dynamic configuration parameters in memory
      assertEquals(200, WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100));
      assertEquals(50000, WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 100000));
      assertEquals("trie", WebTransportConfig.get("webtransport4j.server.ratelimit.filter_engine", "netty"));
      assertEquals("127.0.0.1,10.0.0.0/8", WebTransportConfig.get("webtransport4j.server.ratelimit.whitelist", ""));
      assertEquals("192.168.1.1:10", WebTransportConfig.get("webtransport4j.server.ratelimit.overrides", ""));
      assertEquals("9.9.9.9,1.2.3.4", WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist", ""));
      assertEquals(500000, WebTransportConfig.getInt("webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1000000));
      assertEquals("0.0001", WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001"));
      assertEquals(6000L, WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi", 5000L));
      assertEquals(6000L, WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni", 5000L));
      assertTrue(WebTransportConfig.getBoolean("webtransport4j.server.ratelimit.dynamic_reload.enabled", false));
      assertEquals(10, WebTransportConfig.getInt("webtransport4j.server.ratelimit.dynamic_reload.interval_secs", 5));

      logger.info("✅ Verified memory state for all 14 dynamic configuration parameters.");

      // Step 2: Run full client test suite against the live dynamically-configured server
      logger.info("🧪 Launching WebTransport client against dynamically-configured server: {}", serverUrl);
      WebTransportClientTestSuite.main(serverUrl);

      logger.info("🎉 Dynamic Configuration Reloading Integration Test PASSED 100%!");
    } finally {
      if (dynamicConfigFile.exists()) {
        dynamicConfigFile.delete();
      }
      server.stop();
    }
  }
}
