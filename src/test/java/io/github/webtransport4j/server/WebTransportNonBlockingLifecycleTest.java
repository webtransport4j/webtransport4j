package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test for non-blocking server lifecycle and ephemeral port resolution. */
public class WebTransportNonBlockingLifecycleTest {
  private String previousDispatchMode;
  private String previousPort;

  /**
   * Sets up test environment.
   */
  @Before
  public void setUp() {
    previousDispatchMode = System.getProperty("webtransport4j.dispatch.execution.mode");
    previousPort = System.getProperty("webtransport4j.server.port");
    System.setProperty("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL");
    System.setProperty("webtransport4j.server.port", "0");
  }

  /**
   * Tears down test environment.
   */
  @After
  public void tearDown() {
    restoreProperty("webtransport4j.dispatch.execution.mode", previousDispatchMode);
    restoreProperty("webtransport4j.server.port", previousPort);
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @Test
  public void testNonBlockingStartStopAndEphemeralPort() throws Exception {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0) // Ephemeral port
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assert.assertFalse(server.isStarted());

    // Non-blocking start
    server.start();

    Assert.assertTrue(server.isStarted());
    Assert.assertTrue(server.isRunning());

    int boundPort = server.getPort();
    Assert.assertTrue("Bound port should be > 0 when using ephemeral port 0", boundPort > 0);

    // Stop server
    server.stop();
    Assert.assertFalse(server.isStarted());
  }

  @Test
  public void testStartupFailurePreservesOwnedExecutorForRetry() throws Exception {
    Path directory = Files.createTempDirectory("webtransport-startup-retry");
    Path key = directory.resolve("key.pem");
    Path certificate = directory.resolve("cert.pem");
    WebTransportServer server = WebTransportServer.builder().port(0).transportType("nio")
        .ssl(key.toString(), certificate.toString()).build();
    SelfSignedCertificate tls = null;
    ExecutorService executor = server.getBusinessExecutor();
    try {
      Assert.assertThrows(IllegalStateException.class, server::start);
      Assert.assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      Assert.assertFalse(executor.isShutdown());
      Assert.assertEquals("before retry", executor.submit(() -> "before retry").get(5, TimeUnit.SECONDS));
      tls = new SelfSignedCertificate("localhost");
      Files.copy(tls.privateKey().toPath(), key);
      Files.copy(tls.certificate().toPath(), certificate);
      server.start();
      Assert.assertTrue(server.isStarted());
      Assert.assertSame(executor, server.getBusinessExecutor());
      Assert.assertEquals("after retry", executor.submit(() -> "after retry").get(5, TimeUnit.SECONDS));
    } finally {
      server.stop();
      if (tls != null) {
        tls.delete();
      }
      Files.deleteIfExists(key);
      Files.deleteIfExists(certificate);
      Files.deleteIfExists(directory);
    }
    Assert.assertTrue(executor.isShutdown());
  }

  @Test
  public void testExplicitStopAfterStartupFailureShutsDownOwnedExecutor() {
    WebTransportServer server = WebTransportServer.builder().port(0).transportType("nio")
        .ssl("missing-key.pem", "missing-cert.pem").build();
    try {
      Assert.assertThrows(IllegalStateException.class, server::start);
      Assert.assertFalse(server.getBusinessExecutor().isShutdown());
    } finally {
      server.stop();
    }
    Assert.assertTrue(server.getBusinessExecutor().isShutdown());
  }

  @Test
  public void testCallerExecutorSurvivesFailedStartupAndExplicitStop() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    WebTransportServer failing = WebTransportServer.builder().port(0).transportType("nio")
        .ssl("missing-key.pem", "missing-cert.pem").businessExecutor(executor).build();
    WebTransportServer running = new WebTransportServer(new WebTransportHandler() {}, executor);
    try {
      Assert.assertThrows(IllegalStateException.class, failing::start);
      Assert.assertFalse(executor.isShutdown());
      failing.stop();
      Assert.assertFalse(executor.isShutdown());
      running.start();
      running.stop();
      Assert.assertEquals("still usable", executor.submit(() -> "still usable").get(5, TimeUnit.SECONDS));
    } finally {
      failing.stop();
      running.stop();
      executor.shutdownNow();
    }
  }

}
