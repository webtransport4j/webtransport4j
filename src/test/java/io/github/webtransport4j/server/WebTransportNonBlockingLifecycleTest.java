package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import org.junit.Assert;
import org.junit.Test;

/** Test for non-blocking server lifecycle and ephemeral port resolution. */
public class WebTransportNonBlockingLifecycleTest {

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
}
