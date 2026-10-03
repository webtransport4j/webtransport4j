package io.github.webtransport4j.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Integration test for coordinated server draining and graceful shutdown.
 */
public class WebTransportServerLifecycleDrainTest {

  @Test
  public void testServerDrainTransition() throws Exception {
    WebTransportServer server =
        new WebTransportServerBuilder()
            .port(0)
            .build();

    try {
      server.start();
      assertTrue(server.isRunning());
      assertFalse(server.isDraining());

      server.drain(500, TimeUnit.MILLISECONDS);

      assertTrue(server.isDraining());
      assertFalse(server.isRunning());
    } finally {
      server.close();
    }
  }
}
