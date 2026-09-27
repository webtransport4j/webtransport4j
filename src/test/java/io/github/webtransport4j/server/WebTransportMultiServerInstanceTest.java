package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests concurrency, state isolation, and lifecycle hygiene across multiple
 * {@link WebTransportServer} instances running within the same JVM process.
 */
public class WebTransportMultiServerInstanceTest {

  @Test
  public void testConcurrentServersIsolationAndIndependentLifecycle() throws Exception {
    // Server 1 with 50 KB/s limits
    WebTransportServer server1 =
        WebTransportServer.builder()
            .port(0)
            .globalTrafficLimits(50000L, 50000L)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    // Server 2 with 100 KB/s limits
    WebTransportServer server2 =
        WebTransportServer.builder()
            .port(0)
            .globalTrafficLimits(100000L, 100000L)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    try {
      server1.start();
      server2.start();

      Assert.assertTrue("Server 1 should be started", server1.isStarted());
      Assert.assertTrue("Server 2 should be started", server2.isStarted());

      int port1 = server1.getPort();
      int port2 = server2.getPort();

      Assert.assertTrue("Port 1 should be > 0", port1 > 0);
      Assert.assertTrue("Port 2 should be > 0", port2 > 0);
      Assert.assertNotEquals("Servers should bind to distinct ports", port1, port2);

      GlobalTrafficShapingHandler shaper1 = server1.getTrafficShaper();
      GlobalTrafficShapingHandler shaper2 = server2.getTrafficShaper();

      Assert.assertNotNull("Server 1 should have a traffic shaper", shaper1);
      Assert.assertNotNull("Server 2 should have a traffic shaper", shaper2);
      Assert.assertNotSame("Servers should have distinct traffic shaper instances", shaper1, shaper2);

      // Stop server 1
      server1.stop();

      Assert.assertFalse("Server 1 should be stopped", server1.isStarted());
      Assert.assertNull("Server 1 shaper should be released and cleared", server1.getTrafficShaper());

      // Server 2 must remain completely intact and running
      Assert.assertTrue("Server 2 should still be started after Server 1 stops", server2.isStarted());
      Assert.assertTrue("Server 2 should still be running after Server 1 stops", server2.isRunning());
      Assert.assertNotNull("Server 2 traffic shaper should remain intact", server2.getTrafficShaper());
      Assert.assertSame("Server 2 traffic shaper should be the original instance", shaper2, server2.getTrafficShaper());
    } finally {
      server1.stop();
      server2.stop();
    }

    Assert.assertFalse("Server 1 should be stopped", server1.isStarted());
    Assert.assertFalse("Server 2 should be stopped", server2.isStarted());
  }

  @Test
  public void testCustomTrafficShaperInjection() throws Exception {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    server.start();
    try {
      Assert.assertNull("No traffic shaper configured by default", server.getTrafficShaper());

      GlobalTrafficShapingHandler customShaper =
          new GlobalTrafficShapingHandler(new io.netty.channel.nio.NioEventLoopGroup(1), 2000L, 2000L);
      try {
        server.setTrafficShaper(customShaper);
        Assert.assertSame(customShaper, server.getTrafficShaper());
      } finally {
        customShaper.release();
      }
    } finally {
      server.stop();
    }
  }
}
