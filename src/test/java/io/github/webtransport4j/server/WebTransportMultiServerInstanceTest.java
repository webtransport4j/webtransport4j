package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests concurrency, state isolation, and lifecycle hygiene across multiple {@link
 * WebTransportServer} instances running within the same JVM process.
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
      Assert.assertNotSame(
          "Servers should have distinct traffic shaper instances", shaper1, shaper2);

      // Stop server 1
      server1.stop();

      Assert.assertFalse("Server 1 should be stopped", server1.isStarted());
      Assert.assertNull(
          "Server 1 shaper should be released and cleared", server1.getTrafficShaper());

      // Server 2 must remain completely intact and running
      Assert.assertTrue(
          "Server 2 should still be started after Server 1 stops", server2.isStarted());
      Assert.assertTrue(
          "Server 2 should still be running after Server 1 stops", server2.isRunning());
      Assert.assertNotNull(
          "Server 2 traffic shaper should remain intact", server2.getTrafficShaper());
      Assert.assertSame(
          "Server 2 traffic shaper should be the original instance",
          shaper2,
          server2.getTrafficShaper());
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
        WebTransportServer.builder().port(0).defaultHandler(new WebTransportHandler() {}).build();

    NioEventLoopGroup group = new NioEventLoopGroup(1);
    GlobalTrafficShapingHandler customShaper = null;
    GlobalTrafficShapingHandler replacement = null;
    try {
      customShaper = new GlobalTrafficShapingHandler(group, 2000L, 2000L);
      replacement = new GlobalTrafficShapingHandler(group, 3000L, 3000L);
      server.setTrafficShaper(customShaper);
      server.setTrafficShaper(replacement);
      Assert.assertSame(replacement, server.getTrafficShaper());
      server.start();
      GlobalTrafficShapingHandler rejected = customShaper;
      Assert.assertThrows(IllegalStateException.class, () -> server.setTrafficShaper(rejected));
      Assert.assertThrows(IllegalStateException.class, () -> server.setTrafficShaper(null));
      Assert.assertSame(replacement, server.getTrafficShaper());
    } finally {
      try {
        server.stop();
        if (customShaper != null) {
          customShaper.release();
        }
        if (replacement != null) {
          replacement.release();
        }
      } finally {
        group.shutdownGracefully(0, 5, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
      }
    }
  }

  @Test
  public void testInjectedHandlerCannotBeSharedAcrossServers() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    executor.setRemoveOnCancelPolicy(true);
    GlobalTrafficShapingHandler shaper =
        new GlobalTrafficShapingHandler(executor, 2000L, 2000L, 60000L);
    WebTransportServerBuilder builder = WebTransportServer.builder().trafficShaper(shaper);
    WebTransportServer owner = builder.build();
    WebTransportServer other = WebTransportServer.builder().build();
    try {
      Assert.assertThrows(IllegalStateException.class, builder::build);
      Assert.assertThrows(
          IllegalStateException.class,
          () -> WebTransportServer.builder().trafficShaper(shaper).build());
      Assert.assertThrows(IllegalStateException.class, () -> other.setTrafficShaper(shaper));
      Assert.assertNull(other.getTrafficShaper());
      Assert.assertSame(shaper, owner.getTrafficShaper());
      Assert.assertEquals(
          "Rejected ownership transfers must leave the owner's timer active",
          1,
          executor.getQueue().size());
    } finally {
      shaper.release();
      executor.shutdownNow();
    }
  }

  @Test
  public void testStartupFailurePreservesInjectedHandlerUntilTerminalClose() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    executor.setRemoveOnCancelPolicy(true);
    GlobalTrafficShapingHandler shaper =
        new GlobalTrafficShapingHandler(executor, 2000L, 2000L, 60000L);
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .ssl("missing-key.pem", "missing-cert.pem")
            .trafficShaper(shaper)
            .build();
    try {
      Assert.assertThrows(IllegalStateException.class, server::start);
      Assert.assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      // Injected shaper survives restartable stop() so server can be retried
      Assert.assertSame(shaper, server.getTrafficShaper());
      Assert.assertFalse(
          "Traffic counter must remain active across restartable stop",
          executor.getQueue().isEmpty());

      // Terminal close must release and clear the injected shaper
      server.close();
      Assert.assertEquals(WebTransportServer.ServerState.CLOSED, server.getState());
      Assert.assertNull(server.getTrafficShaper());
      Assert.assertTrue(
          "Terminal close must cancel the traffic counter", executor.getQueue().isEmpty());
    } finally {
      server.close();
      shaper.release();
      executor.shutdownNow();
    }
  }

  @Test
  public void testStartupFailureClearsAutomaticallyCreatedHandler() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .ssl("missing-key.pem", "missing-cert.pem")
            .globalTrafficLimits(2000L, 2000L)
            .build();
    try {
      Assert.assertThrows(IllegalStateException.class, server::start);
      Assert.assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      Assert.assertNull(server.getTrafficShaper());
    } finally {
      server.stop();
    }
  }
}
