package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

      Assertions.assertTrue(server1.isStarted(), "Server 1 should be started");
      Assertions.assertTrue(server2.isStarted(), "Server 2 should be started");

      int port1 = server1.getPort();
      int port2 = server2.getPort();

      Assertions.assertTrue(port1 > 0, "Port 1 should be > 0");
      Assertions.assertTrue(port2 > 0, "Port 2 should be > 0");
      Assertions.assertNotEquals(port1, port2, "Servers should bind to distinct ports");

      GlobalTrafficShapingHandler shaper1 = server1.getTrafficShaper();
      GlobalTrafficShapingHandler shaper2 = server2.getTrafficShaper();

      Assertions.assertNotNull(shaper1, "Server 1 should have a traffic shaper");
      Assertions.assertNotNull(shaper2, "Server 2 should have a traffic shaper");
      Assertions.assertNotSame(shaper1, shaper2, "Servers should have distinct traffic shaper instances");

      // Stop server 1
      server1.stop();

      Assertions.assertFalse(server1.isStarted(), "Server 1 should be stopped");
      Assertions.assertNull(server1.getTrafficShaper(), "Server 1 shaper should be released and cleared");

      // Server 2 must remain completely intact and running
      Assertions.assertTrue(server2.isStarted(), "Server 2 should still be started after Server 1 stops");
      Assertions.assertTrue(server2.isRunning(), "Server 2 should still be running after Server 1 stops");
      Assertions.assertNotNull(server2.getTrafficShaper(), "Server 2 traffic shaper should remain intact");
      Assertions.assertSame(shaper2, server2.getTrafficShaper(), "Server 2 traffic shaper should be the original instance");
    } finally {
      server1.stop();
      server2.stop();
    }

    Assertions.assertFalse(server1.isStarted(), "Server 1 should be stopped");
    Assertions.assertFalse(server2.isStarted(), "Server 2 should be stopped");
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
      Assertions.assertSame(replacement, server.getTrafficShaper());
      server.start();
      GlobalTrafficShapingHandler rejected = customShaper;
      Assertions.assertThrows(IllegalStateException.class, () -> server.setTrafficShaper(rejected));
      Assertions.assertThrows(IllegalStateException.class, () -> server.setTrafficShaper(null));
      Assertions.assertSame(replacement, server.getTrafficShaper());
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
      Assertions.assertThrows(IllegalStateException.class, builder::build);
      Assertions.assertThrows(
          IllegalStateException.class,
          () -> WebTransportServer.builder().trafficShaper(shaper).build());
      Assertions.assertThrows(IllegalStateException.class, () -> other.setTrafficShaper(shaper));
      Assertions.assertNull(other.getTrafficShaper());
      Assertions.assertSame(shaper, owner.getTrafficShaper());
      Assertions.assertEquals(1, executor.getQueue().size(), "Rejected ownership transfers must leave the owner's timer active");
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
      Assertions.assertThrows(IllegalStateException.class, server::start);
      Assertions.assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      // Injected shaper survives restartable stop() so server can be retried
      Assertions.assertSame(shaper, server.getTrafficShaper());
      Assertions.assertFalse(executor.getQueue().isEmpty(), "Traffic counter must remain active across restartable stop");

      // Terminal close must release and clear the injected shaper
      server.close();
      Assertions.assertEquals(WebTransportServer.ServerState.CLOSED, server.getState());
      Assertions.assertNull(server.getTrafficShaper());
      Assertions.assertTrue(executor.getQueue().isEmpty(), "Terminal close must cancel the traffic counter");
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
      Assertions.assertThrows(IllegalStateException.class, server::start);
      Assertions.assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      Assertions.assertNull(server.getTrafficShaper());
    } finally {
      server.stop();
    }
  }
}
