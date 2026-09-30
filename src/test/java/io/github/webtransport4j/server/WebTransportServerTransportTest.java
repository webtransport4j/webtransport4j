package io.github.webtransport4j.server;

import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for web transport server transport. */
public class WebTransportServerTransportTest {

  private WebTransportServer server;

  /** Sets up test fixtures. */
  @Before
  public void setUp() {
    server = new WebTransportServer();
    // Register a dummy handler so the server can start
    server.registerHandler("/test", new WebTransportHandler() {});
  }

  /** Cleans up test fixtures. */
  @After
  public void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  private void startServerAsyncAndVerify() throws Exception {
    Thread t =
        new Thread(
            () -> {
              try {
                server.start();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
    t.start();

    long timeout = System.currentTimeMillis() + 15000;
    while (server.getPort() == 0 && System.currentTimeMillis() < timeout) {
      Thread.sleep(50);
    }
    assertTrue("Server should start successfully", server.getPort() > 0);
    server.stop();
    t.join(2000);
  }

  @Test
  public void testAutoTransport() throws Exception {
    System.setProperty("webtransport4j.server.transport", "auto");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      startServerAsyncAndVerify();
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testNioTransport() throws Exception {
    System.setProperty("webtransport4j.server.transport", "nio");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      startServerAsyncAndVerify();
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testEpollTransport() throws Exception {
    System.setProperty("webtransport4j.server.transport", "epoll");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      if (isEpollAvailable()) {
        startServerAsyncAndVerify();
      } else {
        IllegalStateException ex =
            org.junit.Assert.assertThrows(IllegalStateException.class, server::start);
        assertTrue(
            "Expected explicit epoll unavailability message",
            ex.getMessage() != null
                && (ex.getMessage().contains("Epoll transport was explicitly requested")
                    || ex.getMessage().contains("could not be initialized")));
      }
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testKqueueTransport() throws Exception {
    System.setProperty("webtransport4j.server.transport", "kqueue");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      if (isKqueueAvailable()) {
        startServerAsyncAndVerify();
      } else {
        IllegalStateException ex =
            org.junit.Assert.assertThrows(IllegalStateException.class, server::start);
        assertTrue(
            "Expected explicit kqueue unavailability message",
            ex.getMessage() != null
                && (ex.getMessage().contains("KQueue transport was explicitly requested")
                    || ex.getMessage().contains("could not be initialized")));
      }
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testIoUringTransport() throws Exception {
    System.setProperty("webtransport4j.server.transport", "iouring");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      if (isIouringAvailable()) {
        startServerAsyncAndVerify();
      } else {
        IllegalStateException ex =
            org.junit.Assert.assertThrows(IllegalStateException.class, server::start);
        assertTrue(
            "Expected explicit iouring unavailability message",
            ex.getMessage() != null
                && (ex.getMessage().contains("IOUring transport was explicitly requested")
                    || ex.getMessage().contains("could not be initialized")));
      }
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  private static boolean isEpollAvailable() {
    try {
      Class<?> clazz = Class.forName("io.netty.channel.epoll.Epoll");
      return (boolean) clazz.getMethod("isAvailable").invoke(null);
    } catch (Throwable t) {
      return false;
    }
  }

  private static boolean isKqueueAvailable() {
    try {
      Class<?> clazz = Class.forName("io.netty.channel.kqueue.KQueue");
      return (boolean) clazz.getMethod("isAvailable").invoke(null);
    } catch (Throwable t) {
      return false;
    }
  }

  private static boolean isIouringAvailable() {
    try {
      Class<?> clazz = Class.forName("io.netty.channel.uring.IOUring");
      return (boolean) clazz.getMethod("isAvailable").invoke(null);
    } catch (Throwable t) {
      return false;
    }
  }
}
