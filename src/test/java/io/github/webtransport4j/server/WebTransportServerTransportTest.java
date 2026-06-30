package io.github.webtransport4j.server;

import static org.junit.Assert.assertTrue;

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
    server.registerHandler("/test", new io.github.webtransport4j.api.WebTransportHandler() {});
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
  public void testEpollTransportFallback() throws Exception {
    System.setProperty("webtransport4j.server.transport", "epoll");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      startServerAsyncAndVerify();
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testKqueueTransportFallback() throws Exception {
    System.setProperty("webtransport4j.server.transport", "kqueue");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      startServerAsyncAndVerify();
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }

  @Test
  public void testIoUringTransportFallback() throws Exception {
    System.setProperty("webtransport4j.server.transport", "iouring");
    System.setProperty("webtransport4j.server.port", "0"); // Use random port
    try {
      startServerAsyncAndVerify();
    } finally {
      System.clearProperty("webtransport4j.server.transport");
      System.clearProperty("webtransport4j.server.port");
    }
  }
}
