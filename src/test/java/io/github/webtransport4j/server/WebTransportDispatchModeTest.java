package io.github.webtransport4j.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for web transport dispatch mode. */
public class WebTransportDispatchModeTest {
  /** Sets up test fixtures. */
  @Before
  public void setUp() {
    System.setProperty("webtransport4j.dispatch.execution.mode", "NETTY_EVENT_EXECUTOR_GROUP");
    System.setProperty("webtransport4j.netty.executor.group.size", "8");
    System.setProperty("webtransport4j.server.port", "0"); // random port
  }

  /** Cleans up test fixtures. */
  @After
  public void tearDown() {
    System.clearProperty("webtransport4j.dispatch.execution.mode");
    System.clearProperty("webtransport4j.netty.executor.group.size");
    System.clearProperty("webtransport4j.server.port");
  }

  @Test
  public void testNettyEventExecutorGroupConfig() throws Exception {
    WebTransportServer server = new WebTransportServer(new WebTransportHandler() {});
    try {
      ExecutorService executor = server.getBusinessExecutor();
      assertNotNull("Executor should not be null in NETTY_EVENT_EXECUTOR_GROUP mode", executor);
      assertTrue(
          "Executor should be an instance of DefaultEventExecutorGroup",
          executor instanceof DefaultEventExecutorGroup);
    } finally {
      server.stop();
    }
  }
}
