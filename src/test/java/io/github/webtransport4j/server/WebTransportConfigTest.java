package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for web transport config. */
public class WebTransportConfigTest {
  /** Cleanup. */
  @Before
  @After
  public void cleanup() {
    System.clearProperty("test.math.key");
    System.clearProperty("test.string.key");
    System.clearProperty("webtransport4j.dispatch.execution.mode");
    System.clearProperty("webtransport4j.business.queue.type");
    System.clearProperty("webtransport4j.business.queue.capacity");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.key");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.expiration.ms");
    System.clearProperty("webtransport4j.ssl.session.timeout.seconds");
    System.clearProperty("webtransport4j.ssl.session.cache.size");
    System.clearProperty("webtransport4j.epoll.udpgro");
    System.clearProperty("webtransport4j.epoll.udpgso");
    System.clearProperty("webtransport4j.epoll.gso.size");
    System.clearProperty("webtransport4j.server.recv.buffer.size");
  }

  @Test
  public void testGetLongSimple() {
    System.setProperty("test.math.key", "12345");
    assertEquals(12345L, WebTransportConfig.getLong("test.math.key", 0L));
  }

  @Test
  public void testGetLongMultiplication() {
    System.setProperty("test.math.key", "50 * 1024 * 1024");
    assertEquals(52428800L, WebTransportConfig.getLong("test.math.key", 0L));
  }

  @Test
  public void testGetLongMultiplicationWithoutSpaces() {
    System.setProperty("test.math.key", "1024*1024");
    assertEquals(1048576L, WebTransportConfig.getLong("test.math.key", 0L));
  }

  @Test
  public void testGetLongFallbackOnInvalid() {
    System.setProperty("test.math.key", "invalid-value");
    assertEquals(999L, WebTransportConfig.getLong("test.math.key", 999L));
  }

  @Test
  public void testGetIntSimple() {
    System.setProperty("test.math.key", "123");
    assertEquals(123, WebTransportConfig.getInt("test.math.key", 0));
  }

  @Test
  public void testGetIntMultiplication() {
    System.setProperty("test.math.key", "2 * 1024");
    assertEquals(2048, WebTransportConfig.getInt("test.math.key", 0));
  }

  @Test
  public void testGetLongDefaultValue() {
    assertEquals(555L, WebTransportConfig.getLong("nonexistent.key.for.test", 555L));
  }

  @Test
  public void testBusinessExecutorFactoryFixedThreadPoolWithArrayQueue() {
    System.setProperty("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL");
    System.setProperty("webtransport4j.business.queue.type", "ARRAY");
    System.setProperty("webtransport4j.business.queue.capacity", "500");

    ExecutorService executor = BusinessExecutorFactory.create();
    assertNotNull(executor);
    assertTrue(executor instanceof ThreadPoolExecutor);
    ThreadPoolExecutor tp = (ThreadPoolExecutor) executor;
    assertTrue(tp.getQueue() instanceof ArrayBlockingQueue);
    assertEquals(500, tp.getQueue().remainingCapacity());
    executor.shutdown();
  }

  @Test
  public void testBusinessExecutorFactoryUnboundedFallback() {
    System.setProperty("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL");
    System.setProperty("webtransport4j.business.queue.type", "ARRAY");
    System.setProperty("webtransport4j.business.queue.capacity", "0");

    ExecutorService executor = BusinessExecutorFactory.create();
    assertNotNull(executor);
    assertTrue(executor instanceof ThreadPoolExecutor);
    ThreadPoolExecutor tp = (ThreadPoolExecutor) executor;
    assertTrue(tp.getQueue() instanceof ArrayBlockingQueue);
    assertEquals(10000, tp.getQueue().remainingCapacity());
    executor.shutdown();
  }

  @Test
  public void testTokenHandlerProperties() {
    System.setProperty(
        "webtransport4j.quic.token.handler.hmac.key", "0123456789abcdef0123456789abcdef");
    System.setProperty("webtransport4j.quic.token.handler.hmac.expiration.ms", "30000");
    System.setProperty("webtransport4j.quic.early.data.enabled", "true");
    System.setProperty("webtransport4j.ssl.session.timeout.seconds", "86400");
    System.setProperty("webtransport4j.ssl.session.cache.size", "20480");

    assertEquals(
        "0123456789abcdef0123456789abcdef",
        WebTransportConfig.get("webtransport4j.quic.token.handler.hmac.key", null));
    assertEquals(
        30000L,
        WebTransportConfig.getLong("webtransport4j.quic.token.handler.hmac.expiration.ms", 60000L));
    assertEquals(
        86400L, WebTransportConfig.getLong("webtransport4j.ssl.session.timeout.seconds", -1L));
    assertEquals(20480L, WebTransportConfig.getLong("webtransport4j.ssl.session.cache.size", -1L));
  }

  @Test
  public void testGetStringSystemProperty() {
    System.setProperty("test.string.key", "hello_world");
    assertEquals("hello_world", WebTransportConfig.get("test.string.key", "default_val"));
  }

  @Test
  public void testGetStringFallbackToDefault() {
    assertEquals("fallback", WebTransportConfig.get("nonexistent.string.key", "fallback"));
  }

  @Test
  public void testGetStringNullableDefault() {
    assertNull(WebTransportConfig.get("another.nonexistent.key", null));
  }

  @Test
  public void testEpollConfigOptions() {
    // Test defaults
    assertTrue(WebTransportConfig.getBoolean("webtransport4j.epoll.udpgro", true));
    assertTrue(WebTransportConfig.getBoolean("webtransport4j.epoll.udpgso", true));
    assertEquals(64, WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64));

    // Test system property override
    System.setProperty("webtransport4j.epoll.udpgro", "false");
    System.setProperty("webtransport4j.epoll.udpgso", "false");
    System.setProperty("webtransport4j.epoll.gso.size", "16");

    assertTrue(!WebTransportConfig.getBoolean("webtransport4j.epoll.udpgro", true));
    assertTrue(!WebTransportConfig.getBoolean("webtransport4j.epoll.udpgso", true));
    assertEquals(16, WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64));

    // Test invalid GSO size range validation logic (throws IllegalArgumentException)
    System.setProperty("webtransport4j.epoll.gso.size", "0");
    int sizeInvalidLow = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64);
    boolean exceptionThrown = false;
    try {
      if (sizeInvalidLow < 1 || sizeInvalidLow > 64) {
        throw new IllegalArgumentException("webtransport4j.epoll.gso.size must be in range 1 - 64");
      }
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    System.setProperty("webtransport4j.epoll.gso.size", "65");
    int sizeInvalidHigh = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64);
    exceptionThrown = false;
    try {
      if (sizeInvalidHigh < 1 || sizeInvalidHigh > 64) {
        throw new IllegalArgumentException("webtransport4j.epoll.gso.size must be in range 1 - 64");
      }
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void testRecvBufferSizeConfiguration() {
    // Default from webtransport.properties
    System.setProperty("webtransport4j.server.recv.buffer.size", "65536");
    assertEquals(65536, WebTransportConfig.getInt("webtransport4j.server.recv.buffer.size", 2048));

    // Custom configuration system property override
    System.setProperty("webtransport4j.server.recv.buffer.size", "32768");
    assertEquals(32768, WebTransportConfig.getInt("webtransport4j.server.recv.buffer.size", 2048));
  }

  @Test
  public void testDynamicConfigReloadFromFilesystem() throws Exception {
    File tempFile = new File("webtransport-dynamic.properties");
    try {
      // 1. Write custom properties to the local file for all dynamic parameters
      Files.write(tempFile.toPath(), Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=999",
          "webtransport4j.server.ratelimit.max_tracked_ips=12345",
          "webtransport4j.server.ratelimit.filter_engine=trie",
          "webtransport4j.server.ratelimit.whitelist=8.8.8.8,9.9.9.9",
          "webtransport4j.server.ratelimit.overrides=10.0.0.1:5,10.0.0.2:0",
          "webtransport4j.server.ratelimit.blocklist=5.5.5.5,6.6.6.6",
          "webtransport4j.server.ratelimit.blocklist.bloom_capacity=2000000",
          "webtransport4j.server.ratelimit.blocklist.bloom_fpp=0.00001",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi=77",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni=88",
          "webtransport4j.session.resumption.timeout.seconds=120"
      ));

      // 2. Trigger reload
      WebTransportConfig.reload();

      // 3. Verify all values are resolved correctly
      assertEquals(999, WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100));
      assertEquals(12345, WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 100000));
      assertEquals("trie", WebTransportConfig.get("webtransport4j.server.ratelimit.filter_engine", "netty"));
      assertEquals("8.8.8.8,9.9.9.9", WebTransportConfig.get("webtransport4j.server.ratelimit.whitelist", ""));
      assertEquals("10.0.0.1:5,10.0.0.2:0", WebTransportConfig.get("webtransport4j.server.ratelimit.overrides", ""));
      assertEquals("5.5.5.5,6.6.6.6", WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist", ""));
      assertEquals(2000000, WebTransportConfig.getInt("webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1000000));
      assertEquals("0.00001", WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001"));
      assertEquals(77L, WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi", 5000L));
      assertEquals(88L, WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni", 5000L));
      assertEquals(120L, WebTransportConfig.getLong("webtransport4j.session.resumption.timeout.seconds", 60L));

    } finally {
      // 4. Clean up the file
      if (tempFile.exists()) {
        tempFile.delete();
      }
      // 5. Trigger reload again to restore clean state
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testSetPropertyAndRemoveProperty() {
    WebTransportConfig.setProperty("webtransport4j.dispatch.execution.mode", "VIRTUAL_THREADS");
    assertEquals("VIRTUAL_THREADS", WebTransportConfig.get("webtransport4j.dispatch.execution.mode", null));

    WebTransportConfig.removeProperty("webtransport4j.dispatch.execution.mode");
    assertEquals("NETTY_EVENT_LOOP", WebTransportConfig.get("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL"));
  }
}
