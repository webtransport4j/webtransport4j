package io.github.webtransport4j.server;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebTransportConfigTest {

  @Before
  @After
  public void cleanup() {
    System.clearProperty("test.math.key");
    System.clearProperty("webtransport4j.dispatch.execution.mode");
    System.clearProperty("webtransport4j.business.queue.type");
    System.clearProperty("webtransport4j.business.queue.capacity");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.key");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.expiration.ms");
    System.clearProperty("webtransport4j.ssl.session.timeout.seconds");
    System.clearProperty("webtransport4j.ssl.session.cache.size");
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

    java.util.concurrent.ExecutorService executor = BusinessExecutorFactory.create();
    assertNotNull(executor);
    assertTrue(executor instanceof java.util.concurrent.ThreadPoolExecutor);
    java.util.concurrent.ThreadPoolExecutor tp = (java.util.concurrent.ThreadPoolExecutor) executor;
    assertTrue(tp.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue);
    assertEquals(500, tp.getQueue().remainingCapacity());
    executor.shutdown();
  }

  @Test
  public void testBusinessExecutorFactoryUnboundedFallback() {
    System.setProperty("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL");
    System.setProperty("webtransport4j.business.queue.type", "ARRAY");
    System.setProperty("webtransport4j.business.queue.capacity", "0");

    java.util.concurrent.ExecutorService executor = BusinessExecutorFactory.create();
    assertNotNull(executor);
    assertTrue(executor instanceof java.util.concurrent.ThreadPoolExecutor);
    java.util.concurrent.ThreadPoolExecutor tp = (java.util.concurrent.ThreadPoolExecutor) executor;
    assertTrue(tp.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue);
    assertEquals(10000, tp.getQueue().remainingCapacity());
    executor.shutdown();
  }

  @Test
  public void testTokenHandlerProperties() {
    System.setProperty("webtransport4j.quic.token.handler.hmac.key", "0123456789abcdef0123456789abcdef");
    System.setProperty("webtransport4j.quic.token.handler.hmac.expiration.ms", "30000");
    System.setProperty("webtransport4j.quic.early.data.enabled", "true");
    System.setProperty("webtransport4j.ssl.session.timeout.seconds", "86400");
    System.setProperty("webtransport4j.ssl.session.cache.size", "20480");

    assertEquals("0123456789abcdef0123456789abcdef", WebTransportConfig.get("webtransport4j.quic.token.handler.hmac.key", null));
    assertEquals(30000L, WebTransportConfig.getLong("webtransport4j.quic.token.handler.hmac.expiration.ms", 60000L));
    assertEquals(86400L, WebTransportConfig.getLong("webtransport4j.ssl.session.timeout.seconds", -1L));
    assertEquals(20480L, WebTransportConfig.getLong("webtransport4j.ssl.session.cache.size", -1L));
  }
}
