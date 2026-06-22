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
    assertTrue(tp.getQueue() instanceof java.util.concurrent.LinkedBlockingQueue);
    executor.shutdown();
  }
}
