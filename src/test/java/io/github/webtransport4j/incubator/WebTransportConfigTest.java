package io.github.webtransport4j.incubator;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebTransportConfigTest {

  @Before
  @After
  public void cleanup() {
    System.clearProperty("test.math.key");
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
}
