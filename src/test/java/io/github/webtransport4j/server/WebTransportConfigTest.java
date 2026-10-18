package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for web transport config. */
public class WebTransportConfigTest {
  /** Cleanup. */
  @Before
  @After
  public void cleanup() {
    WebTransportConfig.clearProgrammaticProperties();
    System.clearProperty("test.math.key");
    System.clearProperty("test.string.key");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.key");
    System.clearProperty("webtransport4j.quic.token.handler.hmac.expiration.ms");
    System.clearProperty("webtransport4j.ssl.session.timeout.seconds");
    System.clearProperty("webtransport4j.ssl.session.cache.size");
    System.clearProperty("webtransport4j.epoll.udpgro");
    System.clearProperty("webtransport4j.epoll.udpgso");
    System.clearProperty("webtransport4j.epoll.gso.size");
    System.clearProperty("webtransport4j.iouring.udpgso");
    System.clearProperty("webtransport4j.iouring.gso.size");
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
  public void testGetIntAcceptsRangeBoundaries() {
    for (int value : new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE}) {
      System.setProperty("test.math.key", String.valueOf(value));
      assertEquals(value, WebTransportConfig.getInt("test.math.key", 999));
      assertEquals(
          Integer.valueOf(value), WebTransportConfig.snapshot().getIntOrNull("test.math.key"));
    }
  }

  @Test
  public void testGetIntFallsBackOnOutOfRangeValues() {
    for (String value :
        Arrays.asList(
            "2147483648", "-2147483649", "4294967297", "-4294967295", "4096 * 1024 * 1024")) {
      System.setProperty("test.math.key", value);
      assertEquals(999, WebTransportConfig.getInt("test.math.key", 999));
      assertEquals(999, WebTransportConfig.snapshot().getInt("test.math.key", 999));
      assertNull(WebTransportConfig.snapshot().getIntOrNull("test.math.key"));
    }
  }

  @Test
  public void testGetLongDefaultValue() {
    assertEquals(555L, WebTransportConfig.getLong("nonexistent.key.for.test", 555L));
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

    // Test invalid GSO size range validation via WebTransportServer.validateGsoSize
    System.setProperty("webtransport4j.epoll.gso.size", "0");
    int sizeInvalidLow = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64);
    assertThrows(
        IllegalArgumentException.class, () -> WebTransportServer.validateGsoSize(sizeInvalidLow));

    System.setProperty("webtransport4j.epoll.gso.size", "65");
    int sizeInvalidHigh = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64);
    assertThrows(
        IllegalArgumentException.class, () -> WebTransportServer.validateGsoSize(sizeInvalidHigh));

    // Valid bounds must succeed
    WebTransportServer.validateGsoSize(1);
    WebTransportServer.validateGsoSize(64);
  }

  @Test
  public void testIoUringConfigOptions() {
    // Test defaults
    assertTrue(WebTransportConfig.getBoolean("webtransport4j.iouring.udpgso", true));
    assertEquals(64, WebTransportConfig.getInt("webtransport4j.iouring.gso.size", 64));

    // Test system property override
    System.setProperty("webtransport4j.iouring.udpgso", "false");
    System.setProperty("webtransport4j.iouring.gso.size", "16");

    assertTrue(!WebTransportConfig.getBoolean("webtransport4j.iouring.udpgso", true));
    assertEquals(16, WebTransportConfig.getInt("webtransport4j.iouring.gso.size", 64));

    // Test invalid GSO size range validation via WebTransportServer.validateGsoSize
    System.setProperty("webtransport4j.iouring.gso.size", "0");
    int sizeInvalidLow = WebTransportConfig.getInt("webtransport4j.iouring.gso.size", 64);
    assertThrows(
        IllegalArgumentException.class,
        () -> WebTransportServer.validateGsoSize(sizeInvalidLow, "webtransport4j.iouring.gso.size"));

    System.setProperty("webtransport4j.iouring.gso.size", "65");
    int sizeInvalidHigh = WebTransportConfig.getInt("webtransport4j.iouring.gso.size", 64);
    assertThrows(
        IllegalArgumentException.class,
        () -> WebTransportServer.validateGsoSize(sizeInvalidHigh, "webtransport4j.iouring.gso.size"));

    // Valid bounds must succeed
    WebTransportServer.validateGsoSize(1, "webtransport4j.iouring.gso.size");
    WebTransportServer.validateGsoSize(64, "webtransport4j.iouring.gso.size");
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
      Files.write(
          tempFile.toPath(),
          Arrays.asList(
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
              "webtransport4j.session.resumption.timeout.seconds=120"));

      // 2. Trigger reload
      WebTransportConfig.reload();

      // 3. Verify all values are resolved correctly
      assertEquals(
          999,
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100));
      assertEquals(
          12345,
          WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 100000));
      assertEquals(
          "trie", WebTransportConfig.get("webtransport4j.server.ratelimit.filter_engine", "netty"));
      assertEquals(
          "8.8.8.8,9.9.9.9",
          WebTransportConfig.get("webtransport4j.server.ratelimit.whitelist", ""));
      assertEquals(
          "10.0.0.1:5,10.0.0.2:0",
          WebTransportConfig.get("webtransport4j.server.ratelimit.overrides", ""));
      assertEquals(
          "5.5.5.5,6.6.6.6",
          WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist", ""));
      assertEquals(
          2000000,
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1000000));
      assertEquals(
          "0.00001",
          WebTransportConfig.get(
              "webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001"));
      assertEquals(
          77L,
          WebTransportConfig.getLong(
              "webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi", 5000L));
      assertEquals(
          88L,
          WebTransportConfig.getLong(
              "webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni", 5000L));
      assertEquals(
          120L,
          WebTransportConfig.getLong("webtransport4j.session.resumption.timeout.seconds", 60L));

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
    WebTransportConfig.setProperty("test.sample.custom.key", "CUSTOM_VALUE");
    assertEquals(
        "CUSTOM_VALUE", WebTransportConfig.get("test.sample.custom.key", null));

    WebTransportConfig.removeProperty("test.sample.custom.key");
    assertEquals(
        "FALLBACK_VALUE",
        WebTransportConfig.get("test.sample.custom.key", "FALLBACK_VALUE"));
  }

  @Test
  public void testProgrammaticPropertiesIsolationFromReload() {
    WebTransportConfig.setProperty(
        "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", "555");
    try {
      assertEquals(
          555,
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100));
      // Reload must NOT wipe out programmatic properties
      boolean reloaded = WebTransportConfig.reload();
      org.junit.Assert.assertFalse(reloaded);
      assertEquals(
          555,
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100));
    } finally {
      WebTransportConfig.removeProperty(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
    }
  }

  @Test
  public void testDynamicOverridesProfile() throws Exception {
    File dynamicFile = new File("webtransport-dynamic.properties");
    Files.write(
        dynamicFile.toPath(),
        Arrays.asList("webtransport4j.server.ratelimit.max_tracked_ips=12345"));
    try {
      WebTransportConfig.reload();
      assertEquals(
          12345,
          WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 0));
    } finally {
      if (dynamicFile.exists()) {
        dynamicFile.delete();
      }
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testReloadSkipsUntouchedAndCommentOnlyChanges() throws Exception {
    File dynamicFile = new File("webtransport-dynamic.properties");
    try {
      // 1. Initial write
      Files.write(
          dynamicFile.toPath(),
          Arrays.asList("webtransport4j.server.ratelimit.max_tracked_ips=9999"));
      assertTrue(WebTransportConfig.reload());
      assertEquals(
          9999,
          WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 0));

      // 2. Untouched: parsed properties are identical, reload returns false
      org.junit.Assert.assertFalse(WebTransportConfig.reload());

      // 3. Semantic check: modify file by adding comments only
      Files.write(
          dynamicFile.toPath(),
          Arrays.asList(
              "# This is a harmless comment",
              "webtransport4j.server.ratelimit.max_tracked_ips=9999",
              "# Another comment"));
      // Parsed properties are identical -> returns false without publishing
      org.junit.Assert.assertFalse(WebTransportConfig.reload());

      // 4. Semantic change: modify value
      Files.write(
          dynamicFile.toPath(),
          Arrays.asList("webtransport4j.server.ratelimit.max_tracked_ips=8888"));
      assertTrue(WebTransportConfig.reload());
      assertEquals(
          8888,
          WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 0));
    } finally {
      if (dynamicFile.exists()) {
        dynamicFile.delete();
      }
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testResolveWriteBufferWaterMarkSingleBoundOverrides() {
    try {
      WebTransportConfig.setProperty("webtransport4j.netty.write_buffer.low_water_mark", "8388608");
      io.netty.channel.WriteBufferWaterMark wmHighAdjusted =
          WebTransportUtils.resolveWriteBufferWaterMark();
      assertEquals(8388608, wmHighAdjusted.low());
      assertEquals(8388608, wmHighAdjusted.high());

      WebTransportConfig.clearProgrammaticProperties();
      WebTransportConfig.setProperty("webtransport4j.netty.write_buffer.high_water_mark", "1048576");
      io.netty.channel.WriteBufferWaterMark wmLowAdjusted =
          WebTransportUtils.resolveWriteBufferWaterMark();
      assertTrue(wmLowAdjusted.low() <= wmLowAdjusted.high());
      assertEquals(1048576, wmLowAdjusted.high());
    } finally {
      WebTransportConfig.clearProgrammaticProperties();
    }
  }

  @Test
  public void testResolveWriteBufferWaterMarkMalformedLowBound() {
    assertWriteBufferWaterMark("invalid", "1 * 1024 * 1024", 524288, 1048576);
  }

  @Test
  public void testResolveWriteBufferWaterMarkMalformedHighBound() {
    assertWriteBufferWaterMark("8388608", "invalid", 8388608, 8388608);
  }

  @Test
  public void testResolveWriteBufferWaterMarkBothBoundsMalformed() {
    assertWriteBufferWaterMark("invalid", "invalid", 2097152, 4194304);
  }

  @Test
  public void testResolveWriteBufferWaterMarkSingleMalformedBound() {
    assertWriteBufferWaterMark("invalid", null, 2097152, 4194304);
    assertWriteBufferWaterMark(null, "invalid", 2097152, 4194304);
  }

  @Test
  public void testResolveWriteBufferWaterMarkReversedBounds() {
    assertWriteBufferWaterMark("3145728", "1048576", 3145728, 3145728);
  }

  @Test
  public void testResolveWriteBufferWaterMarkOutOfRangeBounds() {
    for (String value : Arrays.asList("2147483648", "-2147483649", "4096 * 1024 * 1024")) {
      assertWriteBufferWaterMark(value, "1048576", 524288, 1048576);
      assertWriteBufferWaterMark("8388608", value, 8388608, 8388608);
    }
  }

  @Test
  public void testResolveWriteBufferWaterMarkClampsParsedNonPositiveLowBound() {
    for (String value : Arrays.asList("0", "-1", "-2147483648")) {
      assertWriteBufferWaterMark(value, null, 1, 4194304);
      assertWriteBufferWaterMark(value, "invalid", 1, 4194304);
    }
  }

  @Test
  public void testResolveWriteBufferWaterMarkClampsParsedNonPositiveHighBound() {
    for (String value : Arrays.asList("0", "-1", "-2147483648")) {
      assertWriteBufferWaterMark(null, value, 1, 1);
      assertWriteBufferWaterMark("invalid", value, 1, 1);
    }
  }

  @Test
  public void testWriteBufferStaticDefaultsAreNotOverrides() {
    String lowKey = "webtransport4j.netty.write_buffer.low_water_mark";
    String highKey = "webtransport4j.netty.write_buffer.high_water_mark";
    WebTransportConfig.Snapshot before = WebTransportConfig.snapshot();
    assertNotNull(before.get(lowKey, null));
    assertFalse(before.hasNonStaticValue(lowKey));
    assertFalse(before.hasNonStaticValue(highKey));
  }

  @Test
  public void testWriteBufferProgrammaticOverrideSnapshot() {
    String lowKey = "webtransport4j.netty.write_buffer.low_water_mark";
    final WebTransportConfig.Snapshot before = WebTransportConfig.snapshot();
    WebTransportConfig.setProperty(lowKey, "invalid");
    WebTransportConfig.Snapshot overridden = WebTransportConfig.snapshot();
    WebTransportConfig.removeProperty(lowKey);
    assertTrue(overridden.hasNonStaticValue(lowKey));
    assertNull(overridden.getIntOrNull(lowKey));
    assertFalse(before.hasNonStaticValue(lowKey));
    assertFalse(WebTransportConfig.snapshot().hasNonStaticValue(lowKey));
  }

  @Test
  public void testWriteBufferSystemPropertyOverride() {
    String highKey = "webtransport4j.netty.write_buffer.high_water_mark";
    String originalHigh = System.getProperty(highKey);
    try {
      System.setProperty(highKey, "1048576");
      assertTrue(WebTransportConfig.snapshot().hasNonStaticValue(highKey));
      io.netty.channel.WriteBufferWaterMark waterMark =
          WebTransportUtils.resolveWriteBufferWaterMark();
      assertEquals(524288, waterMark.low());
      assertEquals(1048576, waterMark.high());
    } finally {
      if (originalHigh == null) {
        System.clearProperty(highKey);
      } else {
        System.setProperty(highKey, originalHigh);
      }
    }
  }

  private static void assertWriteBufferWaterMark(
      String low, String high, int expectedLow, int expectedHigh) {
    WebTransportConfig.clearProgrammaticProperties();
    if (low != null) {
      WebTransportConfig.setProperty("webtransport4j.netty.write_buffer.low_water_mark", low);
    }
    if (high != null) {
      WebTransportConfig.setProperty("webtransport4j.netty.write_buffer.high_water_mark", high);
    }
    io.netty.channel.WriteBufferWaterMark waterMark = WebTransportUtils.resolveWriteBufferWaterMark();
    assertEquals(expectedLow, waterMark.low());
    assertEquals(expectedHigh, waterMark.high());
  }
}
