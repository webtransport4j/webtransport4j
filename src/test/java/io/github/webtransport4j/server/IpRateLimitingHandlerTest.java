package io.github.webtransport4j.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.quic.QuicChannel;
import java.net.InetSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for ip rate limiting handler. */
public class IpRateLimitingHandlerTest {

  private IpRateLimitingHandler handler;
  private ChannelHandlerContext ctx;
  private QuicChannel channel;

  /** Sets up test fixtures. */
  @Before
  public void setUp() {
    // Set standard config properties for tests
    System.setProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", "2");
    System.setProperty(
        "webtransport4j.server.ratelimit.whitelist", "127.0.0.1, 10.0.0.0/8, 2001:db8::/32");
    System.setProperty(
        "webtransport4j.server.ratelimit.overrides", "192.168.1.100:5, 192.168.2.50:0, fe80::1:10");
    System.setProperty("webtransport4j.server.ratelimit.blocklist", "9.9.9.9, 1.2.3.4");

    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();

    handler = new IpRateLimitingHandler();

    ctx = mock(ChannelHandlerContext.class);
    channel = mock(QuicChannel.class);
    when(ctx.channel()).thenReturn(channel);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);
    when(ctx.pipeline()).thenReturn(pipeline);
  }

  /** Cleans up test fixtures. */
  @After
  public void tearDown() {
    System.clearProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
    System.clearProperty("webtransport4j.server.ratelimit.whitelist");
    System.clearProperty("webtransport4j.server.ratelimit.overrides");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist");
    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();
  }

  private void simulateConnection(String ip) throws Exception {
    InetSocketAddress remoteAddress = new InetSocketAddress(ip, 443);
    when(channel.remoteSocketAddress()).thenReturn(remoteAddress);
    handler.channelActive(ctx);
  }

  @Test
  public void testDefaultRateLimiting() throws Exception {
    // Limit is 2
    simulateConnection("1.1.1.1");
    verify(ctx, times(1)).fireChannelActive();

    simulateConnection("1.1.1.1");
    verify(ctx, times(2)).fireChannelActive();

    // 3rd connection should be dropped
    simulateConnection("1.1.1.1");
    verify(ctx, times(2)).fireChannelActive();
    verify(ctx, times(1)).close(); // Connection is closed
  }

  @Test
  public void testExactIpWhitelist() throws Exception {
    // Whitelisted IP 127.0.0.1 should never be dropped
    for (int i = 0; i < 10; i++) {
      simulateConnection("127.0.0.1");
    }
    verify(ctx, times(10)).fireChannelActive();
    verify(ctx, never()).close();
  }

  @Test
  public void testCidrPrefixWhitelist() throws Exception {
    // 10.0.0.0/8 is whitelisted (prefix 10.)
    for (int i = 0; i < 10; i++) {
      simulateConnection("10.5.5.5");
    }
    verify(ctx, times(10)).fireChannelActive();
    verify(ctx, never()).close();

    // But 11.5.5.5 is NOT whitelisted
    simulateConnection("11.5.5.5");
    simulateConnection("11.5.5.5");
    simulateConnection("11.5.5.5");
    verify(ctx, times(12)).fireChannelActive(); // 10 from previous + 2 passed here
    verify(ctx, times(1)).close(); // 3rd is dropped
  }

  @Test
  public void testOverrideExactIpLimit() throws Exception {
    // 192.168.1.100 has an override limit of 5
    for (int i = 0; i < 5; i++) {
      simulateConnection("192.168.1.100");
    }
    verify(ctx, times(5)).fireChannelActive();
    verify(ctx, never()).close();

    // 6th connection should fail
    simulateConnection("192.168.1.100");
    verify(ctx, times(5)).fireChannelActive();
    verify(ctx, times(1)).close();
  }

  @Test
  public void testOverrideZeroLimit() throws Exception {
    // 192.168.2.50 has an override limit of 0 (dropped immediately)
    simulateConnection("192.168.2.50");
    verify(ctx, never()).fireChannelActive();
    verify(ctx, times(1)).close();
  }

  @Test
  public void testIpv6WhitelistAndOverride() throws Exception {
    // 2001:db8::/32 is whitelisted
    for (int i = 0; i < 10; i++) {
      simulateConnection("2001:db8:1234::1");
    }
    verify(ctx, times(10)).fireChannelActive();
    verify(ctx, never()).close();

    // fe80::1 has an override limit of 10
    for (int i = 0; i < 10; i++) {
      simulateConnection("fe80:0:0:0:0:0:0:1");
    }
    verify(ctx, times(20)).fireChannelActive(); // 10 previous + 10 here

    simulateConnection("fe80:0:0:0:0:0:0:1");
    verify(ctx, times(20)).fireChannelActive(); // Limit reached
    verify(ctx, times(1)).close();
  }

  @Test
  public void testBloomFilterBlocklist() throws Exception {
    // 9.9.9.9 is in the blocklist, should drop instantly on first connection
    simulateConnection("9.9.9.9");
    verify(ctx, never()).fireChannelActive();
    verify(ctx, times(1)).close();

    // 1.2.3.4 is in the blocklist
    simulateConnection("1.2.3.4");
    verify(ctx, never()).fireChannelActive(); // still 0
    verify(ctx, times(2)).close(); // previous + this one
  }

  @Test
  public void testExactBlocklistContents() throws Exception {
    java.lang.reflect.Field field = IpRateLimitingHandler.class.getDeclaredField("exactBlocklist");
    field.setAccessible(true);
    java.util.Set<String> exactBlocklist = (java.util.Set<String>) field.get(handler);

    org.junit.Assert.assertTrue(exactBlocklist.contains("9.9.9.9"));
    org.junit.Assert.assertTrue(exactBlocklist.contains("1.2.3.4"));
    org.junit.Assert.assertFalse(exactBlocklist.contains("1.1.1.1"));
  }

  @Test
  public void testDynamicReloadConfigParsing() {
    System.setProperty("webtransport4j.server.ratelimit.dynamic_reload.enabled", "false");
    System.setProperty("webtransport4j.server.ratelimit.dynamic_reload.interval_secs", "45");
    WebTransportConfig.reload();

    try {
      org.junit.Assert.assertFalse(WebTransportConfig.getBoolean("webtransport4j.server.ratelimit.dynamic_reload.enabled", true));
      org.junit.Assert.assertEquals(45, WebTransportConfig.getInt("webtransport4j.server.ratelimit.dynamic_reload.interval_secs", 10));
    } finally {
      System.clearProperty("webtransport4j.server.ratelimit.dynamic_reload.enabled");
      System.clearProperty("webtransport4j.server.ratelimit.dynamic_reload.interval_secs");
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testSharedRateLimitRulesIncrementalReuse() throws Exception {
    Class<?> rulesClass = Class.forName("io.github.webtransport4j.server.IpRateLimitingHandler$SharedRateLimitRules");
    java.lang.reflect.Constructor<?> defaultCtor = rulesClass.getDeclaredConstructor();
    defaultCtor.setAccessible(true);
    java.lang.reflect.Constructor<?> paramCtor = rulesClass.getDeclaredConstructor(rulesClass);
    paramCtor.setAccessible(true);

    // Initial construction
    Object rules1 = defaultCtor.newInstance();

    // 1. Reload with no changes to rate-limiting configuration
    System.setProperty("webtransport4j.session.resumption.timeout.seconds", "99");
    WebTransportConfig.reload();

    Object rules2;
    try {
      rules2 = paramCtor.newInstance(rules1);
    } finally {
      System.clearProperty("webtransport4j.session.resumption.timeout.seconds");
      WebTransportConfig.reload();
    }

    // Retrieve fields
    java.lang.reflect.Field whitelistField = rulesClass.getDeclaredField("whitelistEngine");
    whitelistField.setAccessible(true);
    java.lang.reflect.Field overridesField = rulesClass.getDeclaredField("overridesEngine");
    overridesField.setAccessible(true);
    java.lang.reflect.Field blocklistField = rulesClass.getDeclaredField("blocklistFilter");
    blocklistField.setAccessible(true);
    java.lang.reflect.Field exactBlocklistField = rulesClass.getDeclaredField("exactBlocklist");
    exactBlocklistField.setAccessible(true);

    // Assert same references are reused
    org.junit.Assert.assertSame(whitelistField.get(rules1), whitelistField.get(rules2));
    org.junit.Assert.assertSame(overridesField.get(rules1), overridesField.get(rules2));
    org.junit.Assert.assertSame(blocklistField.get(rules1), blocklistField.get(rules2));
    org.junit.Assert.assertSame(exactBlocklistField.get(rules1), exactBlocklistField.get(rules2));

    // 2. Change blocklist configuration
    System.setProperty("webtransport4j.server.ratelimit.blocklist", "10.0.0.1");
    WebTransportConfig.reload();

    Object rules3;
    try {
      rules3 = paramCtor.newInstance(rules2);
    } finally {
      System.clearProperty("webtransport4j.server.ratelimit.blocklist");
      WebTransportConfig.reload();
    }

    // Whitelist and overrides should still be reused
    org.junit.Assert.assertSame(whitelistField.get(rules2), whitelistField.get(rules3));
    org.junit.Assert.assertSame(overridesField.get(rules2), overridesField.get(rules3));

    // Blocklist should be recreated
    org.junit.Assert.assertNotSame(blocklistField.get(rules2), blocklistField.get(rules3));
    org.junit.Assert.assertNotSame(exactBlocklistField.get(rules2), exactBlocklistField.get(rules3));
  }
}
