package io.github.webtransport4j.resilience;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import org.junit.Test;

/**
 * Tests for {@link UdpSocketTuner}.
 */
public class UdpSocketTunerTest {

  @Test
  public void testRecommendedBufferSizes() {
    final int rcv = UdpSocketTuner.getRecommendedReceiveBufferSize();
    final int snd = UdpSocketTuner.getRecommendedSendBufferSize();

    assertTrue("Receive recommendation must be positive", rcv > 0);
    assertTrue("Send recommendation must be positive", snd > 0);
    assertTrue(rcv <= UdpSocketTuner.RECOMMENDED_BUFFER_SIZE);
    assertTrue(snd <= UdpSocketTuner.RECOMMENDED_BUFFER_SIZE);
  }

  @Test
  public void testTuneBootstrap() {
    final Bootstrap bootstrap = new Bootstrap();
    UdpSocketTuner.tune(bootstrap);

    final int rcv = UdpSocketTuner.getRecommendedReceiveBufferSize();
    final int snd = UdpSocketTuner.getRecommendedSendBufferSize();
    assertEquals(rcv, bootstrap.config().options().get(ChannelOption.SO_RCVBUF));
    assertEquals(snd, bootstrap.config().options().get(ChannelOption.SO_SNDBUF));
  }

  @Test
  public void testExplicitOptionsArePreservedIndependently() {
    int explicit = 8 * 1024 * 1024;
    Bootstrap receiveConfigured = new Bootstrap().option(ChannelOption.SO_RCVBUF, explicit);
    UdpSocketTuner.tune(receiveConfigured);
    assertEquals(explicit, receiveConfigured.config().options().get(ChannelOption.SO_RCVBUF));
    assertEquals(UdpSocketTuner.getRecommendedSendBufferSize(),
        receiveConfigured.config().options().get(ChannelOption.SO_SNDBUF));

    Bootstrap sendConfigured = new Bootstrap().option(ChannelOption.SO_SNDBUF, explicit);
    UdpSocketTuner.tune(sendConfigured);
    assertEquals(explicit, sendConfigured.config().options().get(ChannelOption.SO_SNDBUF));
    assertEquals(UdpSocketTuner.getRecommendedReceiveBufferSize(),
        sendConfigured.config().options().get(ChannelOption.SO_RCVBUF));

    receiveConfigured.option(ChannelOption.SO_SNDBUF, explicit);
    UdpSocketTuner.tune(receiveConfigured);
    assertEquals(explicit, receiveConfigured.config().options().get(ChannelOption.SO_RCVBUF));
    assertEquals(explicit, receiveConfigured.config().options().get(ChannelOption.SO_SNDBUF));
  }

  @Test
  public void testLinuxRecommendationsRespectDetectedLimits() {
    for (String setting : new String[] {"net.core.rmem_max", "net.core.wmem_max"}) {
      assertEquals(212992, UdpSocketTuner.recommendLinuxBufferSize(212992, setting));
      assertEquals(UdpSocketTuner.MIN_BUFFER_SIZE,
          UdpSocketTuner.recommendLinuxBufferSize(UdpSocketTuner.MIN_BUFFER_SIZE, setting));
      assertEquals(2 * 1024 * 1024,
          UdpSocketTuner.recommendLinuxBufferSize(2 * 1024 * 1024, setting));
      assertEquals(UdpSocketTuner.RECOMMENDED_BUFFER_SIZE,
          UdpSocketTuner.recommendLinuxBufferSize(16 * 1024 * 1024, setting));
      assertEquals(UdpSocketTuner.RECOMMENDED_BUFFER_SIZE,
          UdpSocketTuner.recommendLinuxBufferSize(-1, setting));
    }
  }
}
