package io.github.webtransport4j.resilience;

import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import org.junit.Test;

/**
 * Tests for {@link UdpSocketTuner}.
 */
public class UdpSocketTunerTest {

  @Test
  public void testRecommendedBufferSizes() {
    final int rcv = UdpSocketTuner.getRecommendedReceiveBufferSize();
    final int snd = UdpSocketTuner.getRecommendedSendBufferSize();

    assertTrue("Receive buffer size must be at least 1MB", rcv >= UdpSocketTuner.MIN_BUFFER_SIZE);
    assertTrue("Send buffer size must be at least 1MB", snd >= UdpSocketTuner.MIN_BUFFER_SIZE);
  }

  @Test
  public void testTuneBootstrap() {
    final Bootstrap bootstrap = new Bootstrap();
    UdpSocketTuner.tune(bootstrap);

    final int rcv = UdpSocketTuner.getRecommendedReceiveBufferSize();
    final int snd = UdpSocketTuner.getRecommendedSendBufferSize();
    assertTrue(rcv >= 1024 * 1024);
    assertTrue(snd >= 1024 * 1024);
  }
}
