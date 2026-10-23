package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AfXdpNativeBinding} and native AF_XDP channel bootstrapping.
 */
public class AfXdpNativeBindingTest {

  private EventLoopGroup group;
  private AfXdpUmem umem;
  private AfXdpDatagramChannel channel;

  /** Sets up test event loop and UMEM pool. */
  @BeforeEach
  public void setUp() {
    group = new DefaultEventLoopGroup(1);
    umem = new AfXdpUmem(4096, 64);
  }

  /** Tears down test resources. */
  @AfterEach
  public void tearDown() {
    if (channel != null && channel.isOpen()) {
      channel.close().syncUninterruptibly();
    }
    if (group != null) {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
    if (umem != null && !umem.isClosed()) {
      umem.close();
    }
  }

  @Test
  public void testNativeBindingAvailabilityAndErrnoDescriptions() {
    boolean available = AfXdpNativeBinding.isAvailable();
    assertTrue(available, "libxdp must be detected on Linux host with libxdp-dev");

    String epermDesc = AfXdpNativeBinding.describeErrno(1);
    assertTrue(epermDesc.contains("CAP_NET_ADMIN"));

    String enodevDesc = AfXdpNativeBinding.describeErrno(19);
    assertTrue(enodevDesc.contains("ENODEV"));
  }

  @Test
  public void testGracefulFallbackWhenUnprivileged() throws Exception {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .option(AfXdpChannelOption.INTERFACE_NAME, "lo")
        .option(AfXdpChannelOption.NATIVE_MODE_REQUIRED, false)
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(14433).sync().channel();
    assertTrue(channel.isOpen());
    assertTrue(channel.isActive());
  }

  @Test
  public void testNativeModeRequiredThrowsWhenUnprivileged() {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .option(AfXdpChannelOption.INTERFACE_NAME, "lo")
        .option(AfXdpChannelOption.NATIVE_MODE_REQUIRED, true)
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    Exception thrown = assertThrows(Exception.class, () -> {
      bootstrap.bind(14434).sync();
    });

    Throwable target = thrown instanceof AfXdpException ? thrown : thrown.getCause();
    assertNotNull(target, "Expected AfXdpException or wrapped cause");
    assertTrue(target instanceof AfXdpException, "Target must be AfXdpException");
    AfXdpException afXdpEx = (AfXdpException) target;
    assertEquals(1, afXdpEx.getErrno(), "Errno must be 1 (EPERM) when unprivileged");
  }
}
