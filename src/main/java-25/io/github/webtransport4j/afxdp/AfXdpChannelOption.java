package io.github.webtransport4j.afxdp;

import io.netty.channel.ChannelOption;

/**
 * Channel options specific to the AF_XDP native kernel-bypass transport.
 */
public final class AfXdpChannelOption {

  private AfXdpChannelOption() {}

  /** Pre-allocated AF_XDP UMEM frame pool. */
  public static final ChannelOption<AfXdpUmem> UMEM =
      ChannelOption.valueOf("AF_XDP_UMEM");

  /** Network interface name (e.g., "eth0", "lo"). */
  public static final ChannelOption<String> INTERFACE_NAME =
      ChannelOption.valueOf("AF_XDP_INTERFACE_NAME");

  /** NIC hardware queue ID to bind the AF_XDP socket. */
  public static final ChannelOption<Integer> QUEUE_ID =
      ChannelOption.valueOf("AF_XDP_QUEUE_ID");

  /** Local MAC address (6 bytes) for Layer 2 Ethernet framing. */
  public static final ChannelOption<byte[]> SOURCE_MAC =
      ChannelOption.valueOf("AF_XDP_SOURCE_MAC");

  /** Default gateway MAC address (6 bytes) for Layer 2 Ethernet framing. */
  public static final ChannelOption<byte[]> GATEWAY_MAC =
      ChannelOption.valueOf("AF_XDP_GATEWAY_MAC");
}
