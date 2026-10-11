package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandler;

/**
 * Compatibility dispatcher alias for {@link DefaultMessageDispatcher}.
 *
 * <p>Inbound payloads use direct Netty EventLoop dispatch with zero-allocation buffers
 * natively supported in {@link DefaultMessageDispatcher}.
 */
@ChannelHandler.Sharable
public class ZeroGcMessageDispatcher extends DefaultMessageDispatcher {
  public static final ZeroGcMessageDispatcher INSTANCE = new ZeroGcMessageDispatcher();
}
