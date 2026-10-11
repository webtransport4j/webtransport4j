package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandler;

/**
 * Compatibility dispatcher alias for {@link DefaultMessageDispatcher}.
 *
 * <p>Inbound payloads use direct Netty EventLoop dispatch with thread-confined flyweight
 * buffers natively supported in {@link DefaultMessageDispatcher}.
 *
 * @deprecated dispatch is flyweight-based unconditionally in {@link DefaultMessageDispatcher};
 *     use {@link DefaultMessageDispatcher#INSTANCE} instead.
 */
@Deprecated
@ChannelHandler.Sharable
public class ZeroGcMessageDispatcher extends DefaultMessageDispatcher {
  public static final ZeroGcMessageDispatcher INSTANCE = new ZeroGcMessageDispatcher();
}
