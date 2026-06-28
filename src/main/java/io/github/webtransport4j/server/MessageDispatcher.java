package io.github.webtransport4j.server;

import io.netty.channel.ChannelInboundHandler;

/**
 * Interface for WebTransport message dispatchers.
 * Custom implementations can be provided via {@link WebTransportServer#setMessageDispatcherSupplier}.
 */
public interface MessageDispatcher extends ChannelInboundHandler {
}
