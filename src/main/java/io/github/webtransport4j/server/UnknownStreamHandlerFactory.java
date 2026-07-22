package io.github.webtransport4j.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.function.LongFunction;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates channel handlers for unknown stream types.
 *
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:06 pm
 */
public final class UnknownStreamHandlerFactory implements LongFunction<ChannelHandler> {

  private static final Logger logger = LoggerFactory.getLogger(UnknownStreamHandlerFactory.class);

  @Override
  public @Nullable ChannelHandler apply(long streamType) {
    if (streamType == WebTransportUtils.UNI_STREAM_TYPE) {
      return new WebTransportUniStreamInitializer(streamType);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Unknown stream type: {}", streamType);
    }
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) {
        ch.eventLoop().execute(() -> {
          if (ch instanceof QuicStreamChannel) {
            ((QuicStreamChannel) ch).shutdown(0x010E, ch.newPromise());
          }
          ch.close();
        });
      }
    };
  }
}
