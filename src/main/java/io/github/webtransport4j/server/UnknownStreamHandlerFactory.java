package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandler;
import org.apache.log4j.Logger;

import java.util.function.LongFunction;

/**
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:06 pm
 */
public final class UnknownStreamHandlerFactory
        implements LongFunction<ChannelHandler> {
    private static final Logger logger = Logger.getLogger(UnknownStreamHandlerFactory.class.getName());

    @Override
    public ChannelHandler apply(long streamType) {
        if (streamType == WebTransportUtils.UNI_STREAM_TYPE) {
            return new WebTransportUniStreamInitializer(streamType);
        }
        if(logger.isDebugEnabled()) {
            logger.debug("Unknown stream type: " + streamType);
        }
        return null;
    }
}