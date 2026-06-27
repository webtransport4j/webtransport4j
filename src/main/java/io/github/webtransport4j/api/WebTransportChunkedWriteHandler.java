package io.github.webtransport4j.api;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WebTransportChunkedWriteHandler extends ChunkedWriteHandler {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportChunkedWriteHandler.class);

    @Override
    public void write(ChannelHandlerContext ctx,
                      Object msg,
                      ChannelPromise promise) throws Exception {

        logger.debug("ChunkedWriteHandler.write(): {}", msg.getClass());

        super.write(ctx, msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        logger.debug("ChunkedWriteHandler.flush()");
        super.flush(ctx);
    }
}