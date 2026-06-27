package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

class QuicGlobalSniffer extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(QuicGlobalSniffer.class);

    private final String prefix;

    public QuicGlobalSniffer(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf data = (ByteBuf) msg;
            int len = data.readableBytes();
            // Formatting for readability
            if (len > 0) {
                String hex = ByteBufUtil.hexDump(data);
                if (logger.isTraceEnabled()) {
                    logger.trace("👀 [{}] ID:{} LEN:{}", prefix, ctx.channel().id().asShortText(), len);
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("    HEX: {}", hex);
                }
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("👀 [{}] MsgType: {}", prefix, msg.getClass().getSimpleName());
            }
        }
        // Pass it on!
        ctx.fireChannelRead(msg);
    }

    @Override
    public void userEventTriggered(@NonNull ChannelHandlerContext ctx, @NonNull Object evt) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("👀 [{}] UserEvent: {} -> {}", prefix, evt.getClass().getName(), evt);
        }
        ctx.fireUserEventTriggered(evt);
    }
}
