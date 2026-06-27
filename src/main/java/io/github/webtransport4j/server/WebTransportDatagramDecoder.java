package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import org.jspecify.annotations.NonNull;

class WebTransportDatagramDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportDatagramDecoder.class);

    @Override
    protected void decode(@NonNull ChannelHandlerContext ctx, @NonNull ByteBuf msg, @NonNull List<Object> out) {
        if (logger.isDebugEnabled()) {
            logger.debug("☄️ DatagramHandler received data: {} bytes", msg.readableBytes());
        }
        long sessionId = WebTransportUtils.readVariableLengthInt(msg);
        if (sessionId == -1) {
            return;
        }
        ByteBuf payload = msg.readRetainedSlice(msg.readableBytes());
        out.add(new WebTransportDatagramFrame(sessionId, payload));
    }
}
