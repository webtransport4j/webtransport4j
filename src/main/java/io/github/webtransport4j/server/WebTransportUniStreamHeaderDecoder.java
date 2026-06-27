package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;
import org.jspecify.annotations.NonNull;

/**
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:02 pm
 */
public class WebTransportUniStreamHeaderDecoder extends ByteToMessageDecoder {

    private boolean sessionHeaderRead = false;

    private final long streamType;

    public WebTransportUniStreamHeaderDecoder(long streamType) {
        this.streamType = streamType;
    }

    @Override
    protected void decode(@NonNull ChannelHandlerContext ctx, @NonNull ByteBuf in, @NonNull List<Object> out) throws Exception {
        if (!sessionHeaderRead) {
            in.markReaderIndex();
            long sessionId = readVariableLengthInt(in);
            if (sessionId == -1) {
                in.resetReaderIndex();
                return;
            }
            ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);
            sessionHeaderRead = true;
        }
        if (!in.isReadable()) {
            return;
        }
        String savedPath = ctx.channel().parent().attr(WebTransportAttributeKeys.SESSION_PATH_KEY).get();
        ctx.channel().attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set(this.streamType);
        ctx.channel().attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set(savedPath);
        out.add(in.readRetainedSlice(in.readableBytes()));
    }
}
