package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http3.Http3DataFrame;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * @author https://github.com/sanjomo
 * @date 24/06/26 1:06 pm
 */
final class Http3DataToByteBufHandler extends MessageToMessageDecoder<Http3DataFrame> {

    @Override
    protected void decode(@NonNull ChannelHandlerContext ctx, @NonNull Http3DataFrame frame, @NonNull List<Object> out) {
        out.add(frame.content().retain());
    }
}
