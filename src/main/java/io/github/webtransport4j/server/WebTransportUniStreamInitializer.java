package io.github.webtransport4j.server;

/*
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:05 pm
 */

import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;

public final class WebTransportUniStreamInitializer
        extends ChannelInitializer<QuicStreamChannel> {

    private final long streamType;

    public WebTransportUniStreamInitializer(long streamType) {
        this.streamType = streamType;
    }

    @Override
    protected void initChannel(QuicStreamChannel ch) {
        WebTransportUtils.addTrafficShapers(ch);
        ch.pipeline().addLast(new WebTransportUniStreamHeaderDecoder(this.streamType));
        ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
        ch.pipeline().addLast(new WebTransportCapsuleHandler());
        ch.pipeline().addLast(new MessageDispatcher());
    }
}