package io.github.webtransport4j.server;

import io.netty.channel.*;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.github.webtransport4j.api.WebTransportChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WebTransportStreamChannelInitializer
        extends ChannelInitializer<QuicStreamChannel> {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportStreamChannelInitializer.class);
    @Override
    protected void initChannel(QuicStreamChannel stream) {

        WebTransportUtils.addTrafficShapers(stream);

        stream.pipeline().addFirst(new WebTransportDetectorHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportDetectorHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addFirst(
                new QuicGlobalSniffer("STREAM-" + stream.streamId()));
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added QuicGlobalSniffer (per-stream). Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new RawWebTransportHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added RawWebTransportHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new WebTransportChunkedWriteHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportChunkedWriteHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportStreamFrameDecoder. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new WebTransportHeadersHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportHeadersHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new Http3DataToByteBufHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added Http3DataToByteBufHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new WebTransportCapsuleDecoder());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportCapsuleDecoder. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new WebTransportCapsuleHandler());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportCapsuleHandler. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new MessageDispatcher());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added MessageDispatcher. Pipeline now: {}", stream.pipeline().names());
        }

        stream.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void exceptionCaught(
                    ChannelHandlerContext ctx,
                    Throwable cause) {

                logger.error("❌ PIPELINE ERROR: {}", cause.getMessage(), cause);
                cause.printStackTrace();
            }
        });
    }


}