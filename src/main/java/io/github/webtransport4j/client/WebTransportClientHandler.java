package io.github.webtransport4j.client;

/**
 * @author https://github.com/sanjomo
 * @date 03/07/26 4:53 pm
 */

import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Webtransport client implementation
 */
public class WebTransportClientHandler extends Http3ClientConnectionHandler {
    private Logger logger = LoggerFactory.getLogger(WebTransportClientHandler.class);


    public WebTransportClientHandler() {
        this(null, true, null);
    }


    public WebTransportClientHandler(
            Http3SettingsFrame localSettings, boolean disableQpackDynamicTable) {
        this(localSettings,
                disableQpackDynamicTable, null);
    }


    public WebTransportClientHandler(Http3SettingsFrame localSettings, boolean disableQpackDynamicTable,
                                     Http3Settings.NonStandardHttp3SettingsValidator
                                             nonStandardSettingsValidator) {
        super(null, null, new UnknownStreamHandlerFactory(), localSettings,
                disableQpackDynamicTable, nonStandardSettingsValidator);
    }

    @Override
    protected void initBidirectionalStream(ChannelHandlerContext ctx,
                                           QuicStreamChannel channel) {
        logger.info("Initializing bidirectional stream {}", channel.streamId());

        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = (ByteBuf) msg;
                    System.out.println("=== BIDI STREAM " + channel.streamId() + " ===");
                    System.out.println(io.netty.buffer.ByteBufUtil.prettyHexDump(buf));
                    System.out.println("ASCII: " +
                            buf.toString(buf.readerIndex(), buf.readableBytes(), CharsetUtil.UTF_8));
                }

                ctx.fireChannelRead(msg);
            }
        });
    }

    @Override
    protected void initUnidirectionalStream(ChannelHandlerContext ctx,
                                            QuicStreamChannel streamChannel) {
        logger.info("Initializing unidirectional stream {}", streamChannel.streamId());

        streamChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = (ByteBuf) msg;
                    System.out.println("=== UNI STREAM " + streamChannel.streamId() + " ===");
                    System.out.println(io.netty.buffer.ByteBufUtil.prettyHexDump(buf));
                    System.out.println("ASCII: " +
                            buf.toString(buf.readerIndex(), buf.readableBytes(), CharsetUtil.UTF_8));
                }

                ctx.fireChannelRead(msg);
            }
        });
    }


}
