package io.github.webtransport4j.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author https://github.com/sanjomo
 * @date 02/07/26 9:59 pm
 */
public final class QuicServerReadExample {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getInstance(QuicServerReadExample.class);

    private QuicServerReadExample() { }

    public static void main(String[] args) throws Exception {
        SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
        QuicSslContext context = QuicSslContextBuilder.forServer(
                        selfSignedCertificate.privateKey(), null, selfSignedCertificate.certificate())
                .applicationProtocols(Http3.supportedApplicationProtocols()).build();
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        ChannelHandler codec = new QuicServerCodecBuilder().sslContext(context)
                .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                // Configure some limits for the maximal number of streams (and the data) that we want to handle.
                .initialMaxData(1000000000)
                .initialMaxStreamDataBidirectionalRemote(1000000000)
                .initialMaxStreamsBidirectional(100)
                .initialMaxStreamsUnidirectional(100)

                // Setup a token handler. In a production system you would want to implement and provide your custom
                // one.
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                // ChannelHandler that is added into QuicChannel pipeline.
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) {
                        ((QuicChannel) ctx.channel()).collectStats().addListener(f -> {
                            if (f.isSuccess()) {
                                LOGGER.info("Connection closed: {}", f.getNow());
                            }
                        });
                    }

                    @Override
                    public boolean isSharable() {
                        return true;
                    }
                })
                .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch)  {
                        // Add a LineBasedFrameDecoder here as we just want to do some simple HTTP 0.9 handling.
                        ch.pipeline()
                                .addLast(new ChannelInboundHandlerAdapter() {
                                    private long start;
                                    private long received;
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                        ByteBuf buf=(ByteBuf) msg;
                                        received+=buf.readableBytes();
                                        //System.out.println("received "+ received);
                                        ReferenceCountUtil.release(msg);
                                    }

                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) {
                                        start = System.nanoTime();
                                        ctx.fireChannelActive();
                                    }

                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                        ctx.fireUserEventTriggered(evt);
                                        if (evt instanceof ChannelInputShutdownReadComplete) {
                                            // We received the FIN of the remove peer. This means everything was read. Let's call close() so we also send the FIN.
                                            System.err.println("It takes time to read: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
                                            ctx.close();
                                        }
                                    }
                                });
                    }
                })
                .build();
        try {
            FixedRecvByteBufAllocator recvByteBufAllocator = new FixedRecvByteBufAllocator(2048);
            recvByteBufAllocator.maxMessagesPerRead(Integer.MAX_VALUE);
            Bootstrap bs = new Bootstrap();
            Channel channel = bs.group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(codec)
                    .option(ChannelOption.RECVBUF_ALLOCATOR, recvByteBufAllocator)
                    .bind(new InetSocketAddress(4242)).sync().channel();
            channel.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }}