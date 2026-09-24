package io.github.webtransport4j.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;
import io.netty.util.NetUtil;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class Http3GetClient {

    public static void main(String[] args) throws Exception {

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        try {
            QuicSslContext context = QuicSslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(Http3.supportedApplicationProtocols())
                    .earlyData(true) // Enables early-data capability
                    .build();

            ChannelHandler codec = Http3.newQuicClientCodecBuilder()
                    .sslContext(context)
                    .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                    .initialMaxData(Integer.MAX_VALUE)
                    .initialMaxStreamDataBidirectionalLocal(Integer.MAX_VALUE)
                    .initialMaxStreamDataBidirectionalRemote(Integer.MAX_VALUE)
                    .initialMaxStreamsBidirectional(100)
                    .initialMaxStreamsUnidirectional(100)
                    .build();

            Channel udp = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(codec)
                    .bind(0)
                    .sync()
                    .channel();

            Http3Settings settings = new Http3Settings((id, value) -> true);
            settings.enableConnectProtocol(true);
            settings.enableH3Datagram(true);
            QuicChannel quicChannel = QuicChannel.newBootstrap(udp)
                    .handler(new WebTransportClientHandler(new DefaultHttp3SettingsFrame(settings), true, (id, value) -> true))
                    .remoteAddress(new InetSocketAddress(NetUtil.LOCALHOST4, 4433))
                    .connect()
                    .get();

            QuicStreamChannel requestStream = Http3.newRequestStream(
                    quicChannel,
                    new SimpleChannelInboundHandler<Object>() {

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {

                            if (msg instanceof Http3HeadersFrame) {
                                System.out.println("STATUS = "
                                        + ((Http3HeadersFrame) msg).headers().status());
                            } else if (msg instanceof Http3DataFrame) {
                                System.out.println("BODY = "
                                        + ((Http3DataFrame) msg).content().toString(CharsetUtil.UTF_8));
                            } else {
                                System.out.println(msg);
                            }


                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx,
                                                    Throwable cause) {
                            cause.printStackTrace();
                            ctx.close();
                        }
                    }).sync().getNow();

            Http3HeadersFrame request = new DefaultHttp3HeadersFrame();
            request.headers()
                    .method("GET")
                    .scheme("https")
                    .authority(NetUtil.LOCALHOST4.getHostAddress() + ":4433")
                    .path("/");

            requestStream.writeAndFlush(request).sync();

            Thread.sleep(3000);

            requestStream.close().sync().addListener(f -> {
                if (f.isSuccess()) {
                    System.out.println("Request stream closed successfully.");
                } else {
                    System.err.println("Failed to close request stream: " + f.cause());
                }
            });
            quicChannel.close().sync().addListener(
                    f -> {
                        if (f.isSuccess()) {
                            System.out.println("QUIC channel closed successfully.");
                        } else {
                            System.err.println("Failed to close QUIC channel: " + f.cause());
                        }
                    }
            );
            udp.close().sync().addListener(
                    f -> {
                        if (f.isSuccess()) {
                            System.out.println("UDP channel closed successfully.");
                        } else {
                            System.err.println("Failed to close UDP channel: " + f.cause());
                        }
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}