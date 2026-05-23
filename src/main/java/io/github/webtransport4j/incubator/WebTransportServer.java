package io.github.webtransport4j.incubator;

import io.github.webtransport4j.incubator.applayer.ServerPushService;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3UnknownFrame;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import static io.github.webtransport4j.incubator.WebTransportUtils.readVariableLengthInt;

public class WebTransportServer {
    private static final Logger logger = Logger.getLogger(WebTransportServer.class.getName());
    static final int PORT = 4433;
    static final AttributeKey<String> SESSION_PATH_KEY = AttributeKey.valueOf("wt.session.path.key");

    public static void main(String[] args) throws Exception {
        logger.debug("🚀 STARTING DEBUG SERVER...");
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        QuicSslContext sslContext = QuicSslContextBuilder.forServer(
                new File("/Users/sam/Documents/localhost-key.pem"),
                null,
                new File("/Users/sam/Documents/localhost.pem"))
                .applicationProtocols(Http3.supportedApplicationProtocols())
                .build();
        Set<Long> allowed = new HashSet<>(
                Arrays.asList(0x2b603742L)
        );

        Http3Settings settings =
                new Http3Settings(
                        (id, value) -> allowed.contains(id)
                );

        settings.enableH3Datagram(true);
        settings.enableConnectProtocol(true);

// SETTINGS_WEBTRANSPORT_MAX_SESSIONS
        settings.put(0x2b603742L, 1L);

        ChannelHandler serverCodec = Http3.newQuicServerCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(30, TimeUnit.SECONDS)
                .initialMaxData(10_000_000)
                .initialMaxStreamDataBidirectionalLocal(1_000_000)
                .initialMaxStreamDataBidirectionalRemote(1_000_000)
                .initialMaxStreamsBidirectional(100)
                .datagram(10000, 10000)
                .initialMaxStreamsUnidirectional(100)
                .initialMaxStreamDataUnidirectional(1_000_000)
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .handler(new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                        ch.pipeline().addFirst(new QuicGlobalSniffer("GLOBAL-CONN"));
                        InetSocketAddress remote = (InetSocketAddress) ch.remoteSocketAddress();
                        String ip = remote.getAddress().getHostAddress();
                        int port = remote.getPort();
                        String nettyId = ch.id().asShortText();
                        // 2. PRINT NICE LOG
                        logger.debug("\n🔌 NEW QUIC CONNECTION ESTABLISHED");
                        logger.debug("    ├── 🌍 Remote IP:   " + ip);
                        logger.debug("    ├── 🚪 Remote Port: " + port);
                        logger.debug("    └── 🆔 Channel ID:  " + nettyId);
                        ch.attr(WebTransportSessionManager.WT_SESSION_MGR).set(new WebTransportSessionManager());
                        ch.pipeline().addLast(new WebTransportDatagramHandler());
                        ch.pipeline().addLast(new MessageDispatcher());
                        //ch.pipeline().addLast(new WebTransportMessageDispatcher());
                        ch.pipeline().addLast(new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {
              ;                          // DEBUG: Print when a stream is created
                                        // logger.debug("🌊 Stream Created: " + stream.id());
                                        QuicChannel quic = stream.parent();
                                        WebTransportSessionManager mgr = quic
                                                .attr(WebTransportSessionManager.WT_SESSION_MGR).get();
                                        String path = quic.attr(SESSION_PATH_KEY).get();
                                        boolean isSocketIo = (path != null && path.contains("socket.io"));

                                        stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                        stream.pipeline().addLast(new RawWebTransportHandler());
                                        if (isSocketIo) {
                                            stream.pipeline().addLast(new EngineIoFrameDecoder());
                                        }
                                        stream.pipeline().addLast(new MessageDispatcher());
                                        stream.pipeline().addLast(new Http3RequestStreamInboundHandler() {
                                            @Override
                                            protected void channelRead(ChannelHandlerContext ctx,
                                                    Http3HeadersFrame frame) {
                                                logger.debug("=== [DEBUG] Received HTTP/3 Headers ===");

                                                // Loop through all headers and print them
                                                for (Map.Entry<CharSequence, CharSequence> header : frame.headers()) {
                                                    logger.debug(header.getKey() + ": " + header.getValue());
                                                }

                                                logger.debug("=======================================");
                                                logger.debug("📜 HTTP/3 Headers Received: " + frame.headers().path());
                                                CharSequence path = frame.headers().path();
                                                CharSequence method = frame.headers().method();
                                                CharSequence protocol = frame.headers().get(":protocol");

                                                if ("CONNECT".contentEquals(method)
                                                        && "webtransport".contentEquals(protocol)) {
                                                    String pathStr = path.toString();
                                                    ctx.channel().parent().attr(SESSION_PATH_KEY).set(pathStr);
                                                    logger.debug("✅ Handshake Success for Path: " + pathStr);
                                                    Http3Headers responseHeaders = new DefaultHttp3Headers();
                                                    responseHeaders.status("200");
                                                    responseHeaders.add("sec-webtransport-http3-draft", "draft02");
                                                    // PURE HTTP/3 Frame. No manual byte writing here!
                                                    ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
                                                    
                                                    QuicStreamChannel connectStream = (QuicStreamChannel) ctx.channel();
                                                    mgr.register(connectStream);
                                                    
                                                    boolean isConnectSocketIo = pathStr.contains("socket.io");
                                                    if (!isConnectSocketIo) {
                                                        long sessionId = connectStream.streamId();

                                                        // Trigger server initiated uni-stream
                                                        logger.debug(
                                                                "⏰ Creating Server-Push Stream for Session " + sessionId);
                                                        // new ServerPushService(quic, sessionId).startPushing();
                                                        logger.debug("⏳ Creating Push Stream...");
                                                        String key = "session-" + connectStream.id().asShortText();

                                                        WebTransportUtils.createUniStream(quic, sessionId, key)
                                                                .addListener(future -> {
                                                                    if (future.isSuccess()) {
                                                                        logger.debug("🚀 Push Stream Ready!");

                                                                        // this is for testing, remove this, just poc

                                                                        quic.eventLoop().scheduleAtFixedRate(() -> {
                                                                            if (connectStream.isActive()) {
                                                                                ServerPushService.INSTANCE.sendTo(key,
                                                                                        String.valueOf(System.nanoTime()));
                                                                            }
                                                                        }, 0, 1, TimeUnit.SECONDS);

                                                                        connectStream.closeFuture().addListener(f -> {
                                                                            ServerPushService.INSTANCE.unregister(key);
                                                                        });
                                                                    } else {
                                                                        System.err.println("❌ Failed: " + future.cause());
                                                                    }
                                                                });
                                                    }
                                                }
                                                ReferenceCountUtil.release(frame);
                                            }

                                            @Override
                                            protected void channelRead(ChannelHandlerContext ctx,
                                                    Http3DataFrame frame) {
                                                ByteBuf payload = frame.content();
                                                try {
                                                    while (payload.isReadable()) {
                                                        long capType = readVariableLengthInt(payload);
                                                        if (capType == -1) break;
                                                        long capLen = readVariableLengthInt(payload);
                                                        if (capLen == -1 || payload.readableBytes() < capLen) break;
                                                        
                                                        ByteBuf capVal = payload.readSlice((int) capLen);
                                                        if (logger.isDebugEnabled()) {
                                                            logger.debug(String.format("💊 Received Capsule | Type: 0x%X | Length: %d | Hex: %s", 
                                                                    capType, capLen, io.netty.buffer.ByteBufUtil.hexDump(capVal)));
                                                        }
                                                        
                                                        if (capType == 0x2843) {
                                                            logger.info("❌ CLOSE_WEBTRANSPORT_SESSION Capsule received. Closing session.");
                                                            ctx.close();
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    logger.error("Error parsing capsules on CONNECT stream", e);
                                                } finally {
                                                    ReferenceCountUtil.release(frame);
                                                }
                                            }

                                            @Override
                                            protected void channelRead(ChannelHandlerContext ctx,
                                                    Http3UnknownFrame frame) {
                                                ctx.fireChannelRead(frame);
                                            }

                                            @Override
                                            protected void channelInputClosed(ChannelHandlerContext ctx) {
                                                logger.debug("🔒 Stream Closed: " + ctx.channel().id());
                                                ctx.close();
                                            }
                                        });
                                        // stream.pipeline().addLast(new WebTransportMessageDispatcher()); // Removed to
                                        // correct pipeline order
                                        // DEBUG: Catch-all exception handler
                                        stream.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                            @Override
                                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                                System.err.println("❌ PIPELINE ERROR: " + cause.getMessage());
                                                cause.printStackTrace();
                                            }
                                        });
                                    }
                                },
                                null,
                                (streamType) -> {
                                    if (streamType == 0x54) {
                                        return new ChannelInitializer<QuicStreamChannel>() {
                                            @Override
                                            protected void initChannel(QuicStreamChannel ch) {
                                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                                    private boolean sessionHeaderRead = false;

                                                    @Override
                                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                                        if (msg instanceof ByteBuf) {
                                                            ByteBuf data = (ByteBuf) msg;
                                                            if (!sessionHeaderRead) {
                                                                readVariableLengthInt(data);
                                                                sessionHeaderRead = true;
                                                            }
                                                            if (!data.isReadable()) {
                                                                data.release();
                                                                return;
                                                            }
                                                            String savedPath = ctx.channel().parent()
                                                                    .attr(WebTransportServer.SESSION_PATH_KEY).get();
                                                            ctx.channel().attr(WebTransportUtils.STREAM_TYPE_KEY).set(streamType);
                                                            ctx.channel().attr(WebTransportServer.SESSION_PATH_KEY).set(savedPath);
                                                            ctx.fireChannelRead(msg);
                                                        } else {
                                                            ctx.fireChannelRead(msg);
                                                        }
                                                    }
                                                });
                                                ch.pipeline().addLast(new MessageDispatcher());
                                            }
                                        };
                                    }
                                    return null;
                                }, new DefaultHttp3SettingsFrame(settings), true));
                    }
                }).build();
        Channel ch = new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .handler(serverCodec)
                .bind(new InetSocketAddress(PORT))
                .sync()
                .channel();
        logger.debug("✅ WebTransport server listening on " + PORT);
        ch.closeFuture().sync();
    }
}