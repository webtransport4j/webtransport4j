package io.github.webtransport4j.incubator;

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
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static io.github.webtransport4j.incubator.WebTransportUtils.readVariableLengthInt;

public class WebTransportServer {
    private static final Logger logger = Logger.getLogger(WebTransportServer.class.getName());
    static int PORT = 4433;
    public static final AttributeKey<GlobalTrafficShapingHandler> CONN_TRAFFIC_SHAPER =
            AttributeKey.valueOf("wt.conn.traffic.shaper");
    static GlobalTrafficShapingHandler globalTrafficShaper;
    static final AttributeKey<String> SESSION_PATH_KEY = AttributeKey.valueOf("wt.session.path.key");

    public static final AttributeKey<java.util.concurrent.ExecutorService> BUSINESS_EXECUTOR = AttributeKey
            .valueOf("wt.business.executor");
    private static java.util.concurrent.ExecutorService businessExecutor;

    public static final AttributeKey<java.util.List<String>> ALLOWED_ORIGINS = AttributeKey
            .valueOf("wt.allowed.origins");
    private static java.util.List<String> allowedOrigins;

    public static void main(String[] args) throws Exception {
        PORT = WebTransportConfig.getInt("webtransport4j.server.port", 4433);
        String originsProp = WebTransportConfig.get("webtransport4j.allowed.origins", "*");
        allowedOrigins = java.util.Arrays.asList(originsProp.split(","));

        int poolSize = WebTransportConfig.getInt("webtransport4j.business.pool.size",
                Runtime.getRuntime().availableProcessors() * 2);
        int queueCapacity = WebTransportConfig.getInt("webtransport4j.business.queue.capacity", 10000);
        businessExecutor = new java.util.concurrent.ThreadPoolExecutor(
                poolSize,
                poolSize,
                60L, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(queueCapacity),
                new java.util.concurrent.ThreadFactory() {
                    private final java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(
                            1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "wt-business-worker-" + count.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Stopping executor...");
            if (globalTrafficShaper != null) {
                globalTrafficShaper.release();
            }
            businessExecutor.shutdown();
            try {
                if (!businessExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    businessExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                businessExecutor.shutdownNow();
            }
        }));

        logger.debug("🚀 STARTING DEBUG SERVER...");
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        long globalWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
        long globalReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
        if (globalWriteLimit > 0 || globalReadLimit > 0) {
            globalTrafficShaper = new GlobalTrafficShapingHandler(group, globalWriteLimit, globalReadLimit);
        }
        String keyPath = WebTransportConfig.get("webtransport4j.ssl.key.path",
                "/Users/sam/Documents/localhost-key.pem");
        String certPath = WebTransportConfig.get("webtransport4j.ssl.cert.path", "/Users/sam/Documents/localhost.pem");
        QuicSslContext sslContext = QuicSslContextBuilder.forServer(
                new File(keyPath),
                null,
                new File(certPath))
                .applicationProtocols(Http3.supportedApplicationProtocols())
                .build();
        String allowedProp = WebTransportConfig.get("webtransport4j.webtransport.settings.nonstandardallowed",
                "0x2c7cf000,0x2b64,0x2b65,0x2b61");
        Set<Long> allowed = new HashSet<>();
        for (String val : allowedProp.split(",")) {
            allowed.add(Long.decode(val.trim()));
        }

        Http3Settings settings = new Http3Settings(
                (id, value) -> allowed.contains(id));

        long wtMaxStreamsUni = WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.uni", 0L);
        long wtMaxStreamsBidi = WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.bidi", 0L);
        long wtInitialMaxData = WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.data", 0L);

        long quicMaxStreamsUni = WebTransportConfig.getLong("webtransport4j.quic.max.streams.uni", 0L);
        long quicMaxStreamsBidi = WebTransportConfig.getLong("webtransport4j.quic.max.streams.bidi", 0L);
        long quicInitialMaxData = WebTransportConfig.getLong("webtransport4j.quic.initial.max.data", 0L);

        // Validate that QUIC limits are not lesser than WebTransport initial session
        // limits
        validateConfig(quicMaxStreamsBidi, wtMaxStreamsBidi, quicMaxStreamsUni, wtMaxStreamsUni, quicInitialMaxData,
                wtInitialMaxData);

        settings.enableH3Datagram(
                WebTransportConfig.getBoolean("webtransport4j.webtransport.settings.enable_h3_datagram", false));
        settings.enableConnectProtocol(
                WebTransportConfig.getBoolean("webtransport4j.webtransport.settings.enable_connect_protocol", false));

        // SETTINGS_WT_ENABLED (0x2c7cf000) - draft-15
        settings.put(0x2c7cf000L,
                WebTransportConfig.getLong("webtransport4j.webtransport.settings.wt_enabled.value", 0L));
        // SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0x2b64) - draft-15
        settings.put(0x2b64L, wtMaxStreamsUni);
        // SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65) - draft-15
        settings.put(0x2b65L, wtMaxStreamsBidi);
        // SETTINGS_WT_INITIAL_MAX_DATA (0x2b61) - draft-15
        settings.put(0x2b61L, wtInitialMaxData);
        logger.info("Server side settings : " + settings);
        ChannelHandler serverCodec = Http3.newQuicServerCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(WebTransportConfig.getInt("webtransport4j.quic.idle.timeout.seconds", 0),
                        TimeUnit.SECONDS)
                .initialMaxData(quicInitialMaxData)
                .initialMaxStreamDataBidirectionalLocal(
                        WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.local", 0L))
                .initialMaxStreamDataBidirectionalRemote(
                        WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.remote", 0L))
                .initialMaxStreamsBidirectional(quicMaxStreamsBidi)
                .datagram(
                        WebTransportConfig.getInt("webtransport4j.quic.datagram.recv.queue.len", 0),
                        WebTransportConfig.getInt("webtransport4j.quic.datagram.send.queue.len", 0))
                .initialMaxStreamsUnidirectional(quicMaxStreamsUni)
                .initialMaxStreamDataUnidirectional(
                        WebTransportConfig.getLong("webtransport4j.quic.stream.data.uni", 0L))
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .handler(new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                        logger.debug("Opening quic connection " + ch.id());
                        long defUni = settings.get(0x2b64L) == null ? 0L : settings.get(0x2b64L);
                        long defBidi = settings.get(0x2b65L) == null ? 0L : settings.get(0x2b65L);
                        long defData = settings.get(0x2b61L) == null ? 0L : settings.get(0x2b61L);
                        ch.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(defUni);
                        ch.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(defBidi);
                        ch.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_DATA).set(defData);
 
                        long connWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.connection.write.limit", 0L);
                        long connReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.connection.read.limit", 0L);
                        if (connWriteLimit > 0 || connReadLimit > 0) {
                            GlobalTrafficShapingHandler connShaper = new GlobalTrafficShapingHandler(ch.eventLoop(), connWriteLimit, connReadLimit);
                            ch.attr(CONN_TRAFFIC_SHAPER).set(connShaper);
                            ch.closeFuture().addListener(f -> connShaper.release());
                        }

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
                        ch.attr(BUSINESS_EXECUTOR).set(businessExecutor);
                        ch.attr(ALLOWED_ORIGINS).set(allowedOrigins);
                        ch.pipeline().addLast(new WebTransportDatagramHandler());
                        logger.debug("🔧 Added WebTransportDatagramHandler. Pipeline now: " + ch.pipeline().names());
                        ch.pipeline().addLast(new WebTransportCapsuleHandler());
                        logger.debug("🔧 Added WebTransportCapsuleHandler. Pipeline now: " + ch.pipeline().names());
                        ch.pipeline().addLast(new MessageDispatcher());
                        logger.debug("🔧 Added MessageDispatcher. Pipeline now: " + ch.pipeline().names());
                        ch.pipeline().addLast(new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {
                                        QuicChannel quic = stream.parent();
                                        addTrafficShapers(stream);

                                        String path = quic.attr(SESSION_PATH_KEY).get();
                                        boolean isSocketIo = (path != null && path.contains("socket.io"));

                                        stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                        logger.debug("🔧 Added WebTransportDetectorHandler. Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline()
                                                .addFirst(new QuicGlobalSniffer("STREAM-" + stream.streamId()));
                                        logger.debug("🔧 Added QuicGlobalSniffer (per‑stream). Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline().addLast(new RawWebTransportHandler());
                                        logger.debug("🔧 Added RawWebTransportHandler. Pipeline now: "
                                                + stream.pipeline().names());
                                        if (isSocketIo) {
                                            stream.pipeline().addLast(new EngineIoFrameDecoder());
                                            logger.debug("🔧 Added EngineIoFrameDecoder. Pipeline now: "
                                                    + stream.pipeline().names());
                                        }
                                        stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                        logger.debug("🔧 Added WebTransportStreamFrameDecoder. Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline().addLast(new WebTransportHeadersHandler());
                                        logger.debug("🔧 Added WebTransportHeadersHandler. Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline().addLast(new WebTransportDataHandler());
                                        logger.debug("🔧 Added WebTransportDataHandler. Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                        logger.debug("🔧 Added WebTransportCapsuleHandler. Pipeline now: "
                                                + stream.pipeline().names());
                                        stream.pipeline().addLast(new MessageDispatcher());
                                        logger.debug("🔧 Added MessageDispatcher. Pipeline now: "
                                                + stream.pipeline().names());
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
                                new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(
                                            ChannelHandlerContext ctx,
                                            Object msg) {
                                        if (msg instanceof Http3SettingsFrame) {
                                            Http3SettingsFrame settingsFrame = (Http3SettingsFrame) msg;
                                            logger.debug("PEER SETTINGS: " + settingsFrame);
                                            io.netty.handler.codec.http3.Http3Settings settings = settingsFrame
                                                    .settings();
                                            if (settings != null) {
                                                QuicChannel quic = null;
                                                if (ctx.channel() instanceof QuicStreamChannel) {
                                                    quic = ((QuicStreamChannel) ctx.channel()).parent();
                                                } else if (ctx.channel() instanceof QuicChannel) {
                                                    quic = (QuicChannel) ctx.channel();
                                                }

                                                // Section 5.1: Verify required setting SETTINGS_H3_DATAGRAM (0x33) is
                                                // enabled (1)
                                                if (!settings.h3DatagramEnabled()) {
                                                    logger.warn(
                                                            "❌ WebTransport requirements not met: Client does not support H3 Datagrams. Closing connection with WT_REQUIREMENTS_NOT_MET (0x61616164)");
                                                    if (quic != null) {
                                                        quic.close(true, 0x212c0d48,
                                                                io.netty.buffer.Unpooled.EMPTY_BUFFER);
                                                    } else {
                                                        ctx.close();
                                                    }
                                                    io.netty.util.ReferenceCountUtil.release(msg);
                                                    return;
                                                }

                                                if (quic != null) {
                                                    quic.attr(WebTransportConfig.PEER_SETTINGS_MAX_STREAMS_UNI)
                                                            .set(settings.get(0x2b64L));
                                                    quic.attr(WebTransportConfig.PEER_SETTINGS_MAX_STREAMS_BIDI)
                                                            .set(settings.get(0x2b65L));
                                                    quic.attr(WebTransportConfig.PEER_SETTINGS_MAX_DATA)
                                                            .set(settings.get(0x2b61L));
                                                }
                                            }
                                        }
                                        io.netty.util.ReferenceCountUtil.release(msg);
                                    }

                                },
                                (streamType) -> {
                                    if (streamType == 0x54) {
                                        return new ChannelInitializer<QuicStreamChannel>() {
                                            @Override
                                            protected void initChannel(QuicStreamChannel ch) {
                                                addTrafficShapers(ch);
                                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                                    private boolean sessionHeaderRead = false;

                                                    @Override
                                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                                        if (msg instanceof ByteBuf) {
                                                            ByteBuf data = (ByteBuf) msg;
                                                            if (!sessionHeaderRead) {
                                                                long sessionId = readVariableLengthInt(data);
                                                                ctx.channel().attr(WebTransportUtils.SESSION_ID_KEY)
                                                                        .set(sessionId);
                                                                sessionHeaderRead = true;
                                                            }
                                                            if (!data.isReadable()) {
                                                                data.release();
                                                                return;
                                                            }
                                                            String savedPath = ctx.channel().parent()
                                                                    .attr(WebTransportServer.SESSION_PATH_KEY).get();
                                                            ctx.channel().attr(WebTransportUtils.STREAM_TYPE_KEY)
                                                                    .set(streamType);
                                                            ctx.channel().attr(WebTransportServer.SESSION_PATH_KEY)
                                                                    .set(savedPath);
                                                            ctx.fireChannelRead(msg);
                                                        } else {
                                                            ctx.fireChannelRead(msg);
                                                        }
                                                    }
                                                });
                                                ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                                ch.pipeline().addLast(new WebTransportCapsuleHandler());
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

    static void validateConfig(long quicMaxBidi, long wtMaxBidi, long quicMaxUni, long wtMaxUni, long quicMaxData,
            long wtMaxData) {
        if (quicMaxBidi < wtMaxBidi) {
            throw new IllegalArgumentException("Configuration Mismatch: quic.max.streams.bidi (" + quicMaxBidi
                    + ") must be greater than or equal to webtransport.initial.max.streams.bidi (" + wtMaxBidi + ")");
        }
        if (quicMaxUni < wtMaxUni) {
            throw new IllegalArgumentException("Configuration Mismatch: quic.max.streams.uni (" + quicMaxUni
                    + ") must be greater than or equal to webtransport.initial.max.streams.uni (" + wtMaxUni + ")");
        }
        if (quicMaxData < wtMaxData) {
            throw new IllegalArgumentException("Configuration Mismatch: quic.initial.max.data (" + quicMaxData
                    + ") must be greater than or equal to webtransport.initial.max.data (" + wtMaxData + ")");
        }
    }

    static void addTrafficShapers(QuicStreamChannel stream) {
        QuicChannel quic = stream.parent();
        if (globalTrafficShaper != null) {
            stream.pipeline().addFirst("global-traffic-shaper", globalTrafficShaper);
            logger.debug("🔧 Added global traffic shaper to stream " + stream.streamId());
        }
        GlobalTrafficShapingHandler connShaper = quic.attr(CONN_TRAFFIC_SHAPER).get();
        if (connShaper != null) {
            stream.pipeline().addFirst("conn-traffic-shaper", connShaper);
            logger.debug("🔧 Added connection traffic shaper to stream " + stream.streamId());
        }
        long streamWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.stream.write.limit", 0L);
        long streamReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.stream.read.limit", 0L);
        if (streamWriteLimit > 0 || streamReadLimit > 0) {
            ChannelTrafficShapingHandler streamShaper = new ChannelTrafficShapingHandler(streamWriteLimit, streamReadLimit);
            stream.pipeline().addFirst("stream-traffic-shaper", streamShaper);
            logger.debug("🔧 Added stream traffic shaper to stream " + stream.streamId());
        }
    }
}