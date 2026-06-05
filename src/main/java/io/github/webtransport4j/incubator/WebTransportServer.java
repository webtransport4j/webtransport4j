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
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
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
    static final AttributeKey<String> SESSION_PATH_KEY = AttributeKey.valueOf("wt.session.path.key");

    public static final AttributeKey<java.util.concurrent.ExecutorService> BUSINESS_EXECUTOR = AttributeKey.valueOf("wt.business.executor");
    private static java.util.concurrent.ExecutorService businessExecutor;

    public static final AttributeKey<java.util.List<String>> ALLOWED_ORIGINS = AttributeKey.valueOf("wt.allowed.origins");
    private static java.util.List<String> allowedOrigins;

    public static void main(String[] args) throws Exception {
        PORT = WebTransportConfig.getInt("webtransport4j.server.port", 4433);
        String originsProp = WebTransportConfig.get("webtransport4j.allowed.origins", "*");
        allowedOrigins = java.util.Arrays.asList(originsProp.split(","));

        int poolSize = WebTransportConfig.getInt("webtransport4j.business.pool.size", Runtime.getRuntime().availableProcessors() * 2);
        int queueCapacity = WebTransportConfig.getInt("webtransport4j.business.queue.capacity", 10000);
        businessExecutor = new java.util.concurrent.ThreadPoolExecutor(
                poolSize,
                poolSize,
                60L, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(queueCapacity),
                new java.util.concurrent.ThreadFactory() {
                    private final java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "wt-business-worker-" + count.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy()
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Stopping executor...");
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
        String keyPath = WebTransportConfig.get("webtransport4j.ssl.key.path", "/Users/sam/Documents/localhost-key.pem");
        String certPath = WebTransportConfig.get("webtransport4j.ssl.cert.path", "/Users/sam/Documents/localhost.pem");
        QuicSslContext sslContext = QuicSslContextBuilder.forServer(
                new File(keyPath),
                null,
                new File(certPath))
                .applicationProtocols(Http3.supportedApplicationProtocols())
                .build();
        String allowedProp = WebTransportConfig.get("webtransport4j.webtransport.settings.allowed", "0x2c7cf000,0x2b64,0x2b65,0x2b61");
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

        // Validate that QUIC limits are not lesser than WebTransport initial session limits
        validateConfig(quicMaxStreamsBidi, wtMaxStreamsBidi, quicMaxStreamsUni, wtMaxStreamsUni, quicInitialMaxData, wtInitialMaxData);

        settings.enableH3Datagram(WebTransportConfig.getBoolean("webtransport4j.webtransport.settings.enable_h3_datagram", false));
        settings.enableConnectProtocol(WebTransportConfig.getBoolean("webtransport4j.webtransport.settings.enable_connect_protocol", false));

        // SETTINGS_WT_ENABLED (0x2c7cf000) - draft-15
        settings.put(0x2c7cf000L, WebTransportConfig.getLong("webtransport4j.webtransport.settings.wt_enabled.value", 0L));
        //SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0x2b64) - draft-15
        settings.put(0x2b64L, wtMaxStreamsUni);
        //SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65) - draft-15
        settings.put(0x2b65L, wtMaxStreamsBidi);
        //SETTINGS_WT_INITIAL_MAX_DATA (0x2b61) - draft-15
        settings.put(0x2b61L, wtInitialMaxData);

        ChannelHandler serverCodec = Http3.newQuicServerCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(WebTransportConfig.getInt("webtransport4j.quic.idle.timeout.seconds", 0), TimeUnit.SECONDS)
                .initialMaxData(quicInitialMaxData)
                .initialMaxStreamDataBidirectionalLocal(WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.local", 0L))
                .initialMaxStreamDataBidirectionalRemote(WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.remote", 0L))
                .initialMaxStreamsBidirectional(quicMaxStreamsBidi)
                .datagram(
                        WebTransportConfig.getInt("webtransport4j.quic.datagram.recv.queue.len", 0),
                        WebTransportConfig.getInt("webtransport4j.quic.datagram.send.queue.len", 0))
                .initialMaxStreamsUnidirectional(quicMaxStreamsUni)
                .initialMaxStreamDataUnidirectional(WebTransportConfig.getLong("webtransport4j.quic.stream.data.uni", 0L))
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .handler(new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                        long defUni = settings.get(0x2b64L) == null ? 0L : settings.get(0x2b64L);
                        long defBidi = settings.get(0x2b65L) == null ? 0L : settings.get(0x2b65L);
                        long defData = settings.get(0x2b61L) == null ? 0L : settings.get(0x2b61L);
                        ch.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_UNI).set(defUni);
                        ch.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_BIDI).set(defBidi);
                        ch.attr(WebTransportUtils.SETTINGS_MAX_DATA).set(defData);
                        
                        
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
                        ch.pipeline().addLast(new MessageDispatcher());
                        logger.debug("🔧 Added MessageDispatcher. Pipeline now: " + ch.pipeline().names());
                        ch.pipeline().addLast(new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {
                                        QuicChannel quic = stream.parent();
                                        
                                        String path = quic.attr(SESSION_PATH_KEY).get();
                                        boolean isSocketIo = (path != null && path.contains("socket.io"));

                                        stream.pipeline().addFirst(new WebTransportDetectorHandler());
logger.debug("🔧 Added WebTransportDetectorHandler. Pipeline now: " + stream.pipeline().names());
                                        stream.pipeline()
                                                .addFirst(new QuicGlobalSniffer("STREAM-" + stream.streamId()));
logger.debug("🔧 Added QuicGlobalSniffer (per‑stream). Pipeline now: " + stream.pipeline().names());
                                        stream.pipeline().addLast(new RawWebTransportHandler());
logger.debug("🔧 Added RawWebTransportHandler. Pipeline now: " + stream.pipeline().names());
                                        if (isSocketIo) {
                                            stream.pipeline().addLast(new EngineIoFrameDecoder());
logger.debug("🔧 Added EngineIoFrameDecoder. Pipeline now: " + stream.pipeline().names());
                                        }
                                        stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
logger.debug("🔧 Added WebTransportStreamFrameDecoder. Pipeline now: " + stream.pipeline().names());
                                        stream.pipeline().addLast(new WebTransportHeadersHandler());
logger.debug("🔧 Added WebTransportHeadersHandler. Pipeline now: " + stream.pipeline().names());
                                        stream.pipeline().addLast(new WebTransportDataHandler());
logger.debug("🔧 Added WebTransportDataHandler. Pipeline now: " + stream.pipeline().names());
                                        stream.pipeline().addLast(new MessageDispatcher());
logger.debug("🔧 Added MessageDispatcher. Pipeline now: " + stream.pipeline().names());
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
                                                                long sessionId = readVariableLengthInt(data);
                                                                ctx.channel().attr(WebTransportUtils.SESSION_ID_KEY).set(sessionId);
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

    static void validateConfig(long quicMaxBidi, long wtMaxBidi, long quicMaxUni, long wtMaxUni, long quicMaxData, long wtMaxData) {
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
}