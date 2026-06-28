package io.github.webtransport4j.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.webtransport4j.api.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;

public class ConnectionMigrationIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ConnectionMigrationIntegrationTest.class);


    private NioEventLoopGroup serverGroup;
    private NioEventLoopGroup clientGroup;
    private Channel serverChannel;
    private int serverPort;
    private WebTransportServer webTransportServer;

    static class UdpProxy {
        private DatagramSocket clientFacing;
        private DatagramSocket serverFacing;
        private InetSocketAddress serverAddr;
        private InetSocketAddress lastKnownClientAddr;
        private volatile boolean running = true;
        
        public UdpProxy(int serverPort) throws Exception {
            clientFacing = new DatagramSocket(0);
            serverFacing = new DatagramSocket(0);
            serverAddr = new InetSocketAddress("127.0.0.1", serverPort);
            
            // Client -> Proxy -> Server
            new Thread(() -> {
                byte[] buf = new byte[65535];
                while(running) {
                    try {
                        DatagramPacket p = new DatagramPacket(buf, buf.length);
                        clientFacing.receive(p);
                        lastKnownClientAddr = new InetSocketAddress(p.getAddress(), p.getPort());
                        DatagramPacket fwd = new DatagramPacket(buf, p.getLength(), serverAddr);
                        serverFacing.send(fwd);
                    } catch(Exception e) {}
                }
            }).start();
            
            startServerToClientThread();
        }
        
        private void startServerToClientThread() {
            // Server -> Proxy -> Client
            new Thread(() -> {
                byte[] buf = new byte[65535];
                DatagramSocket currentSocket = serverFacing;
                while(running && currentSocket == serverFacing) {
                    try {
                        DatagramPacket p = new DatagramPacket(buf, buf.length);
                        currentSocket.receive(p);
                        if (lastKnownClientAddr != null) {
                            DatagramPacket fwd = new DatagramPacket(buf, p.getLength(), lastKnownClientAddr);
                            clientFacing.send(fwd);
                        }
                    } catch(Exception e) {}
                }
            }).start();
        }
        
        public int getProxyPort() { 
            return clientFacing.getLocalPort(); 
        }
        
        public void migrate() throws Exception {
            log.info("🔴 PROXY: Triggering Connection Migration. Dropping socket on port " + serverFacing.getLocalPort());
            DatagramSocket old = serverFacing;
            serverFacing = new DatagramSocket(0); // Binds to NEW random port!
            old.close(); // Interrupts the receiving thread
            log.info("🟢 PROXY: Bound new outgoing socket on port " + serverFacing.getLocalPort());
            startServerToClientThread();
        }
        
        public void stop() {
            running = false;
            if (clientFacing != null) clientFacing.close();
            if (serverFacing != null) serverFacing.close();
        }
    }

    @Before
    public void setUp() throws Exception {
        setUpServer();
    }

    private void setUpServer() throws Exception {
        System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
        webTransportServer = new WebTransportServer();
        
        serverGroup = new NioEventLoopGroup(1);
        clientGroup = new NioEventLoopGroup(1);

        io.netty.handler.ssl.util.SelfSignedCertificate ssc = new io.netty.handler.ssl.util.SelfSignedCertificate();
        QuicSslContext serverSslContext =
                QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
                        .applicationProtocols(Http3.supportedApplicationProtocols())
                        .build();

        Http3Settings serverSettings = new Http3Settings((id, value) -> true);
        serverSettings.enableH3Datagram(true);
        serverSettings.enableConnectProtocol(true);
        serverSettings.put(0x2c7cf000L, 1L); // wt_enabled
        serverSettings.put(0x2b64L, 100L);
        serverSettings.put(0x2b65L, 100L);
        serverSettings.put(0x2b61L, 1000000L);

        ChannelHandler serverCodec =
                Http3.newQuicServerCodecBuilder()
                        .sslContext(serverSslContext)
                        .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
                        .initialMaxData(10000000)
                        .initialMaxStreamDataBidirectionalLocal(1000000)
                        .initialMaxStreamDataBidirectionalRemote(1000000)
                        .initialMaxStreamsBidirectional(100)
                        .initialMaxStreamsUnidirectional(100)
                        .datagram(10000, 10000)
                        .tokenHandler(WebTransportServer.getTokenHandler())
                        .handler(
                                new ChannelInitializer<QuicChannel>() {
                                    @Override
                                    protected void initChannel(QuicChannel ch) {
                                        ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(webTransportServer);
                                        ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                                                .set(new WebTransportSessionManager());
                                        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
                                        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(100L);
                                        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(1000000L);
                                        ch.pipeline().addLast(new WebTransportDatagramDecoder());
                                        ch.pipeline().addLast(new WebTransportCapsuleHandler());
                                        ch.pipeline().addLast(new DefaultMessageDispatcher());
                                        ch.pipeline().addLast(
                                                        new Http3ServerConnectionHandler(
                                                                new ChannelInitializer<QuicStreamChannel>() {
                                                                    @Override
                                                                    protected void initChannel(QuicStreamChannel stream) {
                                                                        stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                                                        stream.pipeline().addLast(new RawWebTransportHandler());
                                                                        stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                                                        stream.pipeline().addLast(new WebTransportHeadersHandler());
                                                                        stream.pipeline().addLast(new Http3DataToByteBufHandler());
                                                                        stream.pipeline().addLast(new WebTransportCapsuleDecoder());
                                                                        stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                                                        stream.pipeline().addLast(new DefaultMessageDispatcher());
                                                                    }
                                                                },
                                                                new ChannelInboundHandlerAdapter() {},
                                                                (streamType) -> null,
                                                                new DefaultHttp3SettingsFrame(serverSettings),
                                                                true,
                                                                (id, value) -> true));
                                    }
                                })
                        .build();

        serverChannel =
                new Bootstrap()
                        .group(serverGroup)
                        .channel(NioDatagramChannel.class)
                        .handler(serverCodec)
                        .bind(new InetSocketAddress("127.0.0.1", 0))
                        .sync()
                        .channel();

        serverPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @After
    public void tearDown() throws Exception {
        if (serverChannel != null) serverChannel.close().sync();
        if (serverGroup != null) serverGroup.shutdownGracefully();
        if (clientGroup != null) clientGroup.shutdownGracefully();
    }

    @Test
    public void testConnectionMigration() throws Exception {
        CountDownLatch preMigrationLatch = new CountDownLatch(1);
        CountDownLatch postMigrationLatch = new CountDownLatch(1);
        
        AtomicReference<InetSocketAddress> preMigrationAddress = new AtomicReference<>();
        AtomicReference<InetSocketAddress> postMigrationAddress = new AtomicReference<>();
        
        AtomicReference<ChannelId> preMigrationChannelId = new AtomicReference<>();
        AtomicReference<ChannelId> postMigrationChannelId = new AtomicReference<>();

        // Server handler
        webTransportServer.registerHandler("/migrate", new WebTransportHandler() {
            @Override
            public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
                stream.onData(data -> {
                    String msg = new String(data.readBytes(), StandardCharsets.UTF_8);
                    log.info("SERVER: Received '" + msg + "'");
                    
                    QuicChannel quicChannel = (QuicChannel) ((NettyWebTransportStream) stream).streamChannel().parent();
                    InetSocketAddress remoteAddress = (InetSocketAddress) quicChannel.remoteSocketAddress();
                    
                    if (msg.contains("PING 1")) {
                        preMigrationAddress.set(remoteAddress);
                        preMigrationChannelId.set(quicChannel.id());
                        stream.writeText("PONG 1");
                    } else if (msg.contains("PING 2")) {
                        postMigrationAddress.set(remoteAddress);
                        postMigrationChannelId.set(quicChannel.id());
                        stream.writeText("PONG 2");
                    }
                });
            }
        });

        // 1. Start UDP Proxy
        UdpProxy proxy = new UdpProxy(serverPort);
        log.info("Proxy started on port " + proxy.getProxyPort() + " forwarding to Server port " + serverPort);

        // 2. Start Client connecting to PROXY
        QuicSslContext clientSslContext =
                QuicSslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .applicationProtocols(Http3.supportedApplicationProtocols())
                        .build();

        Http3Settings clientSettings = new Http3Settings((id, value) -> true);
        clientSettings.enableH3Datagram(true);
        clientSettings.enableConnectProtocol(true);
        clientSettings.put(0x2c7cf000L, 1L); 
        clientSettings.put(0x2b64L, 100L);
        clientSettings.put(0x2b65L, 100L);
        clientSettings.put(0x2b61L, 1000000L);

        Channel clientChannel =
                new Bootstrap()
                        .group(clientGroup)
                        .channel(NioDatagramChannel.class)
                        .handler(
                                Http3.newQuicClientCodecBuilder()
                                        .sslContext(clientSslContext)
                                        .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                                        .initialMaxData(10000000)
                                        .initialMaxStreamDataBidirectionalLocal(1000000)
                                        .initialMaxStreamDataBidirectionalRemote(1000000)
                                        .initialMaxStreamsBidirectional(100)
                                        .initialMaxStreamsUnidirectional(100)
                                        .datagram(10000, 10000)
                                        .build())
                        .bind(0)
                        .sync()
                        .channel();

        QuicChannel quicClient = QuicChannel.newBootstrap(clientChannel)
                        .handler(new ChannelInitializer<QuicChannel>() {
                            @Override
                            protected void initChannel(QuicChannel ch) {
                                ch.pipeline().addLast(new Http3ClientConnectionHandler(
                                        new ChannelInitializer<QuicStreamChannel>() {
                                            @Override
                                            protected void initChannel(QuicStreamChannel stream) {}
                                        },
                                        (streamType) -> null,
                                        (streamType) -> null,
                                        new DefaultHttp3SettingsFrame(clientSettings),
                                        false,
                                        (id, value) -> true));
                            }
                        })
                        .remoteAddress(new InetSocketAddress("127.0.0.1", proxy.getProxyPort()))
                        .connect().sync().getNow();

        // Handshake
        CountDownLatch handshakeLatch = new CountDownLatch(1);
        final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];
        Http3.newRequestStream(quicClient, new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof Http3HeadersFrame) {
                            Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                            if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                            }
                        }
                    }
                });
            }
        }).addListener((Future<QuicStreamChannel> f) -> {
            if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT").scheme("https").path("/migrate").authority("localhost").set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
            }
        });

        assertTrue("CONNECT handshake failed", handshakeLatch.await(5, TimeUnit.SECONDS));

        // Create Bi-Di stream
        final QuicStreamChannel[] bidiStream = new QuicStreamChannel[1];
        CountDownLatch streamLatch = new CountDownLatch(1);
        quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void handlerAdded(ChannelHandlerContext ctx) {
                        ctx.channel().eventLoop().execute(() -> {
                            java.util.List<String> toRemove = new java.util.ArrayList<>();
                            for (String name : ctx.pipeline().names()) {
                                ChannelHandler h = ctx.pipeline().get(name);
                                if (h != null && h != this && (name.contains("Http3") || h.getClass().getName().contains("Http3"))) {
                                    toRemove.add(name);
                                }
                            }
                            for (String name : toRemove) {
                                try { ctx.pipeline().remove(name); } catch (Exception e) {}
                            }
                        });
                    }
                });
                
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof ByteBuf) {
                            String resp = ((ByteBuf) msg).toString(StandardCharsets.UTF_8);
                            log.info("CLIENT: Received '" + resp + "'");
                            if (resp.contains("PONG 1")) preMigrationLatch.countDown();
                            if (resp.contains("PONG 2")) postMigrationLatch.countDown();
                            ((ByteBuf) msg).release();
                        }
                    }
                });
            }
        }).addListener((Future<QuicStreamChannel> f) -> {
            if (f.isSuccess()) {
                bidiStream[0] = f.getNow();
                streamLatch.countDown();
            }
        });

        assertTrue("Stream creation failed", streamLatch.await(5, TimeUnit.SECONDS));
        QuicStreamChannel stream = bidiStream[0];
        long sessionId = connectStream[0].streamId();

        // Send PING 1 (Pre-Migration)
        ByteBuf data1 = stream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(data1, 0x41); 
        WebTransportUtils.writeVarInt(data1, sessionId);
        data1.writeBytes("PING 1".getBytes(StandardCharsets.UTF_8));
        stream.writeAndFlush(data1);

        assertTrue("Failed to receive PONG 1 before migration", preMigrationLatch.await(5, TimeUnit.SECONDS));

        // MIGRATE (Simulate Network Change)
        proxy.migrate();
        Thread.sleep(500); // Give network a moment

        // Send PING 2 on SAME STREAM!
        ByteBuf data2 = stream.alloc().directBuffer();
        data2.writeBytes("PING 2".getBytes(StandardCharsets.UTF_8));
        stream.writeAndFlush(data2);

        assertTrue("Failed to receive PONG 2 AFTER migration! Connection Migration broke!", postMigrationLatch.await(5, TimeUnit.SECONDS));

        assertNotNull("Pre-migration address should not be null", preMigrationAddress.get());
        assertNotNull("Post-migration address should not be null", postMigrationAddress.get());
        assertNotEquals("The client's source port MUST change to prove connection migration!",
                preMigrationAddress.get().getPort(), postMigrationAddress.get().getPort());
                
        assertNotNull("Pre-migration ChannelId should not be null", preMigrationChannelId.get());
        assertEquals("The underlying QUIC Connection object MUST remain identical to prove no new handshake occurred!",
                preMigrationChannelId.get(), postMigrationChannelId.get());
        
        log.info("✅ VERIFIED: Server detected port change from " + 
                preMigrationAddress.get().getPort() + " to " + postMigrationAddress.get().getPort() + 
                " on EXACTLY the same QUIC Connection (ID: " + preMigrationChannelId.get() + ")");

        quicClient.close().sync();
        proxy.stop();
    }
}
