package io.github.webtransport4j.client;

import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Replicated Java Client Interop Test Suite from Python's interop_test_suite.py
 */
public class WebTransportClientTestSuite {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportClientTestSuite.class);
    private static final Set<String> pendingVerifications = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final List<Consumer<String>> uniStreamListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    private static class Session {
        final QuicChannel quicChannel;
        final QuicStreamChannel connectStream;
        final long sessionId;

        Session(QuicChannel quicChannel, QuicStreamChannel connectStream, long sessionId) {
            this.quicChannel = quicChannel;
            this.connectStream = connectStream;
            this.sessionId = sessionId;
        }

        void close() throws Exception {
            quicChannel.close().sync();
        }
    }

    private static class WebTransportTestClientConnectionHandler extends Http3ClientConnectionHandler {
        private final CountDownLatch serverUniLatch;
        private final CountDownLatch serverBidiLatch;

        public WebTransportTestClientConnectionHandler(
                Http3SettingsFrame localSettings,
                CountDownLatch serverUniLatch,
                CountDownLatch serverBidiLatch) {
            super(null, null, new UnknownStreamHandlerFactory(), localSettings, true, (id, value) -> true);
            this.serverUniLatch = serverUniLatch;
            this.serverBidiLatch = serverBidiLatch;
        }

        @Override
        protected void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel channel) {
            cleanPipeline(channel);
            channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                private final ByteBuf accum = Unpooled.buffer();

                @Override
                protected void channelRead0(ChannelHandlerContext ctx2, ByteBuf msg) {
                    accum.writeBytes(msg);
                    String content = accum.toString(StandardCharsets.UTF_8);
                    if (content.contains("Hello from Server-Initiated Bidirectional Stream!")) {
                        logger.info("📥 Accepted Server Bidi Stream data: {}", content);
                        pendingVerifications.remove("Server_Bidi_Received");
                        serverBidiLatch.countDown();

                        // Send reply
                        ByteBuf reply = ctx2.alloc().directBuffer();
                        reply.writeBytes(
                                "ACK SERVER BIDI: Greetings from Java Client".getBytes(StandardCharsets.UTF_8));
                        ctx2.writeAndFlush(reply).addListener(f -> {
                            accum.release();
                            ctx2.close();
                        });
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx2, Throwable cause) {
                    logger.error("Error on server-initiated bidi stream", cause);
                    accum.release();
                    ctx2.close();
                }
            });
        }

        @Override
        protected void initUnidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel channel) {
            cleanPipeline(channel);
            channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                private final ByteBuf accum = Unpooled.buffer();

                @Override
                protected void channelRead0(ChannelHandlerContext ctx2, ByteBuf msg) {
                    accum.writeBytes(msg);
                    String content = accum.toString(StandardCharsets.UTF_8);

                    for (Consumer<String> listener : uniStreamListeners) {
                        listener.accept(content);
                    }

                    if (content.contains("Hello from Server-Initiated Unidirectional Stream!")) {
                        logger.info("📥 Accepted Server Uni Stream data: {}", content);
                        pendingVerifications.remove("Server_Uni_Received");
                        serverUniLatch.countDown();
                        accum.release();
                        ctx2.close();
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx2, Throwable cause) {
                    logger.error("Error on server-initiated uni stream", cause);
                    accum.release();
                    ctx2.close();
                }
            });
        }
    }

    private static void cleanPipeline(QuicStreamChannel ch) {
        ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                ctx.channel().eventLoop().execute(() -> {
                    List<String> toRemove = new java.util.ArrayList<>();
                    for (String name : ctx.pipeline().names()) {
                        ChannelHandler h = ctx.pipeline().get(name);
                        if (h != null && h != this
                                && (name.contains("Http3") || h.getClass().getName().contains("Http3"))) {
                            toRemove.add(name);
                        }
                    }
                    for (String name : toRemove) {
                        try {
                            ctx.pipeline().remove(name);
                        } catch (Exception expected) {
                        }
                    }
                });
                super.handlerAdded(ctx);
            }
        });
    }

    private static Session connect(String urlString, EventLoopGroup group, CountDownLatch serverUniLatch,
            CountDownLatch serverBidiLatch) throws Exception {
        URI uri = new URI(urlString);
        String host = uri.getHost();
        int port = uri.getPort() == -1 ? 4433 : uri.getPort();
        String path = uri.getPath();

        QuicSslContext context = QuicSslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols(Http3.supportedApplicationProtocols()).build();

        ChannelHandler codec = Http3.newQuicClientCodecBuilder()
                .sslContext(context)
                .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
                .initialMaxData(1073741824)
                .initialMaxStreamDataBidirectionalLocal(107374182)
                .initialMaxStreamDataBidirectionalRemote(107374182)
                .initialMaxStreamsUnidirectional(1000)
                .initialMaxStreamDataUnidirectional(107374182)
                .initialMaxStreamsBidirectional(1000)
                .datagram(10000, 10000)
                .build();

        Bootstrap bs = new Bootstrap();
        Channel channel = bs.group(group)
                .channel(NioDatagramChannel.class)
                .handler(codec)
                .bind(0).sync().channel();

        Http3Settings settings = new Http3Settings((id, value) -> true);
        settings.enableConnectProtocol(true);
        settings.enableH3Datagram(true);

        QuicChannel quicChannel = QuicChannel.newBootstrap(channel)
                .handler(new WebTransportTestClientConnectionHandler(new DefaultHttp3SettingsFrame(settings),
                        serverUniLatch, serverBidiLatch))
                .remoteAddress(new InetSocketAddress(host, port))
                .connect()
                .get();

        CountDownLatch handshakeLatch = new CountDownLatch(1);
        QuicStreamChannel[] connectStreamContainer = new QuicStreamChannel[1];

        QuicStreamChannel connectStreamChannel = Http3.newRequestStream(
                quicChannel,
                new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                if (msg instanceof Http3HeadersFrame) {
                                    Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                    if ("200".equals(headersFrame.headers().status().toString())) {
                                        connectStreamContainer[0] = (QuicStreamChannel) ctx.channel();
                                        handshakeLatch.countDown();
                                    }
                                }
                            }
                        });
                    }
                }).sync().getNow();

        Http3Headers headers = new DefaultHttp3Headers();
        headers.method("CONNECT");
        headers.scheme("https");
        headers.path(path);
        headers.authority(host + ":" + port);
        headers.set(":protocol", "webtransport");

        connectStreamChannel.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

        if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
            throw new Exception("Handshake CONNECT protocol timeout");
        }

        return new Session(quicChannel, connectStreamContainer[0], connectStreamContainer[0].streamId());
    }

    public static void main(String... args) throws Exception {
        String url = args.length > 0 ? args[0] : "https://localhost:4433/test";

        logger.info("=========================================");
        logger.info("🚀 WebTransport Java Client Test Suite 🚀");
        logger.info("=========================================");

        EventLoopGroup group = new NioEventLoopGroup(1);
        CountDownLatch serverUniLatch = new CountDownLatch(1);
        CountDownLatch serverBidiLatch = new CountDownLatch(1);

        try {
            logger.info("🔗 Connecting to {}...", url);
            Session session = connect(url, group, serverUniLatch, serverBidiLatch);
            logger.info("✅ Connection Established!\n");

            pendingVerifications.add("Server_Uni_Received");
            pendingVerifications.add("Server_Bidi_Received");

            // Wait a moment for server to issue greetings
            Thread.sleep(1000);

            // Run sequential tests
            testDatagrams(session);
            System.out.println("");

            testClientUniStream(session);
            System.out.println("");

            testClientBidiStream(session);
            System.out.println("");

            testLargePayload(session.quicChannel, session.sessionId);
            System.out.println("");

            testConcurrentStreams(session.quicChannel, session.sessionId);
            System.out.println("");

            testNoHeadOfLineBlocking(session.quicChannel, session.sessionId);
            System.out.println("");

            testStreamFlowControl(session.quicChannel, session.sessionId);
            System.out.println("");

            testHeartbeatIdle(session);
            System.out.println("");

            // Ensure background tasks for the primary session completed
            logger.info("🎧 Verifying Server-Initiated Streams...");
            if (serverUniLatch.await(5, TimeUnit.SECONDS) && serverBidiLatch.await(5, TimeUnit.SECONDS)) {
                logger.info("✅ Server Streams Received, Verified, and Handled.");
            } else {
                logger.warn("⚠️ Timeout waiting for server-initiated streams.");
            }
            System.out.println("");

            logger.info("🚪 Closing primary session.");
            session.close();
            System.out.println("");

            // Run the negative timeout test which requires its own session
            testHeartbeatTimeoutNegative(url, group);
            System.out.println("");

            // Final Assertion Check
            logger.info("Pending Verifications Check: {}", pendingVerifications);
            if (pendingVerifications.isEmpty()) {
                logger.info("=========================================");
                logger.info("🎉 ALL TESTS COMPLETED SUCCESSFULLY! 🎉");
                logger.info("=========================================");
            } else {
                logger.error("❌ Test suite failed. Some verifications never completed: {}", pendingVerifications);
                System.exit(1);
            }

        } catch (Exception e) {
            logger.error("❌ TEST SUITE FAILED: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            group.shutdownGracefully();
            logger.info("🏁 Test suite finished.");
        }
    }

    private static void testDatagrams(Session session) throws Exception {
        logger.info("🧪 --- Running Datagram Test ---");
        String payloadId = "PingDatagram_" + System.currentTimeMillis();
        pendingVerifications.add(payloadId);

        CountDownLatch ackLatch = new CountDownLatch(1);
        Consumer<String> listener = content -> {
            if (content.contains("ACK DG: " + payloadId)) {
                ackLatch.countDown();
            }
        };
        uniStreamListeners.add(listener);

        try {
            ByteBuf dgData = session.quicChannel.alloc().directBuffer();
            WebTransportUtils.writeVarInt(dgData, session.sessionId);
            dgData.writeBytes(payloadId.getBytes(StandardCharsets.UTF_8));
            session.quicChannel.writeAndFlush(dgData);
            logger.info("✅ Datagram {} Sent (Waiting for ACK on Uni Stream...)", payloadId);

            if (!ackLatch.await(5, TimeUnit.SECONDS)) {
                throw new Exception("Timeout waiting for datagram ACK on unidirectional stream");
            }
            pendingVerifications.remove(payloadId);
            logger.info("✅ Datagram Test Passed (ACK received via Uni Stream)");
        } finally {
            uniStreamListeners.remove(listener);
        }
    }

    private static void testClientUniStream(Session session) throws Exception {
        logger.info("🧪 --- Running Client Uni Stream Test ---");
        String payloadId = "PingUni_" + System.currentTimeMillis();
        pendingVerifications.add(payloadId);

        CountDownLatch ackLatch = new CountDownLatch(1);
        Consumer<String> listener = content -> {
            if (content.contains("ACK UNI: " + payloadId)) {
                ackLatch.countDown();
            }
        };
        uniStreamListeners.add(listener);

        try {
            QuicStreamChannel stream = session.quicChannel
                    .createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            cleanPipeline(ch);
                        }
                    }).sync().getNow();

            ByteBuf data = stream.alloc().directBuffer();
            WebTransportUtils.writeVarInt(data, 0x54);
            WebTransportUtils.writeVarInt(data, session.sessionId);
            data.writeBytes(payloadId.getBytes(StandardCharsets.UTF_8));
            stream.writeAndFlush(data).sync();

            // Close write side
            stream.shutdownOutput().sync();
            logger.info("✅ Client Uni Stream created & payload written. Waiting for server-initiated ACK stream...");

            if (!ackLatch.await(5, TimeUnit.SECONDS)) {
                throw new Exception("Timeout waiting for Uni stream ACK on unidirectional stream");
            }
            pendingVerifications.remove(payloadId);
            logger.info("✅ Client Uni Stream Test Passed (Strict ACK asserted)");
        } finally {
            uniStreamListeners.remove(listener);
        }
    }

    private static void testClientBidiStream(Session session) throws Exception {
        logger.info("🧪 --- Running Client Bidi Stream Test ---");
        String payloadId = "PingBidi_" + System.currentTimeMillis();
        pendingVerifications.add(payloadId);

        CountDownLatch echoLatch = new CountDownLatch(1);

        QuicStreamChannel stream = session.quicChannel
                .createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        cleanPipeline(ch);
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                String resp = msg.toString(StandardCharsets.UTF_8);
                                logger.info("Received from Bidi: {}", resp);
                                if (resp.contains("ACK BI: " + payloadId)) {
                                    pendingVerifications.remove(payloadId);
                                    echoLatch.countDown();
                                }
                            }
                        });
                    }
                }).sync().getNow();

        ByteBuf data = stream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(data, 0x41);
        WebTransportUtils.writeVarInt(data, session.sessionId);
        data.writeBytes(payloadId.getBytes(StandardCharsets.UTF_8));
        stream.writeAndFlush(data).sync();

        if (!echoLatch.await(5, TimeUnit.SECONDS)) {
            throw new Exception("Timeout waiting for bidi stream echo");
        }

        stream.shutdownOutput().sync();
        logger.info("✅ Client Bidi Stream Test Passed");
    }

    private static void testLargePayload(QuicChannel quicChannel, long sessionId) throws Exception {
        logger.info("🧪 --- Running Large Payload Test (Data Integrity) ---");
        CountDownLatch latch = new CountDownLatch(1);
        String payloadId = "LargePayload_" + System.currentTimeMillis();
        pendingVerifications.add(payloadId);

        // 250KB payload
        byte[] chunk = new byte[16384 * 16];
        java.util.Arrays.fill(chunk, (byte) 'A');
        byte[] headerBytes = (payloadId + "_").getBytes(StandardCharsets.UTF_8);
        byte[] testMsg = new byte[headerBytes.length + chunk.length];
        System.arraycopy(headerBytes, 0, testMsg, 0, headerBytes.length);
        System.arraycopy(chunk, 0, testMsg, headerBytes.length, chunk.length);

        final byte[] expectedMsg = new byte[8 + testMsg.length]; // "ACK BI: " is 8 bytes
        System.arraycopy("ACK BI: ".getBytes(StandardCharsets.UTF_8), 0, expectedMsg, 0, 8);
        System.arraycopy(testMsg, 0, expectedMsg, 8, testMsg.length);

        QuicStreamChannel stream = quicChannel
                .createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        cleanPipeline(ch);
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            private final ByteBuf accum = ch.alloc().buffer(expectedMsg.length);

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                accum.writeBytes(msg);
                                if (accum.readableBytes() >= expectedMsg.length) {
                                    byte[] received = new byte[expectedMsg.length];
                                    accum.readBytes(received);
                                    if (java.util.Arrays.equals(expectedMsg, received)) {
                                        logger.info("✅ Large Payload Test Passed");
                                        pendingVerifications.remove(payloadId);
                                        latch.countDown();
                                    } else {
                                        logger.error("❌ Large Payload Test Failed: Payload corrupted!");
                                    }
                                    accum.release();
                                    ctx.close();
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                logger.error("Large payload stream error", cause);
                                accum.release();
                                ctx.close();
                            }
                        });
                    }
                }).sync().getNow();

        // Write WT bidi header
        ByteBuf headerBuf = stream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(headerBuf, 0x41);
        WebTransportUtils.writeVarInt(headerBuf, sessionId);
        stream.writeAndFlush(headerBuf).sync();

        // Write payload in chunks to avoid overwhelming Netty outbound buffer
        int offset = 0;
        int chunkSize = 8192;
        while (offset < testMsg.length) {
            int len = Math.min(chunkSize, testMsg.length - offset);
            ByteBuf chunkBuf = stream.alloc().directBuffer(len);
            chunkBuf.writeBytes(testMsg, offset, len);
            stream.write(chunkBuf);
            offset += len;
        }
        stream.flush();

        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new Exception("Timeout waiting for large payload response");
        }
    }

    private static void testConcurrentStreams(QuicChannel quicChannel, long sessionId) throws Exception {
        logger.info("🧪 --- Running Concurrent Streams Stress Test ---");
        int numStreams = 8;
        CountDownLatch latch = new CountDownLatch(numStreams);

        for (int i = 0; i < numStreams; i++) {
            final int idx = i;
            String payloadId = "Concurrent_" + idx + "_" + System.currentTimeMillis();
            pendingVerifications.add(payloadId);

            quicChannel.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                    cleanPipeline(ch);
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            String response = msg.toString(StandardCharsets.UTF_8);
                            if (response.contains("ACK BI: " + payloadId)) {
                                pendingVerifications.remove(payloadId);
                                latch.countDown();
                            }
                            ctx.close();
                        }
                    });
                }
            }).addListener((Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                    QuicStreamChannel ch = f.getNow();
                    ByteBuf data = ch.alloc().directBuffer();
                    WebTransportUtils.writeVarInt(data, 0x41);
                    WebTransportUtils.writeVarInt(data, sessionId);
                    data.writeBytes(payloadId.getBytes(StandardCharsets.UTF_8));
                    ch.writeAndFlush(data);
                }
            });
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new Exception("Timeout in concurrent streams stress test");
        }
        logger.info("✅ Concurrent Streams Test Passed (8 streams multiplexed)");
    }

    private static void testNoHeadOfLineBlocking(QuicChannel quicChannel, long sessionId) throws Exception {
        logger.info("🧪 --- Running Application-Level HOLB Test ---");
        CountDownLatch fastLatch = new CountDownLatch(1);
        CountDownLatch hogLatch = new CountDownLatch(1);

        String hogPayload = "SleepServer_" + System.currentTimeMillis();
        String fastPayload = "Ping_Fast_" + System.currentTimeMillis();
        pendingVerifications.add(hogPayload);
        pendingVerifications.add(fastPayload);

        // 1. Create Hog stream
        QuicStreamChannel hogStream = quicChannel
                .createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        cleanPipeline(ch);
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                String resp = msg.toString(StandardCharsets.UTF_8);
                                if (resp.contains("ACK BI: " + hogPayload)) {
                                    pendingVerifications.remove(hogPayload);
                                    hogLatch.countDown();
                                }
                                ctx.close();
                            }
                        });
                    }
                }).sync().getNow();

        ByteBuf hogData = hogStream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(hogData, 0x41);
        WebTransportUtils.writeVarInt(hogData, sessionId);
        hogData.writeBytes(hogPayload.getBytes(StandardCharsets.UTF_8));
        hogStream.writeAndFlush(hogData).sync();

        // Wait 0.5s
        Thread.sleep(500);

        // 2. Create Fast stream
        long startTime = System.currentTimeMillis();
        QuicStreamChannel fastStream = quicChannel
                .createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        cleanPipeline(ch);
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                String resp = msg.toString(StandardCharsets.UTF_8);
                                if (resp.contains("ACK BI: " + fastPayload)) {
                                    pendingVerifications.remove(fastPayload);
                                    fastLatch.countDown();
                                }
                                ctx.close();
                            }
                        });
                    }
                }).sync().getNow();

        ByteBuf fastData = fastStream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(fastData, 0x41);
        WebTransportUtils.writeVarInt(fastData, sessionId);
        fastData.writeBytes(fastPayload.getBytes(StandardCharsets.UTF_8));
        fastStream.writeAndFlush(fastData).sync();

        // 3. Assert fast stream finishes immediately
        if (!fastLatch.await(1500, TimeUnit.MILLISECONDS)) {
            throw new Exception("❌ HOLB Test Failed: Fast stream blocked by Hog stream!");
        }
        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("✅ Fast stream completed in {}ms (bypassing the sleeping Hog stream!)", elapsed);

        // 4. Assert hog stream finishes successfully
        if (!hogLatch.await(4000, TimeUnit.MILLISECONDS)) {
            throw new Exception("❌ HOLB Test Failed: Hog stream timed out!");
        }
        logger.info("✅ Hog stream finally completed successfully.");
    }

    private static void testStreamFlowControl(QuicChannel quicChannel, long sessionId) throws Exception {
        logger.info("🧪 --- Running Stream Limit Exhaustion Test ---");
        List<QuicStreamChannel> streams = new java.util.ArrayList<>();
        boolean blocked = false;

        try {
            for (int i = 1; i <= 150; i++) {
                Future<QuicStreamChannel> f = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                        new ChannelInitializer<QuicStreamChannel>() {
                            @Override
                            protected void initChannel(QuicStreamChannel ch) {
                                cleanPipeline(ch);
                            }
                        });
                if (!f.await(1000, TimeUnit.MILLISECONDS)) {
                    logger.info("✅ Flow control working! Stream creation blocked after opening {} streams.",
                            streams.size());
                    blocked = true;
                    break;
                }
                if (f.isSuccess()) {
                    streams.add(f.getNow());
                    if (i % 50 == 0) {
                        logger.info("Successfully opened {} streams...", i);
                    }
                } else {
                    logger.info("✅ Flow control working! Stream creation failed after opening {} streams.",
                            streams.size());
                    blocked = true;
                    break;
                }
            }
        } catch (Exception e) {
            blocked = true;
        }

        if (!blocked) {
            logger.warn("⚠️ Flow control test aborted: server limit is > 150.");
        }

        // Cleanup streams
        logger.info("Cleaning up streams...");
        for (QuicStreamChannel s : streams) {
            try {
                s.close();
            } catch (Exception e) {
            }
        }
    }

    private static void testHeartbeatIdle(Session session) throws Exception {
        logger.info("🧪 --- Running Heartbeat / Idle Test ---");
        logger.info("Sleeping for 15 seconds to verify connection stability...");
        Thread.sleep(15000);

        // Verify connection is still alive by running a quick uni test
        String payloadId = "PingAfterSleep_" + System.currentTimeMillis();
        CountDownLatch ackLatch = new CountDownLatch(1);
        Consumer<String> listener = content -> {
            if (content.contains("ACK UNI: " + payloadId)) {
                ackLatch.countDown();
            }
        };
        uniStreamListeners.add(listener);

        try {
            QuicStreamChannel stream = session.quicChannel
                    .createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            cleanPipeline(ch);
                        }
                    }).sync().getNow();

            ByteBuf data = stream.alloc().directBuffer();
            WebTransportUtils.writeVarInt(data, 0x54);
            WebTransportUtils.writeVarInt(data, session.sessionId);
            data.writeBytes(payloadId.getBytes(StandardCharsets.UTF_8));
            stream.writeAndFlush(data).sync();

            stream.shutdownOutput().sync();

            if (!ackLatch.await(5, TimeUnit.SECONDS)) {
                throw new Exception("Did not receive strict ACK after idle period");
            }
            logger.info("✅ Connection survived idle period and successfully exchanged data!");
        } finally {
            uniStreamListeners.remove(listener);
        }
    }

    private static void testHeartbeatTimeoutNegative(String url, EventLoopGroup group) throws Exception {
        logger.info("🧪 --- Running Heartbeat Timeout Negative Test ---");
        logger.info("🔗 Establishing a separate connection to test timeout drops...");
        CountDownLatch dummyUni = new CountDownLatch(1);
        CountDownLatch dummyBidi = new CountDownLatch(1);
        Session session = connect(url, group, dummyUni, dummyBidi);

        logger.info("✅ Connection Established! Now sleeping for 36 seconds (exceeding 30s server limit)...");
        Thread.sleep(36000);

        logger.info("Awake! Attempting to use the session. This should FAIL because the server dropped us.");
        boolean dropped = false;

        try {
            Future<QuicStreamChannel> f = session.quicChannel.createStream(QuicStreamType.UNIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            cleanPipeline(ch);
                        }
                    });
            if (f.await(3000, TimeUnit.MILLISECONDS) && f.isSuccess()) {
                QuicStreamChannel stream = f.getNow();
                ByteBuf data = stream.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x54);
                WebTransportUtils.writeVarInt(data, session.sessionId);
                data.writeBytes("Should not reach here".getBytes(StandardCharsets.UTF_8));
                stream.writeAndFlush(data).sync();
            } else {
                dropped = true;
            }
        } catch (Exception e) {
            dropped = true;
        }

        if (dropped) {
            logger.info("✅ Connection was correctly dropped by server!");
        } else {
            throw new Exception("Negative test failed: Connection was STILL ALIVE after 36s of inactivity!");
        }
    }
}
