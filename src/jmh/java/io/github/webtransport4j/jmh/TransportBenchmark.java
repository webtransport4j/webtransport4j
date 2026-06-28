package io.github.webtransport4j.jmh;

import io.github.webtransport4j.api.*;
import io.github.webtransport4j.server.WebTransportServer;
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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.openjdk.jmh.annotations.*;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Dbenchmark.forks=1"})
public class TransportBenchmark {

    @Param({"nio", "kqueue"})
    private String transportType;

    private WebTransportServer server;
    private NioEventLoopGroup clientGroup;
    private QuicChannel clientChannel;
    
    // We will keep a single WebTransport bidirectional stream open for the lifetime of the trial
    private QuicStreamChannel wtStream;
    private final LinkedBlockingQueue<ByteBuf> responseQueue = new LinkedBlockingQueue<>();
    
    // Test payload of 64KB
    private byte[] payload;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        // Sleep first to let previous trial release the bound port
        Thread.sleep(1000);

        System.setProperty("webtransport4j.server.transport", transportType);
        System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
        System.setProperty("webtransport4j.webtransport.initial.max.streams.bidi", "10000");
        System.setProperty("webtransport4j.quic.max.streams.bidi", "10000");
        System.setProperty("webtransport4j.webtransport.initial.max.streams.uni", "10000");
        System.setProperty("webtransport4j.quic.max.streams.uni", "10000");
        System.setProperty("webtransport4j.quic.initial.max.data", "100000000000");
        System.setProperty("webtransport4j.webtransport.initial.max.data", "100000000000");
        
        payload = new byte[65536]; // 64KB payload
        for(int i = 0; i < payload.length; i++) payload[i] = 'A';

        // 1. Start Server
        server = new WebTransportServer();
        server.registerHandler("/bench", new WebTransportHandler() {
            @Override
            public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
                stream.onData(data -> {
                    // Echo back the same payload size
                    stream.write(data.readBytes());
                });
            }
        });
        new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(1000);

        // 2. Setup Client
        clientGroup = new NioEventLoopGroup(1);
        
        QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols(Http3.supportedApplicationProtocols())
                .build();

        Http3Settings clientSettings = new Http3Settings((id, value) -> true);
        clientSettings.enableH3Datagram(true);
        clientSettings.enableConnectProtocol(true);
        clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
        clientSettings.put(0x2b64L, 10000L); // SETTINGS_WT_INITIAL_MAX_STREAMS_UNI
        clientSettings.put(0x2b65L, 10000L); // SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI
        clientSettings.put(0x2b61L, 100000000000L); // SETTINGS_WT_INITIAL_MAX_DATA

        ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
                .sslContext(clientSslContext)
                .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                .initialMaxData(100000000000L)
                .initialMaxStreamDataBidirectionalLocal(1000000)
                .initialMaxStreamDataBidirectionalRemote(1000000)
                .initialMaxStreamsBidirectional(10000)
                .initialMaxStreamsUnidirectional(10000)
                .datagram(10000, 10000)
                .build();

        Bootstrap bs = new Bootstrap();
        Channel channel = bs.group(clientGroup)
                .channel(NioDatagramChannel.class)
                .handler(clientCodec)
                .bind(0)
                .sync()
                .channel();

        QuicChannelBootstrap qcb = QuicChannel.newBootstrap(channel)
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
                .remoteAddress(new InetSocketAddress("127.0.0.1", 4433));

        clientChannel = qcb.connect().get();

        // 3. Create WebTransport CONNECT Stream
        CountDownLatch handshakeLatch = new CountDownLatch(1);
        final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];
        
        Http3.newRequestStream(clientChannel, new ChannelInitializer<QuicStreamChannel>() {
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
                headers.method("CONNECT");
                headers.scheme("https");
                headers.authority("localhost:4433");
                headers.path("/bench");
                headers.set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
            }
        });
        
        if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("WebTransport CONNECT handshake timed out");
        }
        
        long sessionId = connectStream[0].streamId();

        // 4. Create WebTransport Bidi Stream
        wtStream = clientChannel.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                        ctx.channel().eventLoop().execute(() -> {
                            for (String name : new java.util.ArrayList<>(ctx.pipeline().names())) {
                                ChannelHandler h = ctx.pipeline().get(name);
                                if (h != null && h != this && (name.contains("Http3") || h.getClass().getName().contains("Http3"))) {
                                    ctx.pipeline().remove(name);
                                }
                            }
                        });
                        super.handlerAdded(ctx);
                    }
                });
                
                ch.pipeline().addLast("responseQueueHandler", new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof ByteBuf) {
                            responseQueue.add((ByteBuf) msg);
                        } else {
                            ReferenceCountUtil.release(msg);
                        }
                    }
                });
            }
        }).get();

        // Write prefix to wtStream to register it on the server
        ByteBuf initBuf = wtStream.alloc().directBuffer();
        WebTransportUtils.writeVarInt(initBuf, 0x41);
        WebTransportUtils.writeVarInt(initBuf, sessionId);
        wtStream.writeAndFlush(initBuf).sync();
        
        // Wait a brief moment to ensure server has processed the incoming stream registration
        Thread.sleep(500);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (wtStream != null) wtStream.close();
        if (clientChannel != null) clientChannel.close();
        if (clientGroup != null) clientGroup.shutdownGracefully();
        if (server != null) server.stop();
    }

    @Benchmark
    public void testRoundtrip() throws Exception {
        ByteBuf buf = Unpooled.wrappedBuffer(payload);
        wtStream.writeAndFlush(buf);
        
        int received = 0;
        while (received < payload.length) {
            ByteBuf resp = responseQueue.poll(5, TimeUnit.SECONDS);
            if (resp == null) {
                throw new IllegalStateException("Timeout waiting for echo response; received " + received + " of " + payload.length);
            }
            received += resp.readableBytes();
            resp.release();
        }
    }
}
