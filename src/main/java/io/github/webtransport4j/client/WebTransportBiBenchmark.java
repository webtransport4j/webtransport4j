package io.github.webtransport4j.client;

import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Bidirectional Throughput Benchmark for WebTransport Client
 */
public final class WebTransportBiBenchmark {

    // Benchmark configuration
    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int MEASUREMENT_ITERATIONS = 500_000;

    public static void main(String... args) throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        try {
            // 1. Setup codec and bootstrap
            QuicSslContext context = QuicSslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(Http3.supportedApplicationProtocols()).build();

            ChannelHandler codec = Http3.newQuicClientCodecBuilder()
                    .sslContext(context)
                    .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                    .initialMaxData(Integer.MAX_VALUE)
                    .initialMaxStreamDataBidirectionalLocal(Integer.MAX_VALUE)
                    .initialMaxStreamDataBidirectionalRemote(Integer.MAX_VALUE)
                    .initialMaxStreamsUnidirectional(Integer.MAX_VALUE)
                    .initialMaxStreamDataUnidirectional(Integer.MAX_VALUE)
                    .initialMaxStreamsBidirectional(Integer.MAX_VALUE)
                    .datagram(Integer.MAX_VALUE, Integer.MAX_VALUE)
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
                    .handler(new WebTransportClientHandler(new DefaultHttp3SettingsFrame(settings), true, (id, value) -> true))
                    .remoteAddress(new InetSocketAddress(NetUtil.LOCALHOST4, 4433))
                    .connect()
                    .get();

            QuicStreamChannel connectStreamChannel = Http3.newRequestStream(
                    quicChannel,
                    new ChannelInboundHandlerAdapter()
            ).sync().getNow();

            // 2. Establish the CONNECT request for the /echo endpoint
            Http3HeadersFrame frame = new DefaultHttp3HeadersFrame();
            frame.headers().method("CONNECT").path("/echo")
                    .authority(NetUtil.LOCALHOST4.getHostAddress() + ":" + 4433)
                    .scheme("https").protocol("webtransport-h3");
            connectStreamChannel.writeAndFlush(frame).sync();

            long sessionId = connectStreamChannel.streamId();

            // 3. Create the Bidirectional Stream Handler
            BenchmarkBiHandler biHandler = new BenchmarkBiHandler();

            QuicStreamChannel biStreamChannel = quicChannel.createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    biHandler
            ).sync().getNow();

            // Write the WebTransport Bidirectional Stream Header (0x41 + Session ID)
            ByteBuf headerBuf = biStreamChannel.alloc().buffer();
            WebTransportUtils.writeVarInt(headerBuf, 0x41);
            WebTransportUtils.writeVarInt(headerBuf, sessionId);
            biStreamChannel.writeAndFlush(headerBuf).sync();

            // Prepare the payload buffer ONCE
            ByteBuf payload = biStreamChannel.alloc().directBuffer();
            payload.writeCharSequence("benchmark_data", CharsetUtil.UTF_8);

            // 4. Warmup Phase (Send & Receive)
            System.out.println("Starting warmup phase (" + WARMUP_ITERATIONS + " iterations)...");
            runLoad(biStreamChannel, biHandler, payload, WARMUP_ITERATIONS);
            System.out.println("Warmup complete.");

            // 5. Measurement Phase (Send & Receive)
            System.out.println("Starting measurement phase (" + MEASUREMENT_ITERATIONS + " iterations)...");
            long startTime = System.nanoTime();

            runLoad(biStreamChannel, biHandler, payload, MEASUREMENT_ITERATIONS);

            long endTime = System.nanoTime();

            // 6. Calculate Metrics
            long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            double opsPerSec = (MEASUREMENT_ITERATIONS * 1000.0) / durationMs;

            System.out.println("========================================");
            System.out.println("Messages Sent & Received : " + MEASUREMENT_ITERATIONS);
            System.out.println("Payload Size             : " + payload.readableBytes() + " bytes");
            System.out.println("Time Taken               : " + durationMs + " ms");
            System.out.printf("Throughput               : %,.2f ops/sec%n", opsPerSec);
            System.out.println("========================================");

            payload.release();

        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * Sends messages and blocks until all corresponding bytes are echoed back by the server.
     */
    private static void runLoad(QuicStreamChannel channel, BenchmarkBiHandler handler, ByteBuf payload, int iterations) throws InterruptedException {
        int payloadSize = payload.readableBytes();
        long totalBytesExpected = (long) iterations * payloadSize;

        CountDownLatch latch = new CountDownLatch(1);
        handler.expect(totalBytesExpected, latch);

        for (int i = 1; i <= iterations; i++) {
            channel.write(payload.retainedDuplicate());

            // Periodically flush to prevent Netty's outbound buffer from causing OOM issues
            if (i % 5000 == 0) {
                channel.flush();
            }
        }
        // Final flush for any remaining packets
        channel.flush();

        // Wait for the echo server to stream everything back
        latch.await();
    }
}

/**
 * Custom handler to track incoming bytes and release the latch when the expected amount is received.
 */
class BenchmarkBiHandler extends ChannelDuplexHandler {
    private long bytesReceived = 0;
    private long bytesExpected = 0;
    private CountDownLatch currentLatch;

    public void expect(long bytes, CountDownLatch latch) {
        this.bytesExpected = bytes;
        this.bytesReceived = 0;
        this.currentLatch = latch;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            bytesReceived += buf.readableBytes();

            if (currentLatch != null && bytesReceived >= bytesExpected) {
                currentLatch.countDown();
            }
            ReferenceCountUtil.release(msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}