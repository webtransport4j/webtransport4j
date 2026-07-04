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
import io.netty.util.NetUtil;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Aggressive Bulk Throughput Benchmark for 1GB WebTransport Transfer
 */
public final class WebTransportBytesBenchmark {

    // 1 GB total data = 1,073,741,824 bytes
    private static final long TOTAL_BYTES = 1024L * 1024L * 1024L;

    // Aggressive Chunking: 256 KB per write
    private static final int CHUNK_SIZE = 256 * 1024;

    public static void main(String... args) throws Exception {
        // MultiThreadIoEventLoopGroup with 8 threads for high I/O capacity
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(8, NioIoHandler.newFactory());

        try {
            QuicSslContext context = QuicSslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(Http3.supportedApplicationProtocols()).build();

            ChannelHandler codec = Http3.newQuicClientCodecBuilder()
                    .sslContext(context)
                    .maxIdleTimeout(10000, TimeUnit.MILLISECONDS)
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

            // Connect to the registered /echo endpoint
            Http3HeadersFrame frame = new DefaultHttp3HeadersFrame();
            frame.headers().method("CONNECT").path("/echo")
                    .authority(NetUtil.LOCALHOST4.getHostAddress() + ":" + 4433)
                    .scheme("https").protocol("webtransport-h3");
            connectStreamChannel.writeAndFlush(frame).sync();

            long sessionId = connectStreamChannel.streamId();
            BulkThroughputHandler throughputHandler = new BulkThroughputHandler();

            QuicStreamChannel biStreamChannel = quicChannel.createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    throughputHandler
            ).sync().getNow();

            // Write WebTransport Bi-stream header
            ByteBuf headerBuf = biStreamChannel.alloc().buffer();
            WebTransportUtils.writeVarInt(headerBuf, 0x41);
            WebTransportUtils.writeVarInt(headerBuf, sessionId);
            biStreamChannel.writeAndFlush(headerBuf).sync();

            // Allocate a single 256KB direct memory buffer to reuse
            ByteBuf payloadChunk = biStreamChannel.alloc().directBuffer(CHUNK_SIZE);
            for (int i = 0; i < CHUNK_SIZE; i++) {
                payloadChunk.writeByte((byte) 'x');
            }

            System.out.printf("Starting Aggressive Bidirectional Stream Benchmark for %,d bytes...%n", TOTAL_BYTES);
            long startTime = System.nanoTime();

            // Start sending data with aggressive backpressure safety
            streamDataWithBackpressure(biStreamChannel, throughputHandler, payloadChunk);

            long endTime = System.nanoTime();
            payloadChunk.release();

            // Calculate Metrics
            long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            double seconds = durationMs / 1000.0;
            double mbps = (TOTAL_BYTES / (1024.0 * 1024.0)) / seconds;
            double gbps = (TOTAL_BYTES * 8.0 / 1_000_000_000.0) / seconds;

            System.out.println("\n================ BENCHMARK RESULTS ================");
            System.out.printf("Total Data Transferred : %.2f MB (Send & Receive)%n", TOTAL_BYTES / (1024.0 * 1024.0));
            System.out.println("Time Taken             : " + durationMs + " ms (" + String.format("%.2f", seconds) + " seconds)");
            System.out.printf("Throughput (MB/s)      : %,.2f MB/s%n", mbps);
            System.out.printf("Throughput (Network)   : %,.2f Gbps%n", gbps);
            System.out.println("===================================================");

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void streamDataWithBackpressure(QuicStreamChannel channel, BulkThroughputHandler handler, ByteBuf chunk) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        handler.expect(TOTAL_BYTES, latch);

        long bytesSent = 0;
        int flushCounter = 0;

        while (bytesSent < TOTAL_BYTES) {
            if (channel.isWritable()) {
                int bytesToWrite = (int) Math.min(CHUNK_SIZE, TOTAL_BYTES - bytesSent);
                if (bytesToWrite == CHUNK_SIZE) {
                    channel.write(chunk.retainedDuplicate());
                } else {
                    channel.write(chunk.retainedSlice(0, bytesToWrite));
                }
                bytesSent += bytesToWrite;
                flushCounter++;

                // Flush every 16 chunks (4 MB batches) to lower CPU overhead
                if (flushCounter % 16 == 0) {
                    channel.flush();
                }
            } else {
                // Buffer is full! Force a flush and instantly yield CPU back to Netty
                channel.flush();

                // CRITICAL: Do not leave this empty.
                // onSpinWait tells the CPU we are in a busy-wait loop, heavily optimizing instruction pipelines.
                LockSupport.parkNanos(1);
            }
        }

        // Final flush for remaining chunks
        channel.flush();

        System.out.printf("All %,d bytes sent. Awaiting full echo receipt from server...%n", TOTAL_BYTES);
        latch.await();
    }
}

/**
 * Monitors incoming byte stream to count payload progress without keeping data in memory.
 */
class BulkThroughputHandler extends ChannelDuplexHandler {
    private long bytesReceived = 0;
    private long bytesExpected = 0;
    private CountDownLatch currentLatch;
    private long lastReportedPercent = 0;

    public void expect(long bytes, CountDownLatch latch) {
        this.bytesExpected = bytes;
        this.bytesReceived = 0;
        this.currentLatch = latch;
        this.lastReportedPercent = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            bytesReceived += buf.readableBytes();

            // Print progress every 10% milestone
            long currentPercent = (bytesReceived * 100) / bytesExpected;
            if (currentPercent >= lastReportedPercent + 10) {
                lastReportedPercent = (currentPercent / 10) * 10;
                System.out.println("Progress: Received " + lastReportedPercent + "% (" + (bytesReceived / (1024 * 1024)) + " MB)");
                System.out.flush();
            }

            if (currentLatch != null && bytesReceived >= bytesExpected) {
                if (lastReportedPercent < 100) {
                    System.out.println("Progress: Received 100% (" + (bytesReceived / (1024 * 1024)) + " MB)");
                    System.out.flush();
                }
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