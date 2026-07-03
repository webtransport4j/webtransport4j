package io.github.webtransport4j.client;

/**
 * @author https://github.com/sanjomo
 * @date 03/07/26 6:03 pm
 */


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

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Aggressive Bulk Throughput Benchmark for WebTransport File Transfer
 */
public final class WebTransportFileBenchmark {

    // Aggressive Chunking: 256 KB per write
    private static final int CHUNK_SIZE = 256 * 1024;

    public static void main(String... args) throws Exception {

        // Define the file to stream (Pass via args or hardcode here)
        String filePath = args.length > 0 ? args[0] : "/Users/sam/Downloads/1GB.bin";
        File file = new File(filePath);

        if (!file.exists() || !file.isFile()) {
            System.err.println("File not found or is not a valid file: " + file.getAbsolutePath());
            return;
        }

        final long TOTAL_BYTES = file.length();
        System.out.println("Preparing to stream file: " + file.getName() + " (" + (TOTAL_BYTES / (1024 * 1024)) + " MB)");

        // MultiThreadIoEventLoopGroup with 8 threads for high I/O capacity
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(8, NioIoHandler.newFactory());

        try (FileInputStream fis = new FileInputStream(file);
             FileChannel fileChannel = fis.getChannel()) {

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
            BulkThroughputHandler1 throughputHandler = new BulkThroughputHandler1();

            QuicStreamChannel biStreamChannel = quicChannel.createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    throughputHandler
            ).sync().getNow();

            // Write WebTransport Bi-stream header
            ByteBuf headerBuf = biStreamChannel.alloc().buffer();
            WebTransportUtils.writeVarInt(headerBuf, 0x41);
            WebTransportUtils.writeVarInt(headerBuf, sessionId);
            biStreamChannel.writeAndFlush(headerBuf).sync();

            System.out.println("Starting Aggressive Bidirectional Stream Benchmark...");
            long startTime = System.nanoTime();

            // Start sending file data with aggressive backpressure safety
            streamFileWithBackpressure(biStreamChannel, throughputHandler, fileChannel, TOTAL_BYTES);

            long endTime = System.nanoTime();

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

    private static void streamFileWithBackpressure(QuicStreamChannel channel, BulkThroughputHandler1 handler, FileChannel fileChannel, long totalBytes) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        handler.expect(totalBytes, latch);

        long bytesSent = 0;
        int flushCounter = 0;

        while (bytesSent < totalBytes) {
            if (channel.isWritable()) {
                // Calculate how much we need to read for the final chunk
                int bytesToRead = (int) Math.min(CHUNK_SIZE, totalBytes - bytesSent);

                // Allocate a buffer strictly for this read.
                // Netty will automatically release it once the write payload is flushed to the network.
                ByteBuf chunkBuf = channel.alloc().directBuffer(bytesToRead);

                int bytesRead = chunkBuf.writeBytes(fileChannel, bytesToRead);
                if (bytesRead < 0) {
                    chunkBuf.release();
                    break; // Unexpected EOF
                }

                channel.write(chunkBuf);
                bytesSent += bytesRead;
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

        System.out.println("All data sent. Awaiting full echo receipt from server...");
        latch.await();
    }
}

/**
 * Monitors incoming byte stream to count payload progress without keeping data in memory.
 */
class BulkThroughputHandler1 extends ChannelDuplexHandler {
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
            }

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