package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * End-to-End Integration Benchmark: AF_XDP Zero-Copy Ingestion connected directly
 * to a Netty WebTransport Channel Pipeline.
 *
 * <p>Validates the complete zero-copy path:
 * 1. Wire-level Ethernet II + IPv6 + UDP 443 frame in UMEM off-heap arena.
 * 2. AF_XDP zero-copy parsing via Java 25 FFM (Foreign Function & Memory API).
 * 3. Direct handoff into Netty pipeline via fireChannelRead() without kernel sk_buff copy.
 * 4. Netty channel handler consuming and processing the WebTransport payload.
 */
public class AfXdpEndToEndBenchmarkTest {

  private static final int WEBTRANSPORT_PORT = 443;
  private static final int CLIENT_PORT = 54321;

  @Test
  public void testAfXdpEndToEndNettyPipeline() throws Exception {
    final int packetCount = 200_000;
    final int payloadSize = 1024; // 1 KiB per WebTransport datagram

    final AtomicLong handledPackets = new AtomicLong(0);
    final AtomicLong handledBytes = new AtomicLong(0);
    final AtomicLong xorSum = new AtomicLong(0);

    // 1. Setup Netty Inbound Channel Handler (Simulating WebTransport Server Session)
    SimpleChannelInboundHandler<ByteBuf> webTransportChannelHandler =
        new SimpleChannelInboundHandler<ByteBuf>(false) {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            int len = msg.readableBytes();
            handledPackets.incrementAndGet();
            handledBytes.addAndGet(len);

            // Fast XOR check simulating payload consumption
            long accum = 0;
            while (msg.readableBytes() >= 8) {
              accum ^= msg.readLong();
            }
            while (msg.isReadable()) {
              accum ^= (long) msg.readByte();
            }
            xorSum.addAndGet(accum);
          }
        };

    EmbeddedChannel channel = new EmbeddedChannel(webTransportChannelHandler);

    // 2. Setup AF_XDP UMEM and Dispatcher connected to Netty Pipeline
    try (AfXdpUmem umem = new AfXdpUmem(4096, 2048)) {
      AfXdpPacketDispatcher dispatcher =
          new AfXdpPacketDispatcher(
              umem,
              WEBTRANSPORT_PORT,
              byteBuf -> {
                // Direct zero-copy handoff into Netty Pipeline
                channel.writeInbound(byteBuf);
              });

      // 3. Prepare Synthetic IPv6 WebTransport Frame in UMEM
      byte[] testPayload = new byte[payloadSize];
      for (int i = 0; i < payloadSize; i++) {
        testPayload[i] = (byte) (i & 0xFF);
      }

      long frameOffset = umem.leaseFrame();
      int wireLen =
          writeEthernetIpv6UdpPacket(
              umem.getFrameSlice(frameOffset, 4096),
              CLIENT_PORT,
              WEBTRANSPORT_PORT,
              testPayload);

      // 4. Warmup
      for (int i = 0; i < 20_000; i++) {
        dispatcher.onRxDescriptor(frameOffset, wireLen);
      }
      handledPackets.set(0);
      handledBytes.set(0);
      xorSum.set(0);

      // 5. Timed End-to-End Benchmark Run
      long startNanos = System.nanoTime();
      for (int i = 0; i < packetCount; i++) {
        dispatcher.onRxDescriptor(frameOffset, wireLen);
      }
      long elapsedNanos = System.nanoTime() - startNanos;

      double durationSec = elapsedNanos / 1_000_000_000.0;
      double mpps = (packetCount / durationSec) / 1_000_000.0;
      double mbPerSec = ((long) packetCount * payloadSize) / (1024.0 * 1024.0) / durationSec;

      System.out.printf(
          "%n========================================================================%n"
              + ">>> [AF_XDP + Netty End-to-End WebTransport Ingestion Benchmark]%n"
              + "    Total Datagrams Ingested & Handled: %,d packets%n"
              + "    Total Payload Data:                %.2f MB%n"
              + "    Elapsed Duration:                  %.4f s%n"
              + "    End-to-End Packet Rate:            %.2f Mpps (%,.0f PPS)%n"
              + "    End-to-End Throughput:             %.2f MB/s%n"
              + "========================================================================%n",
          packetCount,
          (packetCount * payloadSize) / (1024.0 * 1024.0),
          durationSec,
          mpps,
          mpps * 1_000_000.0,
          mbPerSec);

      assertEquals(packetCount, handledPackets.get());
      assertEquals((long) packetCount * payloadSize, handledBytes.get());
      assertTrue(mpps > 0.3, "End-to-end AF_XDP -> Netty pipeline rate must exceed 300,000 PPS");
    } finally {

      channel.finishAndReleaseAll();
    }
  }

  private static int writeEthernetIpv6UdpPacket(
      MemorySegment seg, int srcPort, int dstPort, byte[] payload) {
    int udpLen = 8 + payload.length;
    final int totalLen = 14 + 40 + udpLen;

    // Ethernet: 2B ethertype IPv6 (0x86DD)
    AfXdpFrameParser.setShortBE(seg, 12, 0x86DD);

    // IPv6: Version=6 (0x60), Payload Length (2B), Next Header=UDP (17), Hop Limit=64
    seg.set(ValueLayout.JAVA_BYTE, 14, (byte) 0x60);
    AfXdpFrameParser.setShortBE(seg, 18, udpLen);
    seg.set(ValueLayout.JAVA_BYTE, 20, (byte) 17); // Next Header: UDP
    seg.set(ValueLayout.JAVA_BYTE, 21, (byte) 64); // Hop Limit

    // UDP: srcPort, dstPort, length, checksum
    int udpOffset = 14 + 40;
    AfXdpFrameParser.setShortBE(seg, udpOffset, srcPort);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 2, dstPort);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 4, udpLen);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 6, 0);

    // Payload
    MemorySegment payloadDest = seg.asSlice(udpOffset + 8, payload.length);
    MemorySegment.copy(MemorySegment.ofArray(payload), 0, payloadDest, 0, payload.length);

    return totalLen;
  }
}
