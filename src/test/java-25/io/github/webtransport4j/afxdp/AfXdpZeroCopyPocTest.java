package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Proof of Concept regression and performance test for AF_XDP zero-copy packet ingestion.
 */
public class AfXdpZeroCopyPocTest {

  private static final int WEBTRANSPORT_PORT = 443;
  private static final int CLIENT_PORT = 54321;

  @Test
  public void testZeroCopyFrameParsingAndDelivery() throws Exception {
    try (AfXdpUmem umem = new AfXdpUmem(4096, 256)) {
      AtomicReference<String> receivedPayload = new AtomicReference<>();
      AtomicReference<ByteBuf> capturedBuf = new AtomicReference<>();

      AfXdpPacketDispatcher dispatcher =
          new AfXdpPacketDispatcher(
              umem,
              WEBTRANSPORT_PORT,
              buf -> {
                capturedBuf.set(buf);
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                receivedPayload.set(new String(bytes, StandardCharsets.UTF_8));
              });

      // Lease frame representing incoming DMA buffer from the NIC
      long frameOffset = umem.leaseFrame();
      assertEquals(255, umem.getFreeFrameCount());

      String testMsg = "WebTransport-AF_XDP-PoC-Payload";
      int totalWireLen =
          writeEthernetIpv4UdpPacket(
              umem.getFrameSlice(frameOffset, 4096),
              CLIENT_PORT,
              WEBTRANSPORT_PORT,
              testMsg.getBytes(StandardCharsets.UTF_8));

      // Dispatch simulated RX packet descriptor
      boolean delivered = dispatcher.onRxDescriptor(frameOffset, totalWireLen);

      assertTrue(delivered, "Packet destined for port 443 must be accepted");
      assertEquals(1, dispatcher.getPacketsReceived());
      assertEquals(0, dispatcher.getPacketsDropped());
      assertEquals(testMsg, receivedPayload.get());

      ByteBuf buf = capturedBuf.get();
      assertNotNull(buf);
      assertTrue(buf.isDirect(), "Wrapped buffer must point directly to off-heap native memory");
      assertFalse(buf.hasArray(), "Off-heap buffer must not have backing Java heap array");
      assertEquals(256, umem.getFreeFrameCount(), "Processed frame must be recycled back to UMEM pool");
    }
  }

  @Test
  public void testDropsNonWebTransportTraffic() throws Exception {
    try (AfXdpUmem umem = new AfXdpUmem(4096, 64)) {
      AfXdpPacketDispatcher dispatcher =
          new AfXdpPacketDispatcher(umem, WEBTRANSPORT_PORT, buf -> {});

      long frameOffset = umem.leaseFrame();
      assertEquals(63, umem.getFreeFrameCount());

      int totalWireLen =
          writeEthernetIpv4UdpPacket(
              umem.getFrameSlice(frameOffset, 4096),
              CLIENT_PORT,
              8080, // Different port
              new byte[] {1, 2, 3});

      boolean delivered = dispatcher.onRxDescriptor(frameOffset, totalWireLen);
      assertFalse(delivered, "Non-WebTransport port must be filtered out");
      assertEquals(0, dispatcher.getPacketsReceived());
      assertEquals(1, dispatcher.getPacketsDropped());
      assertEquals(64, umem.getFreeFrameCount(), "Frame must be recycled even on drop");
    }
  }

  @Test
  public void testHighVelocityThroughputBenchmark() throws Exception {
    int iterations = 200_000;
    try (AfXdpUmem umem = new AfXdpUmem(4096, 1024)) {
      AtomicInteger count = new AtomicInteger(0);
      AfXdpPacketDispatcher dispatcher =
          new AfXdpPacketDispatcher(umem, WEBTRANSPORT_PORT, buf -> count.incrementAndGet());

      byte[] payload = new byte[1024]; // 1 KiB datagram payload
      long frameOffset = umem.leaseFrame();
      int wireLen =
          writeEthernetIpv4UdpPacket(
              umem.getFrameSlice(frameOffset, 4096),
              CLIENT_PORT,
              WEBTRANSPORT_PORT,
              payload);

      long startNanos = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        dispatcher.onRxDescriptor(frameOffset, wireLen);
      }
      long elapsedNanos = System.nanoTime() - startNanos;

      double durationSec = elapsedNanos / 1_000_000_000.0;
      double mpps = (iterations / durationSec) / 1_000_000.0;
      double mbPerSec = ((long) iterations * payload.length) / (1024.0 * 1024.0) / durationSec;

      System.out.printf(
          "%n>>> [AF_XDP PoC Microbenchmark] Processed %,d frames in %.3f s: %.2f Mpps (%.2f MB/s)%n",
          iterations, durationSec, mpps, mbPerSec);

      assertEquals(iterations, count.get());
      assertTrue(mpps > 0.5, "Single-thread zero-copy parsing must exceed 500,000 pps");
    }
  }

  @Test
  public void testZeroCopyIpv6FrameParsingAndDelivery() throws Exception {
    try (AfXdpUmem umem = new AfXdpUmem(4096, 64)) {
      AtomicReference<String> receivedPayload = new AtomicReference<>();
      AfXdpPacketDispatcher dispatcher =
          new AfXdpPacketDispatcher(
              umem,
              WEBTRANSPORT_PORT,
              buf -> {
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                receivedPayload.set(new String(bytes, StandardCharsets.UTF_8));
              });

      long frameOffset = umem.leaseFrame();
      String testMsg = "WebTransport-IPv6-Direct-Bypass";
      int wireLen =
          writeEthernetIpv6UdpPacket(
              umem.getFrameSlice(frameOffset, 4096),
              CLIENT_PORT,
              WEBTRANSPORT_PORT,
              testMsg.getBytes(StandardCharsets.UTF_8));

      boolean delivered = dispatcher.onRxDescriptor(frameOffset, wireLen);
      assertTrue(delivered, "IPv6 packet destined for port 443 must be accepted");
      assertEquals(1, dispatcher.getPacketsReceived());
      assertEquals(testMsg, receivedPayload.get());
      assertEquals(64, umem.getFreeFrameCount(), "Frame must be recycled back to UMEM pool");
    }
  }

  /**
   * Encodes a synthetic Ethernet (14B) + IPv4 (20B) + UDP (8B) frame into off-heap memory.
   */
  private static int writeEthernetIpv4UdpPacket(
      MemorySegment seg, int srcPort, int dstPort, byte[] payload) {
    int udpLen = 8 + payload.length;
    int ipTotalLen = 20 + udpLen;
    final int totalLen = 14 + ipTotalLen;

    // Ethernet: 6B dst, 6B src, 2B ethertype (0x0800)
    AfXdpFrameParser.setShortBE(seg, 12, 0x0800);

    // IPv4: Version=4, IHL=5 (0x45), total length, protocol=UDP (17)
    seg.set(ValueLayout.JAVA_BYTE, 14, (byte) 0x45);
    seg.set(ValueLayout.JAVA_BYTE, 15, (byte) 0x00);
    AfXdpFrameParser.setShortBE(seg, 16, ipTotalLen);
    seg.set(ValueLayout.JAVA_BYTE, 23, (byte) 17); // IPPROTO_UDP

    // UDP: srcPort, dstPort, length, checksum
    int udpOffset = 14 + 20;
    AfXdpFrameParser.setShortBE(seg, udpOffset, srcPort);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 2, dstPort);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 4, udpLen);
    AfXdpFrameParser.setShortBE(seg, udpOffset + 6, 0); // 0 = no checksum

    // Payload
    MemorySegment payloadDest = seg.asSlice(udpOffset + 8, payload.length);
    MemorySegment.copy(MemorySegment.ofArray(payload), 0, payloadDest, 0, payload.length);

    return totalLen;
  }

  /**
   * Encodes a synthetic Ethernet (14B) + IPv6 (40B) + UDP (8B) frame into off-heap memory.
   */
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
