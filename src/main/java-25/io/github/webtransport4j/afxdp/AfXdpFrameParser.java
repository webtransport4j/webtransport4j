package io.github.webtransport4j.afxdp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Zero-copy parser and frame builder for raw Layer 2 Ethernet, IPv4, IPv6, and UDP frames.
 */
public final class AfXdpFrameParser {

  public static final int ETH_HDR_LEN = 14;
  public static final int IPV4_HDR_MIN_LEN = 20;
  public static final int IPV6_HDR_LEN = 40;
  public static final int UDP_HDR_LEN = 8;
  public static final int ETH_P_IP = 0x0800;
  public static final int ETH_P_IPV6 = 0x86DD;
  public static final byte IPPROTO_UDP = 17;

  private AfXdpFrameParser() {}

  /**
   * Represents parsed packet metadata, addressing, and payload.
   */
  public static final class ParsedDatagram {
    private final InetSocketAddress sender;
    private final InetSocketAddress recipient;
    private final ByteBuf payload;
    private final byte[] sourceMac;
    private final byte[] destMac;
    private final int etherType;
    private final long frameAddress;

    public ParsedDatagram(
        InetSocketAddress sender,
        InetSocketAddress recipient,
        ByteBuf payload,
        byte[] sourceMac,
        byte[] destMac,
        int etherType,
        long frameAddress) {
      this.sender = sender;
      this.recipient = recipient;
      this.payload = Objects.requireNonNull(payload, "payload");
      this.sourceMac = sourceMac;
      this.destMac = destMac;
      this.etherType = etherType;
      this.frameAddress = frameAddress;
    }

    public InetSocketAddress getSender() {
      return sender;
    }

    public InetSocketAddress getRecipient() {
      return recipient;
    }

    public int getSrcPort() {
      return sender != null ? sender.getPort() : 0;
    }

    public int getDstPort() {
      return recipient != null ? recipient.getPort() : 0;
    }

    public ByteBuf getPayload() {
      return payload;
    }

    public byte[] getSourceMac() {
      return sourceMac;
    }

    public byte[] getDestMac() {
      return destMac;
    }

    public int getEtherType() {
      return etherType;
    }

    public long getFrameAddress() {
      return frameAddress;
    }

    /**
     * Releases the wrapped payload buffer, which in turn recycles the underlying UMEM frame.
     */
    public void release() {
      if (payload.refCnt() > 0) {
        payload.release();
      }
    }
  }

  /**
   * Parses an Ethernet/IP/UDP packet frame directly from off-heap memory.
   *
   * @param segment memory slice containing the full frame
   * @param frameLen total wire length in bytes
   * @param umem UMEM pool backing the frame (for recycling on ByteBuf release)
   * @param frameAddress relative offset of the frame in UMEM
   * @return parsed datagram with wrapped off-heap ByteBuf, or null if frame is not UDP
   */
  public static ParsedDatagram parse(
      MemorySegment segment, int frameLen, AfXdpUmem umem, long frameAddress) {
    if (frameLen < ETH_HDR_LEN + IPV4_HDR_MIN_LEN + UDP_HDR_LEN) {
      return null;
    }

    byte[] destMac = new byte[6];
    byte[] srcMac = new byte[6];
    for (int i = 0; i < 6; i++) {
      destMac[i] = segment.get(ValueLayout.JAVA_BYTE, i);
      srcMac[i] = segment.get(ValueLayout.JAVA_BYTE, 6 + i);
    }

    int etherType = getUnsignedShortBE(segment, 12);
    int udpOffset;
    byte[] srcIpBytes;
    byte[] dstIpBytes;

    if (etherType == ETH_P_IP) {
      byte versionAndIhl = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN);
      int ihl = (versionAndIhl & 0x0F) * 4;
      if (ihl < 20 || frameLen < ETH_HDR_LEN + ihl + UDP_HDR_LEN) {
        return null;
      }
      byte protocol = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 9);
      if (protocol != IPPROTO_UDP) {
        return null;
      }

      srcIpBytes = new byte[4];
      dstIpBytes = new byte[4];
      for (int i = 0; i < 4; i++) {
        srcIpBytes[i] = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 12 + i);
        dstIpBytes[i] = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 16 + i);
      }
      udpOffset = ETH_HDR_LEN + ihl;
    } else if (etherType == ETH_P_IPV6) {
      if (frameLen < ETH_HDR_LEN + IPV6_HDR_LEN + UDP_HDR_LEN) {
        return null;
      }
      byte nextHeader = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 6);
      if (nextHeader != IPPROTO_UDP) {
        return null;
      }

      srcIpBytes = new byte[16];
      dstIpBytes = new byte[16];
      for (int i = 0; i < 16; i++) {
        srcIpBytes[i] = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 8 + i);
        dstIpBytes[i] = segment.get(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 24 + i);
      }
      udpOffset = ETH_HDR_LEN + IPV6_HDR_LEN;
    } else {
      return null;
    }

    int srcPort = getUnsignedShortBE(segment, udpOffset);
    int dstPort = getUnsignedShortBE(segment, udpOffset + 2);
    int udpLen = getUnsignedShortBE(segment, udpOffset + 4);

    if (udpLen < UDP_HDR_LEN) {
      return null;
    }

    int payloadOffset = udpOffset + UDP_HDR_LEN;
    int payloadLen = udpLen - UDP_HDR_LEN;

    if (payloadOffset + payloadLen > frameLen) {
      return null;
    }

    InetAddress srcIp;
    InetAddress dstIp;
    try {
      srcIp = InetAddress.getByAddress(srcIpBytes);
      dstIp = InetAddress.getByAddress(dstIpBytes);
    } catch (UnknownHostException e) {
      return null;
    }

    InetSocketAddress sender = new InetSocketAddress(srcIp, srcPort);
    InetSocketAddress recipient = new InetSocketAddress(dstIp, dstPort);

    MemorySegment payloadSlice = segment.asSlice(payloadOffset, payloadLen);
    ByteBuffer nioBuffer = payloadSlice.asByteBuffer();
    ByteBuf directBuf = Unpooled.wrappedBuffer(nioBuffer);
    ByteBuf nettyBuf =
        umem != null ? new AfXdpFrameByteBuf(directBuf, umem, frameAddress) : directBuf;

    return new ParsedDatagram(
        sender, recipient, nettyBuf, srcMac, destMac, etherType, frameAddress);
  }

  /**
   * Overload for backward compatibility with tests without UMEM recycling.
   */
  public static ParsedDatagram parse(MemorySegment segment, int frameLen) {
    return parse(segment, frameLen, null, -1L);
  }

  /**
   * Serializes an outbound Ethernet II, IP, and UDP packet frame into UMEM memory.
   *
   * @param segment memory slice in UMEM to write into
   * @param srcMac local source MAC address (6 bytes)
   * @param dstMac destination MAC address (6 bytes)
   * @param srcAddr local source address and port
   * @param dstAddr remote recipient address and port
   * @param payload packet payload bytes
   * @return total wire bytes written
   */
  public static int writeFrame(
      MemorySegment segment,
      byte[] srcMac,
      byte[] dstMac,
      InetSocketAddress srcAddr,
      InetSocketAddress dstAddr,
      ByteBuf payload) {
    byte[] dstIp = dstAddr.getAddress().getAddress();
    byte[] srcIp = srcAddr.getAddress().getAddress();
    boolean isIpv6 = dstIp.length == 16;

    int payloadLen = payload.readableBytes();
    int udpLen = UDP_HDR_LEN + payloadLen;
    int ipHdrLen = isIpv6 ? IPV6_HDR_LEN : IPV4_HDR_MIN_LEN;
    int totalLen = ETH_HDR_LEN + ipHdrLen + udpLen;

    // 1. Ethernet Header
    for (int i = 0; i < 6; i++) {
      segment.set(ValueLayout.JAVA_BYTE, i, dstMac != null && dstMac.length >= 6 ? dstMac[i] : (byte) 0xFF);
      segment.set(ValueLayout.JAVA_BYTE, 6 + i, srcMac != null && srcMac.length >= 6 ? srcMac[i] : (byte) 0x02);
    }
    setShortBE(segment, 12, isIpv6 ? ETH_P_IPV6 : ETH_P_IP);

    int udpOffset;
    if (isIpv6) {
      // 2. IPv6 Header (40 bytes fixed)
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN, (byte) 0x60); // Version 6
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 1, (byte) 0x00);
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 2, (byte) 0x00);
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 3, (byte) 0x00);
      setShortBE(segment, ETH_HDR_LEN + 4, udpLen);
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 6, IPPROTO_UDP);
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 7, (byte) 64); // Hop Limit

      for (int i = 0; i < 16; i++) {
        segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 8 + i, i < srcIp.length ? srcIp[i] : 0);
        segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 24 + i, dstIp[i]);
      }
      udpOffset = ETH_HDR_LEN + IPV6_HDR_LEN;
    } else {
      // 2. IPv4 Header (20 bytes standard)
      int ipTotalLen = IPV4_HDR_MIN_LEN + udpLen;
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN, (byte) 0x45); // Version 4, IHL 5
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 1, (byte) 0x00); // DSCP/ECN
      setShortBE(segment, ETH_HDR_LEN + 2, ipTotalLen);
      setShortBE(segment, ETH_HDR_LEN + 4, 0); // ID
      setShortBE(segment, ETH_HDR_LEN + 6, 0x4000); // DF
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 8, (byte) 64); // TTL
      segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 9, IPPROTO_UDP);
      setShortBE(segment, ETH_HDR_LEN + 10, 0); // Checksum placeholder

      for (int i = 0; i < 4; i++) {
        segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 12 + i, i < srcIp.length ? srcIp[i] : 0);
        segment.set(ValueLayout.JAVA_BYTE, ETH_HDR_LEN + 16 + i, dstIp[i]);
      }

      int ipChecksum = computeIpChecksum(segment, ETH_HDR_LEN, IPV4_HDR_MIN_LEN);
      setShortBE(segment, ETH_HDR_LEN + 10, ipChecksum);
      udpOffset = ETH_HDR_LEN + IPV4_HDR_MIN_LEN;
    }

    // 3. UDP Header
    setShortBE(segment, udpOffset, srcAddr.getPort());
    setShortBE(segment, udpOffset + 2, dstAddr.getPort());
    setShortBE(segment, udpOffset + 4, udpLen);
    setShortBE(segment, udpOffset + 6, 0); // Checksum optional in IPv4

    // 4. Payload
    int payloadOffset = udpOffset + UDP_HDR_LEN;
    int readerIndex = payload.readerIndex();
    for (int i = 0; i < payloadLen; i++) {
      segment.set(ValueLayout.JAVA_BYTE, payloadOffset + i, payload.getByte(readerIndex + i));
    }

    return totalLen;
  }

  public static int getUnsignedShortBE(MemorySegment segment, int offset) {
    int b0 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, offset));
    int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, offset + 1));
    return (b0 << 8) | b1;
  }

  public static void setShortBE(MemorySegment segment, int offset, int val) {
    segment.set(ValueLayout.JAVA_BYTE, offset, (byte) ((val >> 8) & 0xFF));
    segment.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) (val & 0xFF));
  }

  public static int computeIpChecksum(MemorySegment segment, int offset, int length) {
    long sum = 0;
    for (int i = 0; i < length; i += 2) {
      sum += getUnsignedShortBE(segment, offset + i);
    }
    while ((sum >> 16) > 0) {
      sum = (sum & 0xFFFF) + (sum >> 16);
    }
    return (int) (~sum & 0xFFFF);
  }
}
