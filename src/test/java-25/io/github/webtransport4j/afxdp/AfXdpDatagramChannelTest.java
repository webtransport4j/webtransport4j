package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import java.lang.foreign.MemorySegment;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit and integration tests for {@link AfXdpDatagramChannel}.
 */
public class AfXdpDatagramChannelTest {

  private static final int SERVER_PORT = 4433;
  private static final int CLIENT_PORT = 54321;

  private EventLoopGroup group;
  private AfXdpUmem umem;
  private AfXdpDatagramChannel channel;

  /** Sets up test resources. */
  @BeforeEach
  public void setUp() {
    group = new DefaultEventLoopGroup(1);
    umem = new AfXdpUmem(4096, 128);
  }

  /** Tears down test resources. */
  @AfterEach
  public void tearDown() {
    if (channel != null && channel.isOpen()) {
      channel.close().syncUninterruptibly();
    }
    if (group != null) {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }
    if (umem != null && !umem.isClosed()) {
      umem.close();
    }
  }

  @Test
  public void testBootstrapAndBind() throws Exception {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();

    assertTrue(channel.isOpen());
    assertTrue(channel.isActive());
    assertEquals(SERVER_PORT, channel.localAddress().getPort());
    assertNull(channel.remoteAddress());
    assertFalse(channel.isConnected());
  }

  @Test
  public void testInboundDatagramPacketPipelineDelivery() throws Exception {
    AtomicReference<DatagramPacket> receivedPacket = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
            receivedPacket.set(msg.retainedDuplicate());
            latch.countDown();
          }
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();

    // Prepare an incoming IPv4 UDP packet in UMEM
    long frameOffset = umem.leaseFrame();
    byte[] payloadBytes = "Hello-WebTransport-AF_XDP".getBytes(StandardCharsets.UTF_8);

    InetSocketAddress senderAddr = new InetSocketAddress(InetAddress.getByName("192.168.1.100"), CLIENT_PORT);
    InetSocketAddress recipientAddr = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), SERVER_PORT);
    byte[] clientMac = new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    byte[] serverMac = new byte[] {0x02, 0x00, 0x00, 0x00, 0x00, 0x01};

    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payloadBytes);
    MemorySegment seg = umem.getFrameSlice(frameOffset, 4096);
    int wireLen = AfXdpFrameParser.writeFrame(seg, clientMac, serverMac, senderAddr, recipientAddr, payloadBuf);

    // Ingest into AF_XDP channel
    boolean fed = channel.feedRxFrame(frameOffset, wireLen);
    assertTrue(fed, "Frame should be accepted by channel");

    assertTrue(latch.await(3, TimeUnit.SECONDS), "Packet should be received in pipeline");
    DatagramPacket packet = receivedPacket.get();
    assertNotNull(packet);

    assertEquals(SERVER_PORT, packet.recipient().getPort());
    assertEquals(CLIENT_PORT, packet.sender().getPort());
    assertEquals("192.168.1.100", packet.sender().getAddress().getHostAddress());

    byte[] readContent = new byte[packet.content().readableBytes()];
    packet.content().readBytes(readContent);
    assertEquals("Hello-WebTransport-AF_XDP", new String(readContent, StandardCharsets.UTF_8));

    assertEquals(1, channel.getPacketsReceived());
    assertEquals(payloadBytes.length, channel.getBytesReceived());

    // Release packet and verify frame was recycled back to UMEM
    packet.release();
    assertEquals(128, umem.getFreeFrameCount(), "UMEM frame must be recycled back after ByteBuf release");
  }

  @Test
  public void testPortFilteringDropsMismatchedDatagrams() throws Exception {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();

    // Prepare packet destined for port 80 (not 4433)
    long frameOffset = umem.leaseFrame();
    InetSocketAddress senderAddr = new InetSocketAddress(InetAddress.getByName("10.0.0.1"), CLIENT_PORT);
    InetSocketAddress wrongRecipient = new InetSocketAddress(InetAddress.getByName("10.0.0.2"), 80);

    ByteBuf payloadBuf = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
    MemorySegment seg = umem.getFrameSlice(frameOffset, 4096);
    int wireLen = AfXdpFrameParser.writeFrame(seg, null, null, senderAddr, wrongRecipient, payloadBuf);

    boolean fed = channel.feedRxFrame(frameOffset, wireLen);
    assertFalse(fed, "Mismatched destination port must be rejected");
    assertEquals(1, channel.getPacketsDropped());
    assertEquals(128, umem.getFreeFrameCount(), "Rejected frame must be recycled immediately to UMEM");
  }

  @Test
  public void testOutboundWriteFramesEthernetIpUdpWithMacLearning() throws Exception {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();

    InetAddress clientIp = InetAddress.getByName("192.168.1.50");
    byte[] clientMac = new byte[] {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD, (byte) 0xEE, (byte) 0xFF};

    // Pre-populate or learn MAC in neighbor table
    channel.updateNeighborMac(clientIp, clientMac);

    InetSocketAddress recipient = new InetSocketAddress(clientIp, 9999);
    byte[] outboundBytes = "Response-From-WebTransport-Server".getBytes(StandardCharsets.UTF_8);
    ByteBuf outBuf = Unpooled.wrappedBuffer(outboundBytes);

    DatagramPacket writePacket = new DatagramPacket(outBuf, recipient);
    channel.writeAndFlush(writePacket).sync();

    assertEquals(1, channel.getPacketsSent());
    assertEquals(outboundBytes.length, channel.getBytesSent());

    // Verify the TX ring descriptor generated in UMEM
    AfXdpUmem.TxDescriptor txDesc = umem.pollTx();
    assertNotNull(txDesc, "TX descriptor must be present in UMEM TX queue");

    MemorySegment txSeg = umem.getFrameSlice(txDesc.getFrameAddress(), txDesc.getFrameLength());
    AfXdpFrameParser.ParsedDatagram parsedTx =
        AfXdpFrameParser.parse(txSeg, txDesc.getFrameLength());

    assertNotNull(parsedTx);
    assertEquals(SERVER_PORT, parsedTx.getSrcPort());
    assertEquals(9999, parsedTx.getDstPort());
    assertEquals("192.168.1.50", parsedTx.getRecipient().getAddress().getHostAddress());

    // Verify learned destination MAC was placed in Ethernet header!
    byte[] wireDstMac = parsedTx.getDestMac();
    for (int i = 0; i < 6; i++) {
      assertEquals(clientMac[i], wireDstMac[i], "Destination MAC must match learned neighbor MAC");
    }

    umem.completeTx(txDesc.getFrameAddress());
    assertEquals(128, umem.getFreeFrameCount());
  }

  @Test
  public void testMulticastUnsupportedOperations() throws Exception {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();
    InetAddress groupAddr = InetAddress.getByName("239.255.0.1");

    ChannelFuture future = channel.joinGroup(groupAddr);
    assertTrue(future.await(1, TimeUnit.SECONDS));
    assertFalse(future.isSuccess());
    assertTrue(future.cause() instanceof UnsupportedOperationException);
  }

  @Test
  public void testAfXdpChannelOptionsAndMetrics() throws Exception {
    byte[] srcMac = new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    byte[] gwMac = new byte[] {0x02, (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD, (byte) 0xEE};

    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channelFactory(() -> new AfXdpDatagramChannel(umem))
        .option(AfXdpChannelOption.INTERFACE_NAME, "eth0")
        .option(AfXdpChannelOption.QUEUE_ID, 3)
        .option(AfXdpChannelOption.SOURCE_MAC, srcMac)
        .option(AfXdpChannelOption.GATEWAY_MAC, gwMac)
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
        });

    channel = (AfXdpDatagramChannel) bootstrap.bind(SERVER_PORT).sync().channel();

    assertEquals("eth0", channel.config().getInterfaceName());
    assertEquals(3, channel.config().getQueueId());
    assertNotNull(channel.config().getSourceMac());
    assertEquals(srcMac[0], channel.config().getSourceMac()[0]);
    assertNotNull(channel.config().getGatewayMac());
    assertEquals(gwMac[0], channel.config().getGatewayMac()[0]);

    assertEquals(0, channel.getActiveLeasedFrames());
    assertEquals(128, channel.getFreeFrames());
    assertEquals(0, channel.getPacketsDropped());
  }
}
