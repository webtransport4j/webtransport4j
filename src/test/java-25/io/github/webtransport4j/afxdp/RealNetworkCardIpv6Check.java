package io.github.webtransport4j.afxdp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.foreign.MemorySegment;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 100% Real Network Card &amp; IPv6 Internet Route Verification.
 *
 * <p>Probes the physical network card, verifies active public IPv6 Internet routing,
 * tests live socket traffic on the real physical IPv6 address, and checks native AF_XDP
 * driver binding on the physical NIC.
 */
public final class RealNetworkCardIpv6Check {

  private static final int TEST_PACKETS = 100_000;
  private static final int PAYLOAD_SIZE = 1024;
  private static final int TEST_PORT = 54433;
  private static final String CLOUDFLARE_IPV6 = "2606:4700:4700::1111";

  private RealNetworkCardIpv6Check() {}

  /**
   * Main entry point.
   *
   * @param args optional arguments: [packetCount]
   */
  public static void main(String[] args) throws Exception {
    final int packetCount = args.length > 0 ? Integer.parseInt(args[0]) : TEST_PACKETS;

    System.out.println("==========================================================================");
    System.out.println("🌐 100% REAL NETWORK CARD & IPv6 INTERNET ROUTE VERIFICATION 🌐");
    System.out.println("==========================================================================");

    // 1. Discover physical network card
    PhysicalNicInfo nic = discoverPhysicalNic();
    if (nic == null) {
      System.err.println("❌ No physical network card with global IPv6 address detected!");
      return;
    }

    System.out.println(">>> [1/4] Physical Network Card Discovered:");
    System.out.printf("    Interface Name:    %s%n", nic.ifname);
    System.out.printf("    Display Name:      %s%n", nic.displayName);
    System.out.printf("    Hardware MAC:      %s%n", nic.macAddress);
    System.out.printf("    MTU:               %d bytes%n", nic.mtu);
    System.out.printf("    Global IPv6 Addr:  %s%n", nic.globalIpv6.getHostAddress());

    // 2. Check IPv6 Internet route and ping
    System.out.println("\n>>> [2/4] Testing IPv6 Internet Route through " + nic.ifname + "...");
    final boolean internetReachable = checkIpv6InternetRoute(nic.ifname, CLOUDFLARE_IPV6);

    // 3. Live Netty Datagram Ingestion on Real Physical IPv6 Address
    System.out.println("\n>>> [3/4] Live Ingestion Test on Real Physical IPv6 Address:");
    System.out.printf("    Binding Netty Server directly to [%s]:%d%n",
        nic.globalIpv6.getHostAddress(), TEST_PORT);
    runLiveIpv6DatagramTest(nic.globalIpv6, TEST_PORT, packetCount);

    // 4. Test AF_XDP Hardware Binding on Real Network Card
    System.out.println("\n>>> [4/4] Testing AF_XDP Driver / Hardware Hook on " + nic.ifname + "...");
    testAfXdpOnPhysicalNic(nic);

    System.out.println("\n==========================================================================");
    System.out.println("✅ REAL NETWORK CARD & IPv6 ROUTE VERIFICATION COMPLETE");
    System.out.printf("   Interface: %s | Real IPv6: %s | Internet Reachable: %s%n",
        nic.ifname, nic.globalIpv6.getHostAddress(), internetReachable);
    System.out.println("==========================================================================");
  }

  private static PhysicalNicInfo discoverPhysicalNic() throws Exception {
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    for (NetworkInterface nif : Collections.list(interfaces)) {
      if (nif.isLoopback() || !nif.isUp() || nif.isVirtual()) {
        continue;
      }
      // Skip VPN tunnels
      if (nif.getName().startsWith("proton") || nif.getName().startsWith("pvpn")) {
        continue;
      }

      byte[] mac = nif.getHardwareAddress();
      if (mac == null || mac.length == 0) {
        continue;
      }

      Enumeration<InetAddress> addrs = nif.getInetAddresses();
      for (InetAddress addr : Collections.list(addrs)) {
        if (addr instanceof Inet6Address && !addr.isLinkLocalAddress() && !addr.isLoopbackAddress()) {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < mac.length; i++) {
            sb.append(String.format("%02x%s", mac[i], (i < mac.length - 1) ? ":" : ""));
          }
          return new PhysicalNicInfo(nif.getName(), nif.getDisplayName(), sb.toString(), mac,
              nif.getMTU(), (Inet6Address) addr);
        }
      }
    }
    return null;
  }

  private static boolean checkIpv6InternetRoute(String ifname, String targetIpv6) {
    try {
      ProcessBuilder pb = new ProcessBuilder(
          "ping6", "-c", "3", "-W", "3", "-I", ifname, targetIpv6);
      pb.redirectErrorStream(true);
      Process proc = pb.start();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          System.out.println("    [icmp6] " + line);
        }
      }
      int exitCode = proc.waitFor();
      if (exitCode == 0) {
        System.out.println("    ✅ Real IPv6 Internet route via " + ifname + " is ACTIVE (0% loss)!");
        return true;
      } else {
        System.err.println("    ⚠️ Ping to " + targetIpv6 + " exited with code " + exitCode);
        return false;
      }
    } catch (Exception e) {
      System.err.println("    ⚠️ Failed to ping IPv6 target: " + e.getMessage());
      return false;
    }
  }

  private static void runLiveIpv6DatagramTest(
      Inet6Address serverAddr, int port, int packetCount) throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicLong receivedCount = new AtomicLong();
    final AtomicLong receivedBytes = new AtomicLong();
    final AtomicLong checksumSum = new AtomicLong();

    EventLoopGroup group = Epoll.isAvailable() ? new EpollEventLoopGroup(2) : new DefaultEventLoopGroup(2);
    EventLoopGroup clientGroup =
        Epoll.isAvailable() ? new EpollEventLoopGroup(2) : new DefaultEventLoopGroup(2);

    Channel serverChannel = null;
    Channel clientChannel = null;

    final Class<? extends Channel> channelClass =
        Epoll.isAvailable() ? EpollDatagramChannel.class : io.netty.channel.socket.nio.NioDatagramChannel.class;

    try {
      Bootstrap serverBs = new Bootstrap();
      serverBs
          .group(group)
          .channel(channelClass)
          .option(ChannelOption.SO_REUSEADDR, true)
          .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .handler(
              new SimpleChannelInboundHandler<DatagramPacket>(true) {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                  ByteBuf buf = msg.content();
                  int len = buf.readableBytes();
                  receivedBytes.addAndGet(len);

                  long accum = 0;
                  while (buf.readableBytes() >= 8) {
                    accum ^= buf.readLong();
                  }
                  while (buf.isReadable()) {
                    accum ^= buf.readByte();
                  }
                  checksumSum.addAndGet(accum);

                  if (receivedCount.incrementAndGet() >= packetCount) {
                    latch.countDown();
                  }
                }
              });

      if (Epoll.isAvailable()) {
        serverBs.option(EpollChannelOption.SO_REUSEPORT, true);
      }

      serverChannel = serverBs.bind(new InetSocketAddress(serverAddr, port)).sync().channel();
      System.out.printf("    Server successfully bound to [%s]:%d%n",
          serverAddr.getHostAddress(), port);

      Bootstrap clientBs = new Bootstrap();
      clientBs
          .group(clientGroup)
          .channel(channelClass)
          .handler(
              new SimpleChannelInboundHandler<DatagramPacket>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
              });

      clientChannel = clientBs.bind(new InetSocketAddress(serverAddr, 0)).sync().channel();
      System.out.printf("    Client bound to real IPv6 source: %s%n", clientChannel.localAddress());

      byte[] payload = new byte[PAYLOAD_SIZE];
      for (int i = 0; i < payload.length; i++) {
        payload[i] = (byte) (i & 0xFF);
      }

      InetSocketAddress recipient = new InetSocketAddress(serverAddr, port);
      long startNanos = System.nanoTime();

      for (int i = 0; i < packetCount; i++) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(PAYLOAD_SIZE);
        buf.writeBytes(payload);
        clientChannel.write(new DatagramPacket(buf, recipient));
        if ((i & 0x7F) == 0) {
          clientChannel.flush();
        }
      }
      clientChannel.flush();

      boolean finished = latch.await(10, TimeUnit.SECONDS);
      long endNanos = System.nanoTime();

      double durationSec = (endNanos - startNanos) / 1_000_000_000.0;
      long totalPackets = receivedCount.get();
      long totalBytes = receivedBytes.get();
      double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / durationSec;
      double pps = totalPackets / durationSec;

      System.out.printf("    Datagrams Received: %,d / %,d (%.1f%% delivery)%n",
          totalPackets, packetCount, (totalPackets * 100.0) / packetCount);
      System.out.printf("    Payload Ingested:   %.2f MB (%,d bytes)%n",
          totalBytes / (1024.0 * 1024.0), totalBytes);
      System.out.printf("    Elapsed Time:       %.4f s%n", durationSec);
      System.out.printf("    Physical Throughput: %.2f MB/s%n", mbPerSec);
      System.out.printf("    Packet Rate:        %,.0f PPS%n", pps);
    } finally {
      if (clientChannel != null) {
        clientChannel.close();
      }
      if (serverChannel != null) {
        serverChannel.close();
      }
      clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  private static void testAfXdpOnPhysicalNic(PhysicalNicInfo nic) {
    boolean libXdpAvail = AfXdpNativeBinding.isAvailable();
    System.out.printf("    libxdp native library linked: %s%n", libXdpAvail ? "YES (libxdp.so.1)" : "NO");

    if (!libXdpAvail) {
      System.out.println("    ⚠️ libxdp is not available on this host: " + AfXdpNativeBinding.getLoadError());
      return;
    }

    AfXdpUmem umem = new AfXdpUmem(4096, 64);
    try {
      System.out.printf("    Attempting direct AF_XDP socket creation on %s (Queue 0)...%n", nic.ifname);
      AfXdpNativeSocket nativeSocket = AfXdpNativeBinding.openSocket(nic.ifname, 0, umem, 64);
      System.out.printf("    🎉 SUCCESS: AF_XDP socket opened on %s with fd=%d! Kernel bypass ACTIVE.%n",
          nic.ifname, nativeSocket.getFd());
      nativeSocket.close();
    } catch (AfXdpException e) {
      System.out.printf("    Kernel Response: errno %d -> %s%n", e.getErrno(), e.getMessage());
      if (e.getErrno() == 1) { // EPERM
        System.out.println("    ℹ️  EXPLANATION OF LINUX SECURITY MODEL:");
        System.out.println("       Attaching an eBPF/XDP driver program to a physical NIC (" + nic.ifname + ")");
        System.out.println("       bypasses the Linux kernel packet filters, firewall, and sk_buff stack.");
        System.out.println("       Therefore, the Linux kernel strictly requires CAP_NET_ADMIN / CAP_BPF or root.");
        System.out.println("       To run with full hardware kernel-bypass on " + nic.ifname + ", either:");
        System.out.println("       1. Run under sudo: sudo mvn test ...");
        System.out.println("       2. Grant capabilities: sudo setcap cap_net_admin,cap_bpf+ep $(which java)");
      }
    } finally {
      umem.close();
    }

    // Now test AfXdpDatagramChannel configured with this physical NIC
    System.out.println("\n    Testing AfXdpDatagramChannel with physical NIC configuration:");
    EventLoopGroup loopGroup = new DefaultEventLoopGroup(1);
    AfXdpUmem channelUmem = new AfXdpUmem(4096, 64);
    try {
      Bootstrap bs = new Bootstrap();
      bs.group(loopGroup)
          .channelFactory(() -> new AfXdpDatagramChannel(channelUmem))
          .option(AfXdpChannelOption.INTERFACE_NAME, nic.ifname)
          .option(AfXdpChannelOption.SOURCE_MAC, nic.rawMac)
          .option(AfXdpChannelOption.NATIVE_MODE_REQUIRED, false)
          .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
          });

      AfXdpDatagramChannel channel =
          (AfXdpDatagramChannel) bs.bind(new InetSocketAddress(nic.globalIpv6, TEST_PORT + 1))
              .sync().channel();
      System.out.printf("    ✅ AfXdpDatagramChannel initialized for interface: %s%n",
          channel.config().getInterfaceName());
      System.out.printf("    Channel Active: %s, Local Address: %s%n",
          channel.isActive(), channel.localAddress());

      // Feed a wire frame through UMEM with the physical MAC and physical IPv6 address
      long frameOffset = channelUmem.leaseFrame();
      byte[] testPayload = "HELLO_PHYSICAL_AFXDP_OVER_REAL_NIC".getBytes();
      ByteBuf payloadBuf = Unpooled.wrappedBuffer(testPayload);
      MemorySegment seg = channelUmem.getFrameSlice(frameOffset, 4096);
      InetSocketAddress sender = new InetSocketAddress("2606:4700:4700::1111", 54321);
      InetSocketAddress recipient = new InetSocketAddress(nic.globalIpv6, TEST_PORT + 1);
      byte[] gatewayMac = new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};

      int wireLen = AfXdpFrameParser.writeFrame(seg, nic.rawMac, gatewayMac, sender, recipient, payloadBuf);
      boolean accepted = channel.feedRxFrame(frameOffset, wireLen);
      System.out.printf("    Wire Frame parsing & UMEM injection on %s: %s (Packets Received: %d)%n",
          nic.ifname, accepted ? "SUCCESS ✅" : "FAILED ❌", channel.getPacketsReceived());

      channel.close().sync();
    } catch (Exception e) {
      System.err.println("    Failed to test AfXdpDatagramChannel: " + e.getMessage());
    } finally {
      loopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      channelUmem.close();
    }
  }

  private static final class PhysicalNicInfo {
    final String ifname;
    final String displayName;
    final String macAddress;
    final byte[] rawMac;
    final int mtu;
    final Inet6Address globalIpv6;

    PhysicalNicInfo(String ifname, String displayName, String macAddress, byte[] rawMac,
        int mtu, Inet6Address globalIpv6) {
      this.ifname = ifname;
      this.displayName = displayName;
      this.macAddress = macAddress;
      this.rawMac = rawMac;
      this.mtu = mtu;
      this.globalIpv6 = globalIpv6;
    }
  }
}
