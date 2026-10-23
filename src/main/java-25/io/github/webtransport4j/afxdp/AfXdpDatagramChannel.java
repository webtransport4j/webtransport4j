package io.github.webtransport4j.afxdp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import java.lang.foreign.MemorySegment;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Netty {@link DatagramChannel} implementation powered by Linux AF_XDP (XSK)
 * and Java 25 Foreign Function and Memory (FFM).
 *
 * <p>Provides zero-copy kernel-bypass datagram ingestion and transmission for QUIC and WebTransport.
 */
public class AfXdpDatagramChannel extends AbstractChannel implements DatagramChannel {

  private static final ChannelMetadata METADATA = new ChannelMetadata(true);
  private static final byte[] DEFAULT_SRC_MAC = new byte[] {0x02, 0x00, 0x00, 0x00, 0x00, 0x01};
  private static final byte[] DEFAULT_GATEWAY_MAC =
      new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
  private static final int MAX_NEIGHBOR_ENTRIES = 16384;

  private final AfXdpDatagramChannelConfig config;
  private final AfXdpUmem umem;
  private final boolean ownsUmem;
  private final byte[] sourceMac;
  private final byte[] defaultGatewayMac;
  private final ConcurrentMap<InetAddress, byte[]> neighborTable;

  private volatile InetSocketAddress localAddress;
  private volatile InetSocketAddress remoteAddress;
  private volatile boolean open = true;
  private volatile boolean active;
  private volatile boolean connected;
  private volatile AfXdpNativeSocket nativeSocket;

  private final AtomicLong packetsReceived = new AtomicLong();

  private final AtomicLong bytesReceived = new AtomicLong();
  private final AtomicLong packetsSent = new AtomicLong();
  private final AtomicLong bytesSent = new AtomicLong();
  private final AtomicLong packetsDropped = new AtomicLong();

  /**
   * Creates an AF_XDP DatagramChannel with default UMEM pool (16 MiB).
   */
  public AfXdpDatagramChannel() {
    this(new AfXdpUmem(), true, DEFAULT_SRC_MAC, DEFAULT_GATEWAY_MAC);
  }

  /**
   * Creates an AF_XDP DatagramChannel using an externally supplied {@link AfXdpUmem}.
   *
   * @param umem UMEM buffer pool
   */
  public AfXdpDatagramChannel(AfXdpUmem umem) {
    this(umem, false, DEFAULT_SRC_MAC, DEFAULT_GATEWAY_MAC);
  }

  /**
   * Creates an AF_XDP DatagramChannel with custom UMEM and source MAC.
   *
   * @param umem UMEM buffer pool
   * @param sourceMac local interface MAC address (6 bytes)
   */
  public AfXdpDatagramChannel(AfXdpUmem umem, byte[] sourceMac) {
    this(umem, false, sourceMac, DEFAULT_GATEWAY_MAC);
  }

  private AfXdpDatagramChannel(
      AfXdpUmem umem, boolean ownsUmem, byte[] sourceMac, byte[] defaultGatewayMac) {
    super(null);
    this.umem = Objects.requireNonNull(umem, "umem");
    this.ownsUmem = ownsUmem;
    this.sourceMac = sourceMac != null ? sourceMac.clone() : DEFAULT_SRC_MAC.clone();
    this.defaultGatewayMac =
        defaultGatewayMac != null ? defaultGatewayMac.clone() : DEFAULT_GATEWAY_MAC.clone();
    this.neighborTable = new ConcurrentHashMap<>();
    this.config = new AfXdpDatagramChannelConfig(this);
  }

  @Override
  public ChannelMetadata metadata() {
    return METADATA;
  }

  @Override
  public AfXdpDatagramChannelConfig config() {
    return config;
  }

  @Override
  public boolean isOpen() {
    return open && !umem.isClosed();
  }

  @Override
  public boolean isActive() {
    return isOpen() && active;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public InetSocketAddress localAddress() {
    return (InetSocketAddress) super.localAddress();
  }

  @Override
  public InetSocketAddress remoteAddress() {
    return (InetSocketAddress) super.remoteAddress();
  }

  @Override
  protected SocketAddress localAddress0() {
    return localAddress;
  }

  @Override
  protected SocketAddress remoteAddress0() {
    return remoteAddress;
  }

  @Override
  protected boolean isCompatible(EventLoop loop) {
    return true;
  }

  @Override
  protected AbstractUnsafe newUnsafe() {
    return new AfXdpUnsafe();
  }

  @Override
  protected void doBind(SocketAddress localAddress) throws Exception {
    if (!(localAddress instanceof InetSocketAddress)) {
      throw new IllegalArgumentException("localAddress must be an InetSocketAddress: " + localAddress);
    }
    this.localAddress = (InetSocketAddress) localAddress;

    String ifname = config.getInterfaceName();
    if (ifname != null && !ifname.isEmpty()) {
      if (AfXdpNativeBinding.isAvailable()) {
        try {
          this.nativeSocket = AfXdpNativeBinding.openSocket(
              ifname,
              config.getQueueId(),
              umem,
              2048
          );
        } catch (AfXdpException e) {
          if (config.isNativeModeRequired()) {
            throw e;
          }
        }
      } else if (config.isNativeModeRequired()) {
        throw new AfXdpException(
            "Native AF_XDP required but unavailable: " + AfXdpNativeBinding.getLoadError()
        );
      }
    }

    this.active = true;
  }

  @Override
  protected void doDisconnect() throws Exception {
    this.connected = false;
    this.remoteAddress = null;
  }

  @Override
  protected void doClose() throws Exception {
    open = false;
    this.active = false;
    this.connected = false;
    AfXdpNativeSocket nativeSock = this.nativeSocket;
    if (nativeSock != null) {
      this.nativeSocket = null;
      nativeSock.close();
    }
    if (ownsUmem) {
      umem.close();
    }
  }


  @Override
  protected void doBeginRead() throws Exception {
    // Read trigger; RX loop or feedRxFrame delivers directly
  }

  @Override
  protected Object filterOutboundMessage(Object msg) {
    if (msg instanceof DatagramPacket) {
      return msg;
    }
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      InetSocketAddress remote = remoteAddress;
      if (remote != null) {
        return new DatagramPacket(buf, remote);
      }
    }
    throw new UnsupportedOperationException(
        "Unsupported message type: " + (msg != null ? msg.getClass().getName() : "null"));
  }

  @Override
  protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    while (true) {
      Object msg = in.current();
      if (msg == null) {
        break;
      }
      if (!(msg instanceof DatagramPacket)) {
        in.remove(new IllegalArgumentException("Unsupported message type: " + msg.getClass()));
        continue;
      }

      DatagramPacket packet = (DatagramPacket) msg;
      ByteBuf content = packet.content();
      InetSocketAddress recipient = packet.recipient();
      if (recipient == null) {
        recipient = remoteAddress;
      }
      if (recipient == null) {
        in.remove(new IllegalStateException("No recipient specified and channel is not connected"));
        continue;
      }

      long frameOffset = umem.leaseFrame();
      if (frameOffset < 0) {
        break;
      }

      InetSocketAddress local = localAddress != null ? localAddress : new InetSocketAddress(0);
      byte[] srcMac = config.getSourceMac() != null ? config.getSourceMac() : this.sourceMac;
      byte[] dstMac = resolveDestMac(recipient.getAddress());
      MemorySegment seg = umem.getFrameSlice(frameOffset, umem.getFrameSize());
      int wireLen = AfXdpFrameParser.writeFrame(seg, srcMac, dstMac, local, recipient, content);
      AfXdpNativeSocket nativeSock = this.nativeSocket;
      if (nativeSock != null) {

        nativeSock.reapCompletions(64);
        boolean sent = nativeSock.sendTx(frameOffset, wireLen);
        if (!sent) {
          umem.recycleFrame(frameOffset);
          break;
        }
      } else {
        umem.submitTx(frameOffset, wireLen);
      }
      packetsSent.incrementAndGet();
      bytesSent.addAndGet(content.readableBytes());

      in.remove();
    }
  }


  /**
   * Ingests a raw wire packet descriptor received from the AF_XDP RX ring into Netty's pipeline.
   *
   * @param frameAddress relative offset of the frame in UMEM
   * @param frameLength total bytes in the wire frame
   * @return true if accepted and dispatched to pipeline, false if dropped or rejected
   */
  public boolean feedRxFrame(long frameAddress, int frameLength) {
    if (!isOpen() || !isActive()) {
      umem.recycleFrame(frameAddress);
      packetsDropped.incrementAndGet();
      return false;
    }

    MemorySegment slice = umem.getFrameSlice(frameAddress, frameLength);
    AfXdpFrameParser.ParsedDatagram parsed =
        AfXdpFrameParser.parse(slice, frameLength, umem, frameAddress);
    if (parsed == null) {
      umem.recycleFrame(frameAddress);
      packetsDropped.incrementAndGet();
      return false;
    }

    int boundPort = localAddress != null ? localAddress.getPort() : 0;
    if (boundPort != 0 && parsed.getDstPort() != boundPort) {
      parsed.release();
      packetsDropped.incrementAndGet();
      return false;
    }

    InetSocketAddress sender = parsed.getSender();
    InetSocketAddress recipient = parsed.getRecipient();
    if (recipient == null) {
      recipient = localAddress;
    }

    if (parsed.getSourceMac() != null && sender != null) {
      updateNeighborMac(sender.getAddress(), parsed.getSourceMac());
    }

    DatagramPacket packet = new DatagramPacket(parsed.getPayload(), recipient, sender);
    packetsReceived.incrementAndGet();
    bytesReceived.addAndGet(packet.content().readableBytes());

    EventLoop loop = eventLoop();
    if (loop != null && !loop.inEventLoop()) {
      loop.execute(() -> {
        pipeline().fireChannelRead(packet);
        pipeline().fireChannelReadComplete();
      });
    } else {
      pipeline().fireChannelRead(packet);
      pipeline().fireChannelReadComplete();
    }
    return true;
  }

  /**
   * Updates learned MAC address for a remote host IP to avoid ARP lookups on transmit.
   */
  public void updateNeighborMac(InetAddress ip, byte[] mac) {
    if (ip != null && mac != null && mac.length == 6) {
      if (neighborTable.size() >= MAX_NEIGHBOR_ENTRIES && !neighborTable.containsKey(ip)) {
        neighborTable.clear();
      }
      neighborTable.put(ip, mac.clone());
    }
  }

  private byte[] resolveDestMac(InetAddress ip) {
    byte[] learned = neighborTable.get(ip);
    if (learned != null) {
      return learned;
    }
    byte[] gateway = config.getOption(AfXdpChannelOption.GATEWAY_MAC);
    if (gateway != null && gateway.length == 6) {
      return gateway;
    }
    return defaultGatewayMac;
  }

  public AfXdpUmem getUmem() {
    return umem;
  }

  public long getPacketsReceived() {
    return packetsReceived.get();
  }

  public long getBytesReceived() {
    return bytesReceived.get();
  }

  public long getPacketsSent() {
    return packetsSent.get();
  }

  public long getBytesSent() {
    return bytesSent.get();
  }

  public long getPacketsDropped() {
    return packetsDropped.get();
  }

  /**
   * Returns the count of currently leased UMEM frames.
   */
  public int getActiveLeasedFrames() {
    return umem.getFrameCount() - umem.getFreeFrameCount();
  }

  /**
   * Returns the count of available free UMEM frames.
   */
  public int getFreeFrames() {
    return umem.getFreeFrameCount();
  }

  /**
   * Returns the active native AF_XDP socket, or null if operating in virtual/simulated mode.
   *
   * @return native socket instance or null
   */
  public AfXdpNativeSocket getNativeSocket() {
    return nativeSocket;
  }

  /**
   * Polls the native AF_XDP RX ring directly and delivers packets to Netty's pipeline.
   *
   * @param maxBatch maximum packets to poll
   * @return number of packets dispatched
   */
  public int pollNativeRx(int maxBatch) {
    AfXdpNativeSocket nativeSock = this.nativeSocket;
    if (nativeSock != null) {
      return nativeSock.pollRx(this::feedRxFrame, maxBatch);
    }
    return 0;
  }

  // --- Multicast Not Supported in AF_XDP Raw Stack ---


  @Override
  public ChannelFuture joinGroup(InetAddress multicastAddress) {
    return joinGroup(multicastAddress, newPromise());
  }

  @Override
  public ChannelFuture joinGroup(InetAddress multicastAddress, ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture joinGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface) {
    return joinGroup(multicastAddress, networkInterface, newPromise());
  }

  @Override
  public ChannelFuture joinGroup(
      InetSocketAddress multicastAddress, NetworkInterface networkInterface, ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture joinGroup(
      InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source) {
    return joinGroup(multicastAddress, networkInterface, source, newPromise());
  }

  @Override
  public ChannelFuture joinGroup(
      InetAddress multicastAddress,
      NetworkInterface networkInterface,
      InetAddress source,
      ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture leaveGroup(InetAddress multicastAddress) {
    return leaveGroup(multicastAddress, newPromise());
  }

  @Override
  public ChannelFuture leaveGroup(InetAddress multicastAddress, ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture leaveGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface) {
    return leaveGroup(multicastAddress, networkInterface, newPromise());
  }

  @Override
  public ChannelFuture leaveGroup(
      InetSocketAddress multicastAddress, NetworkInterface networkInterface, ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture leaveGroup(
      InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source) {
    return leaveGroup(multicastAddress, networkInterface, source, newPromise());
  }

  @Override
  public ChannelFuture leaveGroup(
      InetAddress multicastAddress,
      NetworkInterface networkInterface,
      InetAddress source,
      ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture block(
      InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress sourceToBlock) {
    return block(multicastAddress, networkInterface, sourceToBlock, newPromise());
  }

  @Override
  public ChannelFuture block(
      InetAddress multicastAddress,
      NetworkInterface networkInterface,
      InetAddress sourceToBlock,
      ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  @Override
  public ChannelFuture block(InetAddress multicastAddress, InetAddress sourceToBlock) {
    return block(multicastAddress, sourceToBlock, newPromise());
  }

  @Override
  public ChannelFuture block(InetAddress multicastAddress, InetAddress sourceToBlock, ChannelPromise promise) {
    return promise.setFailure(new UnsupportedOperationException("Multicast is not supported in AF_XDP transport"));
  }

  private final class AfXdpUnsafe extends AbstractUnsafe {
    @Override
    public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
      if (!ensureOpen(promise)) {
        return;
      }
      try {
        if (remoteAddress instanceof InetSocketAddress) {
          AfXdpDatagramChannel.this.remoteAddress = (InetSocketAddress) remoteAddress;
          AfXdpDatagramChannel.this.connected = true;
          safeSetSuccess(promise);
        } else {
          throw new IllegalArgumentException(
              "remoteAddress must be an InetSocketAddress: " + remoteAddress);
        }
      } catch (Throwable t) {
        safeSetFailure(promise, t);
      }
    }
  }
}
