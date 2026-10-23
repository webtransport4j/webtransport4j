package io.github.webtransport4j.afxdp;

import io.netty.buffer.ByteBuf;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Dispatcher pipeline simulating or driving the AF_XDP Rx ring polling loop.
 *
 * <p>Consumes packet descriptors directly from UMEM, verifies ports, and executes zero-copy
 * delivery to either an {@link AfXdpDatagramChannel} or a raw consumer callback.
 */
public final class AfXdpPacketDispatcher {

  private final AfXdpUmem umem;
  private final int targetPort;
  private final Consumer<ByteBuf> legacyHandler;
  private final BiPredicate<Long, Integer> frameDispatcher;
  private final AtomicLong packetsReceived = new AtomicLong(0);
  private final AtomicLong bytesReceived = new AtomicLong(0);
  private final AtomicLong packetsDropped = new AtomicLong(0);

  /**
   * Constructs a packet dispatcher connected directly to an {@link AfXdpDatagramChannel}.
   *
   * @param channel target datagram channel
   */
  public AfXdpPacketDispatcher(AfXdpDatagramChannel channel) {
    this.umem = Objects.requireNonNull(channel, "channel").getUmem();
    this.targetPort = channel.localAddress() != null ? channel.localAddress().getPort() : 0;
    this.legacyHandler = null;
    this.frameDispatcher = channel::feedRxFrame;
  }

  /**
   * Constructs a packet dispatcher with a raw callback consumer.
   *
   * @param umem UMEM buffer pool
   * @param targetPort UDP destination port (e.g., 443 for WebTransport)
   * @param handler callback consumer for incoming datagrams
   */
  public AfXdpPacketDispatcher(AfXdpUmem umem, int targetPort, Consumer<ByteBuf> handler) {
    this.umem = Objects.requireNonNull(umem, "umem");
    this.targetPort = targetPort;
    this.legacyHandler = Objects.requireNonNull(handler, "handler");
    this.frameDispatcher = null;
  }

  /**
   * Processes a single incoming packet descriptor from the Rx ring.
   *
   * @param frameAddress relative offset of the frame in UMEM
   * @param frameLength total packet bytes received from the wire
   * @return true if datagram was successfully delivered
   */
  public boolean onRxDescriptor(long frameAddress, int frameLength) {
    if (frameDispatcher != null) {
      boolean accepted = frameDispatcher.test(frameAddress, frameLength);
      if (accepted) {
        packetsReceived.incrementAndGet();
      } else {
        packetsDropped.incrementAndGet();
      }
      return accepted;
    }

    try {
      MemorySegment slice = umem.getFrameSlice(frameAddress, frameLength);
      AfXdpFrameParser.ParsedDatagram datagram = AfXdpFrameParser.parse(slice, frameLength);

      if (datagram != null && (targetPort == 0 || datagram.getDstPort() == targetPort)) {
        ByteBuf payload = datagram.getPayload();
        int bytes = payload.readableBytes();
        packetsReceived.incrementAndGet();
        bytesReceived.addAndGet(bytes);

        legacyHandler.accept(payload);
        return true;
      } else {
        packetsDropped.incrementAndGet();
        return false;
      }
    } finally {
      umem.recycleFrame(frameAddress);
    }
  }

  public long getPacketsReceived() {
    return packetsReceived.get();
  }

  public long getBytesReceived() {
    return bytesReceived.get();
  }

  public long getPacketsDropped() {
    return packetsDropped.get();
  }
}
