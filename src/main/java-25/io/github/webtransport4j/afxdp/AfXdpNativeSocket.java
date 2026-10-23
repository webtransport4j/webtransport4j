package io.github.webtransport4j.afxdp;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates an active Linux AF_XDP socket bound to a network interface queue.
 *
 * <p>Provides the zero-copy hybrid data plane: ring head/tail updates and descriptor writes
 * occur directly in off-heap memory from Java without native syscall or JNI transition overhead.
 */
public final class AfXdpNativeSocket implements AutoCloseable {

  @FunctionalInterface
  public interface RxFrameConsumer {
    void accept(long frameAddress, int frameLength);
  }

  private final Arena arena;
  private final MemorySegment umemHandle;
  private final MemorySegment xskHandle;
  private final int fd;
  private final String ifname;
  private final int queueId;

  private final AfXdpRing rxRing;
  private final AfXdpRing txRing;
  private final AfXdpRing fillRing;
  private final AfXdpRing compRing;
  private final AfXdpUmem umem;

  private final AfXdpRing.RxDesc rxDescHolder = new AfXdpRing.RxDesc();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  AfXdpNativeSocket(
      Arena arena,
      MemorySegment umemHandle,
      MemorySegment xskHandle,
      int fd,
      String ifname,
      int queueId,
      AfXdpRing rxRing,
      AfXdpRing txRing,
      AfXdpRing fillRing,
      AfXdpRing compRing,
      AfXdpUmem umem) {
    this.arena = Objects.requireNonNull(arena, "arena");
    this.umemHandle = Objects.requireNonNull(umemHandle, "umemHandle");
    this.xskHandle = Objects.requireNonNull(xskHandle, "xskHandle");
    this.fd = fd;
    this.ifname = Objects.requireNonNull(ifname, "ifname");
    this.queueId = queueId;
    this.rxRing = Objects.requireNonNull(rxRing, "rxRing");
    this.txRing = Objects.requireNonNull(txRing, "txRing");
    this.fillRing = Objects.requireNonNull(fillRing, "fillRing");
    this.compRing = Objects.requireNonNull(compRing, "compRing");
    this.umem = Objects.requireNonNull(umem, "umem");

    initialFill(fillRing.getSize() / 2);
  }

  public int getFd() {
    return fd;
  }

  public String getIfname() {
    return ifname;
  }

  public int getQueueId() {
    return queueId;
  }

  /**
   * Supplies initial frame buffers to the kernel Fill Ring for RX packet reception.
   */
  public int initialFill(int count) {
    if (count <= 0) {
      return 0;
    }
    int reserved = fillRing.reserve(count);
    if (reserved < 0) {
      return 0;
    }
    for (int i = 0; i < count; i++) {
      long addr = umem.leaseFrame();
      if (addr < 0) {
        fillRing.submit(i);
        return i;
      }
      fillRing.writeFillAddr(reserved + i, addr);
    }
    fillRing.submit(count);
    return count;
  }

  /**
   * Polls the RX ring directly from memory and dispatches frames to the consumer.
   *
   * @param consumer frame handler
   * @param maxBatch maximum packets to process in this poll
   * @return number of packets processed
   */
  public int pollRx(RxFrameConsumer consumer, int maxBatch) {
    if (closed.get()) {
      return 0;
    }

    int peeked = rxRing.peek(maxBatch);
    if (peeked <= 0) {
      return 0;
    }

    int startIdx = rxRing.getLastPeekIndex();
    for (int i = 0; i < peeked; i++) {
      rxRing.readRxDesc(startIdx + i, rxDescHolder);
      consumer.accept(rxDescHolder.addr, rxDescHolder.len);
    }
    rxRing.release(peeked);

    // Replenish fill ring with new frames from UMEM pool
    int fillReserved = fillRing.reserve(peeked);
    if (fillReserved >= 0) {
      for (int i = 0; i < peeked; i++) {
        long frameAddr = umem.leaseFrame();
        if (frameAddr < 0) {
          fillRing.submit(i);
          return peeked;
        }
        fillRing.writeFillAddr(fillReserved + i, frameAddr);
      }
      fillRing.submit(peeked);
    }

    return peeked;
  }

  /**
   * Transmits a wire frame through the AF_XDP TX ring.
   *
   * @param frameAddress relative offset of the frame in UMEM
   * @param frameLength total bytes in the wire frame
   * @return true if queued, false if TX ring is full
   */
  public boolean sendTx(long frameAddress, int frameLength) {
    if (closed.get()) {
      return false;
    }

    int idx = txRing.reserve(1);
    if (idx < 0) {
      return false;
    }

    txRing.writeTxDesc(idx, frameAddress, frameLength, 0);
    txRing.submit(1);

    if (txRing.needsWakeup()) {
      AfXdpNativeBinding.kickTx(fd);
    }
    return true;
  }

  /**
   * Reclaims transmitted frame buffers from the completion ring back to the UMEM pool.
   *
   * @param maxBatch maximum entries to reclaim
   * @return number of frames reclaimed
   */
  public int reapCompletions(int maxBatch) {
    if (closed.get()) {
      return 0;
    }

    int peeked = compRing.peek(maxBatch);
    if (peeked <= 0) {
      return 0;
    }

    int startIdx = compRing.getLastPeekIndex();
    for (int i = 0; i < peeked; i++) {
      long addr = compRing.readCompAddr(startIdx + i);
      umem.recycleFrame(addr);
    }
    compRing.release(peeked);
    return peeked;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      AfXdpNativeBinding.deleteSocket(xskHandle);
      AfXdpNativeBinding.deleteUmem(umemHandle);
      arena.close();
    }
  }
}
