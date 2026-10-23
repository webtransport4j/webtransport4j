package io.github.webtransport4j.afxdp;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the User-space Memory (UMEM) pool for zero-copy AF_XDP frame buffers.
 *
 * <p>Allocates aligned off-heap memory using Java 25 Foreign Function and Memory (FFM)
 * and coordinates circular frame recycling between the kernel/network and application.
 */
public final class AfXdpUmem implements AutoCloseable {

  public static final int DEFAULT_FRAME_SIZE = 4096;
  public static final int DEFAULT_FRAME_COUNT = 4096;

  private final int frameSize;
  private final int frameCount;
  private final Arena arena;
  private final MemorySegment memorySegment;
  private final Queue<Long> freeRing;
  private final Queue<Long> fillRing;
  private final Queue<TxDescriptor> txRing;
  private final AtomicInteger freeFrameCount;
  private volatile boolean closed;

  /**
   * Descriptor representing a packet queued for transmission on the TX ring.
   */
  public static final class TxDescriptor {
    private final long frameAddress;
    private final int frameLength;

    public TxDescriptor(long frameAddress, int frameLength) {
      this.frameAddress = frameAddress;
      this.frameLength = frameLength;
    }

    public long getFrameAddress() {
      return frameAddress;
    }

    public int getFrameLength() {
      return frameLength;
    }
  }

  /**
   * Creates a new UMEM instance with default size (16 MiB).
   */
  public AfXdpUmem() {
    this(DEFAULT_FRAME_SIZE, DEFAULT_FRAME_COUNT);
  }

  /**
   * Creates a new UMEM instance with custom frame size and count.
   *
   * @param frameSize size in bytes of each packet frame (must be power of two)
   * @param frameCount total number of frames in the pool
   */
  public AfXdpUmem(int frameSize, int frameCount) {
    if (frameSize <= 0 || (frameSize & (frameSize - 1)) != 0) {
      throw new IllegalArgumentException("Frame size must be a power of two: " + frameSize);
    }
    if (frameCount <= 0) {
      throw new IllegalArgumentException("Frame count must be positive: " + frameCount);
    }
    this.frameSize = frameSize;
    this.frameCount = frameCount;
    this.arena = Arena.ofShared();
    long totalBytes = (long) frameSize * frameCount;
    this.memorySegment = arena.allocate(totalBytes, 64);
    this.freeRing = new ConcurrentLinkedQueue<>();
    this.fillRing = new ConcurrentLinkedQueue<>();
    this.txRing = new ConcurrentLinkedQueue<>();
    this.freeFrameCount = new AtomicInteger(frameCount);

    for (int i = 0; i < frameCount; i++) {
      freeRing.offer((long) i * frameSize);
    }
  }

  /**
   * Returns the base off-heap address of this UMEM segment.
   *
   * @return native memory address
   */
  public long getBaseAddress() {
    return memorySegment.address();
  }

  /**
   * Returns the frame size.
   *
   * @return frame size in bytes
   */
  public int getFrameSize() {
    return frameSize;
  }

  /**
   * Returns total number of frames in pool.
   *
   * @return frame count
   */
  public int getFrameCount() {
    return frameCount;
  }

  /**
   * Retrieves an off-heap slice for the given frame offset.
   *
   * @param offset relative offset within UMEM
   * @param length payload length
   * @return memory segment slice
   */
  public MemorySegment getFrameSlice(long offset, long length) {
    return memorySegment.asSlice(offset, length);
  }

  /**
   * Populates the Fill Ring with free frame addresses to supply the NIC driver.
   *
   * @param count maximum frames to transfer from free pool to fill ring
   * @return number of frames submitted
   */
  public int replenishFillRing(int count) {
    int submitted = 0;
    while (submitted < count) {
      Long addr = freeRing.poll();
      if (addr == null) {
        break;
      }
      freeFrameCount.decrementAndGet();
      fillRing.offer(addr);
      submitted++;
    }
    return submitted;
  }

  /**
   * Recycles a processed frame address back to the free pool.
   *
   * @param frameAddress relative offset of the frame to release
   */
  public void recycleFrame(long frameAddress) {
    freeRing.offer(frameAddress);
    freeFrameCount.incrementAndGet();
  }

  /**
   * Leases a frame offset from the free pool.
   *
   * @return frame offset in bytes, or -1 if empty
   */
  public long leaseFrame() {
    Long addr = freeRing.poll();
    if (addr != null) {
      freeFrameCount.decrementAndGet();
      return addr;
    }
    return -1L;
  }

  /**
   * Submits a frame to the TX ring for network transmission.
   *
   * @param frameAddress relative offset of the frame to transmit
   * @param frameLength total bytes in the wire frame
   */
  public void submitTx(long frameAddress, int frameLength) {
    txRing.offer(new TxDescriptor(frameAddress, frameLength));
  }

  /**
   * Polls a pending packet descriptor from the TX ring.
   *
   * @return pending TX descriptor, or null if ring is empty
   */
  public TxDescriptor pollTx() {
    return txRing.poll();
  }

  /**
   * Completes transmission of a TX frame, recycling its address back to the free pool.
   *
   * @param frameAddress frame offset to complete
   */
  public void completeTx(long frameAddress) {
    recycleFrame(frameAddress);
  }

  /**
   * Returns the number of currently free frames.
   *
   * @return free frame count
   */
  public int getFreeFrameCount() {
    return freeFrameCount.get();
  }

  /**
   * Returns the number of frames pending in the TX ring.
   *
   * @return TX queue size
   */
  public int getTxQueueSize() {
    return txRing.size();
  }

  /**
   * Returns the number of frames pending in the Fill ring.
   *
   * @return Fill ring size
   */
  public int getFillRingSize() {
    return fillRing.size();
  }

  /**
   * Returns true if this UMEM pool has been closed.
   *
   * @return true if closed
   */
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      arena.close();
    }
  }
}
