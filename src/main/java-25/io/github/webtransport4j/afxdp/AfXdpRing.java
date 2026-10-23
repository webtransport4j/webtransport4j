package io.github.webtransport4j.afxdp;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Off-heap ring buffer abstraction for Linux AF_XDP (XSK) producer and consumer rings.
 *
 * <p>Directly manipulates ring memory mapped by {@code libxdp} using Java 25 Foreign Function and Memory (FFM),
 * providing lock-free memory barrier operations without native transition overhead.
 */
public final class AfXdpRing {

  public static final int RING_STRUCT_SIZE = 48;
  public static final int XDP_DESC_SIZE = 16;
  public static final int ADDR_ENTRY_SIZE = 8;
  public static final int XDP_RING_NEED_WAKEUP = 1;

  private final int mask;
  private final int size;
  private final MemorySegment producerPtr;
  private final MemorySegment consumerPtr;
  private final MemorySegment ringPtr;
  private final MemorySegment flagsPtr;

  private int cachedProd;
  private int cachedCons;
  private int lastPeekIndex;

  /**
   * Container for an RX frame descriptor.
   */
  public static final class RxDesc {
    public long addr;
    public int len;
    public int options;

    public void set(long addr, int len, int options) {
      this.addr = addr;
      this.len = len;
      this.options = options;
    }
  }

  /**
   * Constructs an AF_XDP ring view over a native {@code xsk_ring_prod} or {@code xsk_ring_cons} struct.
   *
   * @param ringStruct 48-byte native memory segment of the ring structure
   * @param entrySize byte size per ring entry (16 for RX/TX descriptors, 8 for Fill/Completion addresses)
   */
  public AfXdpRing(MemorySegment ringStruct, int entrySize) {
    this.cachedProd = ringStruct.get(ValueLayout.JAVA_INT, 0);
    this.cachedCons = ringStruct.get(ValueLayout.JAVA_INT, 4);
    this.mask = ringStruct.get(ValueLayout.JAVA_INT, 8);
    this.size = ringStruct.get(ValueLayout.JAVA_INT, 12);

    MemorySegment prodAddr = ringStruct.get(ValueLayout.ADDRESS, 16);
    MemorySegment consAddr = ringStruct.get(ValueLayout.ADDRESS, 24);
    MemorySegment rAddr = ringStruct.get(ValueLayout.ADDRESS, 32);
    MemorySegment flAddr = ringStruct.get(ValueLayout.ADDRESS, 40);

    this.producerPtr = prodAddr.reinterpret(4);
    this.consumerPtr = consAddr.reinterpret(4);
    this.ringPtr = rAddr.reinterpret((long) size * entrySize);
    this.flagsPtr = (flAddr.address() != 0) ? flAddr.reinterpret(4) : null;
  }

  public int getSize() {
    return size;
  }

  public int getMask() {
    return mask;
  }

  public int getLastPeekIndex() {
    return lastPeekIndex;
  }

  /**
   * Checks how many slots are free in a producer ring.
   */
  public int prodNbFree(int nb) {
    int freeEntries = cachedCons - cachedProd;
    if (freeEntries >= nb) {
      return freeEntries;
    }
    VarHandle.acquireFence();
    cachedCons = consumerPtr.get(ValueLayout.JAVA_INT, 0) + size;
    return cachedCons - cachedProd;
  }

  /**
   * Reserves {@code nb} slots in a producer ring.
   *
   * @return starting index, or -1 if insufficient slots
   */
  public int reserve(int nb) {
    if (prodNbFree(nb) < nb) {
      return -1;
    }
    int idx = cachedProd;
    cachedProd += nb;
    return idx;
  }

  /**
   * Writes a fill ring address entry.
   */
  public void writeFillAddr(int idx, long addr) {
    long offset = ((long) (idx & mask)) * ADDR_ENTRY_SIZE;
    ringPtr.set(ValueLayout.JAVA_LONG, offset, addr);
  }

  /**
   * Writes a TX frame descriptor.
   */
  public void writeTxDesc(int idx, long addr, int len, int options) {
    long offset = ((long) (idx & mask)) * XDP_DESC_SIZE;
    ringPtr.set(ValueLayout.JAVA_LONG, offset, addr);
    ringPtr.set(ValueLayout.JAVA_INT, offset + 8, len);
    ringPtr.set(ValueLayout.JAVA_INT, offset + 12, options);
  }

  /**
   * Submits reserved entries to the kernel by updating the producer pointer.
   */
  public void submit(int nb) {
    VarHandle.releaseFence();
    int current = producerPtr.get(ValueLayout.JAVA_INT, 0);
    producerPtr.set(ValueLayout.JAVA_INT, 0, current + nb);
  }

  /**
   * Returns true if the kernel requested a wakeup syscall for this ring.
   */
  public boolean needsWakeup() {
    if (flagsPtr == null) {
      return false;
    }
    return (flagsPtr.get(ValueLayout.JAVA_INT, 0) & XDP_RING_NEED_WAKEUP) != 0;
  }

  /**
   * Checks how many entries are available to consume in a consumer ring.
   */
  public int consNbAvail(int nb) {
    int entries = cachedProd - cachedCons;
    if (entries == 0) {
      VarHandle.acquireFence();
      cachedProd = producerPtr.get(ValueLayout.JAVA_INT, 0);
      entries = cachedProd - cachedCons;
    }
    return Math.min(entries, nb);
  }

  /**
   * Peeks up to {@code nb} available entries from a consumer ring.
   *
   * @return number of entries peeked (0 if empty)
   */
  public int peek(int nb) {
    int available = consNbAvail(nb);
    if (available > 0) {
      this.lastPeekIndex = cachedCons;
      this.cachedCons += available;
      return available;
    }
    return 0;
  }

  /**
   * Reads an RX descriptor from the ring.
   */
  public void readRxDesc(int idx, RxDesc outDesc) {
    long offset = ((long) (idx & mask)) * XDP_DESC_SIZE;
    long addr = ringPtr.get(ValueLayout.JAVA_LONG, offset);
    int len = ringPtr.get(ValueLayout.JAVA_INT, offset + 8);
    int options = ringPtr.get(ValueLayout.JAVA_INT, offset + 12);
    outDesc.set(addr, len, options);
  }

  /**
   * Reads a completed TX buffer address from the completion ring.
   */
  public long readCompAddr(int idx) {
    long offset = ((long) (idx & mask)) * ADDR_ENTRY_SIZE;
    return ringPtr.get(ValueLayout.JAVA_LONG, offset);
  }

  /**
   * Releases consumed entries back to the kernel by updating the consumer pointer.
   */
  public void release(int nb) {
    VarHandle.releaseFence();
    int current = consumerPtr.get(ValueLayout.JAVA_INT, 0);
    consumerPtr.set(ValueLayout.JAVA_INT, 0, current + nb);
  }
}
