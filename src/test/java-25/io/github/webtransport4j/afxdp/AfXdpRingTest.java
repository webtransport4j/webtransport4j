package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AfXdpRing} verifying lock-free off-heap producer/consumer operations.
 */
public class AfXdpRingTest {

  @Test
  public void testProducerRingTxDescriptorOperations() {
    try (Arena arena = Arena.ofConfined()) {
      final int ringSize = 16;
      final MemorySegment ringStruct = arena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      final MemorySegment producer = arena.allocate(4, 4);
      final MemorySegment consumer = arena.allocate(4, 4);
      final MemorySegment descs = arena.allocate((long) ringSize * AfXdpRing.XDP_DESC_SIZE, 8);
      final MemorySegment flags = arena.allocate(4, 4);

      producer.set(ValueLayout.JAVA_INT, 0, 0);
      consumer.set(ValueLayout.JAVA_INT, 0, 0);
      flags.set(ValueLayout.JAVA_INT, 0, AfXdpRing.XDP_RING_NEED_WAKEUP);

      ringStruct.set(ValueLayout.JAVA_INT, 0, 0); // cached_prod
      ringStruct.set(ValueLayout.JAVA_INT, 4, 0); // cached_cons
      ringStruct.set(ValueLayout.JAVA_INT, 8, ringSize - 1); // mask
      ringStruct.set(ValueLayout.JAVA_INT, 12, ringSize); // size
      ringStruct.set(ValueLayout.ADDRESS, 16, producer);
      ringStruct.set(ValueLayout.ADDRESS, 24, consumer);
      ringStruct.set(ValueLayout.ADDRESS, 32, descs);
      ringStruct.set(ValueLayout.ADDRESS, 40, flags);

      AfXdpRing ring = new AfXdpRing(ringStruct, AfXdpRing.XDP_DESC_SIZE);
      assertEquals(ringSize, ring.getSize());
      assertEquals(ringSize - 1, ring.getMask());
      assertTrue(ring.needsWakeup());

      int reservedIdx = ring.reserve(2);
      assertEquals(0, reservedIdx);

      ring.writeTxDesc(0, 4096L, 128, 0);
      ring.writeTxDesc(1, 8192L, 256, 0);
      ring.submit(2);

      assertEquals(2, producer.get(ValueLayout.JAVA_INT, 0));
    }
  }

  @Test
  public void testConsumerRingRxDescriptorOperations() {
    try (Arena arena = Arena.ofConfined()) {
      final int ringSize = 16;
      final MemorySegment ringStruct = arena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      final MemorySegment producer = arena.allocate(4, 4);
      final MemorySegment consumer = arena.allocate(4, 4);
      final MemorySegment descs = arena.allocate((long) ringSize * AfXdpRing.XDP_DESC_SIZE, 8);

      producer.set(ValueLayout.JAVA_INT, 0, 2);
      consumer.set(ValueLayout.JAVA_INT, 0, 0);

      // Pre-fill descriptor 0 and 1
      descs.set(ValueLayout.JAVA_LONG, 0, 1024L);
      descs.set(ValueLayout.JAVA_INT, 8, 64);
      descs.set(ValueLayout.JAVA_INT, 12, 0);

      descs.set(ValueLayout.JAVA_LONG, 16, 2048L);
      descs.set(ValueLayout.JAVA_INT, 24, 128);
      descs.set(ValueLayout.JAVA_INT, 28, 0);

      ringStruct.set(ValueLayout.JAVA_INT, 0, 0); // cached_prod
      ringStruct.set(ValueLayout.JAVA_INT, 4, 0); // cached_cons
      ringStruct.set(ValueLayout.JAVA_INT, 8, ringSize - 1); // mask
      ringStruct.set(ValueLayout.JAVA_INT, 12, ringSize); // size
      ringStruct.set(ValueLayout.ADDRESS, 16, producer);
      ringStruct.set(ValueLayout.ADDRESS, 24, consumer);
      ringStruct.set(ValueLayout.ADDRESS, 32, descs);
      ringStruct.set(ValueLayout.ADDRESS, 40, MemorySegment.NULL);

      AfXdpRing ring = new AfXdpRing(ringStruct, AfXdpRing.XDP_DESC_SIZE);
      assertFalse(ring.needsWakeup());

      int peeked = ring.peek(4);
      assertEquals(2, peeked);
      assertEquals(0, ring.getLastPeekIndex());

      AfXdpRing.RxDesc descHolder = new AfXdpRing.RxDesc();
      ring.readRxDesc(0, descHolder);
      assertEquals(1024L, descHolder.addr);
      assertEquals(64, descHolder.len);

      ring.readRxDesc(1, descHolder);
      assertEquals(2048L, descHolder.addr);
      assertEquals(128, descHolder.len);

      ring.release(peeked);
      assertEquals(2, consumer.get(ValueLayout.JAVA_INT, 0));
    }
  }

  @Test
  public void testFillAndCompletionAddressRings() {
    try (Arena arena = Arena.ofConfined()) {
      final int ringSize = 8;

      // Fill Ring (Producer ring for supplying free frames to kernel)
      final MemorySegment fillRingStruct = arena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      final MemorySegment fillProducer = arena.allocate(4, 4);
      final MemorySegment fillConsumer = arena.allocate(4, 4);
      final MemorySegment fillAddrs = arena.allocate((long) ringSize * AfXdpRing.ADDR_ENTRY_SIZE, 8);

      fillProducer.set(ValueLayout.JAVA_INT, 0, 0);
      fillConsumer.set(ValueLayout.JAVA_INT, 0, 0);

      fillRingStruct.set(ValueLayout.JAVA_INT, 0, 0);
      fillRingStruct.set(ValueLayout.JAVA_INT, 4, 0);
      fillRingStruct.set(ValueLayout.JAVA_INT, 8, ringSize - 1);
      fillRingStruct.set(ValueLayout.JAVA_INT, 12, ringSize);
      fillRingStruct.set(ValueLayout.ADDRESS, 16, fillProducer);
      fillRingStruct.set(ValueLayout.ADDRESS, 24, fillConsumer);
      fillRingStruct.set(ValueLayout.ADDRESS, 32, fillAddrs);
      fillRingStruct.set(ValueLayout.ADDRESS, 40, MemorySegment.NULL);

      AfXdpRing fillRing = new AfXdpRing(fillRingStruct, AfXdpRing.ADDR_ENTRY_SIZE);
      int reserved = fillRing.reserve(1);
      assertEquals(0, reserved);
      fillRing.writeFillAddr(0, 16384L);
      fillRing.submit(1);
      assertEquals(1, fillProducer.get(ValueLayout.JAVA_INT, 0));

      // Completion Ring (Consumer ring populated by kernel with transmitted frames)
      final MemorySegment compRingStruct = arena.allocate(AfXdpRing.RING_STRUCT_SIZE, 8);
      final MemorySegment compProducer = arena.allocate(4, 4);
      final MemorySegment compConsumer = arena.allocate(4, 4);
      final MemorySegment compAddrs = arena.allocate((long) ringSize * AfXdpRing.ADDR_ENTRY_SIZE, 8);

      compProducer.set(ValueLayout.JAVA_INT, 0, 1);
      compConsumer.set(ValueLayout.JAVA_INT, 0, 0);
      compAddrs.set(ValueLayout.JAVA_LONG, 0, 16384L);

      compRingStruct.set(ValueLayout.JAVA_INT, 0, 0);
      compRingStruct.set(ValueLayout.JAVA_INT, 4, 0);
      compRingStruct.set(ValueLayout.JAVA_INT, 8, ringSize - 1);
      compRingStruct.set(ValueLayout.JAVA_INT, 12, ringSize);
      compRingStruct.set(ValueLayout.ADDRESS, 16, compProducer);
      compRingStruct.set(ValueLayout.ADDRESS, 24, compConsumer);
      compRingStruct.set(ValueLayout.ADDRESS, 32, compAddrs);
      compRingStruct.set(ValueLayout.ADDRESS, 40, MemorySegment.NULL);

      AfXdpRing compRing = new AfXdpRing(compRingStruct, AfXdpRing.ADDR_ENTRY_SIZE);
      int peeked = compRing.peek(1);
      assertEquals(1, peeked);
      long readAddr = compRing.readCompAddr(0);
      assertEquals(16384L, readAddr);
      compRing.release(1);
      assertEquals(1, compConsumer.get(ValueLayout.JAVA_INT, 0));
    }
  }
}
