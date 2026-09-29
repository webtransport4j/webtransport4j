package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Lifecycle regression tests for the independently counted wrapper. */
public class DefaultNettyWebTransportBufferLifecycleTest {

  @Test
  public void releasesOwnedDelegateOnlyAfterLastWrapperReference() {
    ByteBuf delegate = Unpooled.buffer(8);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
    try {
      buffer.retain(2);
      assertEquals(3, buffer.refCnt());
      assertEquals(1, delegate.refCnt());
      buffer.release();
      buffer.release();
      assertEquals(1, buffer.refCnt());
      assertEquals(1, delegate.refCnt());
      buffer.close();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, delegate.refCnt());
      buffer.close();
      assertEquals(0, delegate.refCnt());
    } finally {
      releaseAll(buffer);
    }
  }

  @Test
  public void rejectsResurrectionEvenWhenDelegateHasAnotherOwner() {
    ByteBuf delegate = Unpooled.buffer(8);
    delegate.retain();
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
    try {
      buffer.close();
      assertEquals(1, delegate.refCnt());
      expectIllegalState(buffer::retain);
      expectIllegalState(buffer::readableBytes);
      expectIllegalState(buffer::retainedReadableBuffer);
      expectIllegalState(buffer::delegate);
      assertEquals(0, buffer.refCnt());
      assertEquals(1, delegate.refCnt());
    } finally {
      releaseAll(buffer);
      delegate.release();
    }
  }

  @Test
  public void retainedSliceSurvivesWrapperClose() {
    ByteBuf delegate = Unpooled.buffer(8).writeLong(42L);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
    ByteBuf slice = buffer.retainedReadableBuffer();
    try {
      buffer.close();
      assertEquals(42L, slice.readLong());
    } finally {
      releaseAll(buffer);
      slice.release();
    }
    assertEquals(0, delegate.refCnt());
  }

  @Test
  public void invalidIncrementAndOverflowDoNotChangeOwnership() {
    ByteBuf delegate = Unpooled.buffer(8);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
    try {
      try {
        buffer.retain(0);
        fail("zero increment must fail");
      } catch (IllegalArgumentException expected) {
        assertEquals(1, buffer.refCnt());
      }
      expectIllegalState(() -> buffer.retain(Integer.MAX_VALUE));
      assertEquals(1, buffer.refCnt());
      assertEquals(1, delegate.refCnt());
    } finally {
      releaseAll(buffer);
    }
  }

  @Test(timeout = 15000)
  public void concurrentOwnersReleaseExactlyOneDelegateReference() throws Exception {
    ByteBuf delegate = Unpooled.buffer(8);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 4; i++) {
        buffer.retain();
        futures.add(executor.submit(() -> {
          try {
            start.await();
            for (int j = 0; j < 1000; j++) {
              buffer.retain();
              buffer.release();
            }
          } finally {
            buffer.release();
          }
          return null;
        }));
      }
      buffer.release();
      start.countDown();
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, buffer.refCnt());
      assertEquals(0, delegate.refCnt());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      releaseAll(buffer);
    }
  }

  @Test(timeout = 15000)
  public void retainRacingFinalReleaseCannotResurrect() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 100; i++) {
        ByteBuf delegate = Unpooled.buffer(8);
        DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(delegate);
        CountDownLatch start = new CountDownLatch(1);
        Future<Boolean> retain = executor.submit(() -> {
          start.await();
          try {
            buffer.retain();
            return true;
          } catch (IllegalStateException expected) {
            return false;
          }
        });
        Future<?> release = executor.submit(() -> {
          start.await();
          buffer.release();
          return null;
        });
        start.countDown();
        boolean acquired = retain.get(5, TimeUnit.SECONDS);
        release.get(5, TimeUnit.SECONDS);
        try {
          assertEquals(acquired ? 1 : 0, buffer.refCnt());
          assertEquals(acquired ? 1 : 0, delegate.refCnt());
          if (!acquired) {
            assertFalse(delegate.refCnt() > 0);
          }
        } finally {
          releaseAll(buffer);
        }
        assertEquals(0, delegate.refCnt());
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void expectIllegalState(Runnable action) {
    try {
      action.run();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // Expected.
    }
  }

  private static void releaseAll(DefaultNettyWebTransportBuffer buffer) {
    while (buffer.refCnt() > 0) {
      buffer.release();
    }
  }
}
