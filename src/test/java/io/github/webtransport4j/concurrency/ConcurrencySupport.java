package io.github.webtransport4j.concurrency;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Bounded synchronization and reflection seams used only by concurrency tests. */
public final class ConcurrencySupport {
  private ConcurrencySupport() {}

  /** Waits for a controlled transition, failing rather than hanging. */
  public static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for controlled transition");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  /** Waits until the contender finishes or is blocked on a monitor held by the paused owner. */
  public static void awaitCompletionOrBlockedBy(Future<?> contender, Thread caller, Thread owner) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!contender.isDone()) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(caller.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockOwnerId() == owner.getId()) {
        return;
      }
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Contender neither completed nor reached the owner's monitor");
      }
      Thread.yield();
    }
  }

  /** Gets an implementation field without adding production test hooks. */
  public static Object field(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  /** Installs a controllable queue/executor at the existing implementation boundary. */
  public static void replace(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
