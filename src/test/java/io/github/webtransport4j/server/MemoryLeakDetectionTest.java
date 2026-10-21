package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * A test case designed to intentionally leak an object to verify that Netty's leak detection logic
 * is working correctly and catches unreleased references.
 */
public class MemoryLeakDetectionTest {

  private static volatile Object leakedRef;

  @Test
  public void testIntentionalMemoryLeak() throws InterruptedException {
    AtomicBoolean leakDetected = new AtomicBoolean(false);

    // Create a custom detector just for this test to intercept the leak report
    ResourceLeakDetector<Object> detector =
        new ResourceLeakDetector<Object>(Object.class, 1) {
          @Override
          protected void reportTracedLeak(String resourceType, String records) {
            super.reportTracedLeak(resourceType, records);
            leakDetected.set(true);
          }

          @Override
          protected void reportUntracedLeak(String resourceType) {
            super.reportUntracedLeak(resourceType);
            leakDetected.set(true);
          }
        };

    // Track a dummy object via volatile static reference to prevent JIT escape analysis optimization
    leakedRef = new Object();
    ResourceLeakTracker<Object> tracker = detector.track(leakedRef);

    // Intentionally lose the reference without calling tracker.close(dummyLeakedObject)
    leakedRef = null;

    // Force Garbage Collection to trigger the LeakDetector's phantom references
    for (int i = 0; i < 50; i++) {
      System.gc();
      Thread.sleep(100);

      // Allocate garbage byte arrays to force young generation collection and phantom reference enqueuing
      byte[] garbage = new byte[512 * 1024];
      garbage[0] = (byte) i;

      // Allocate another tracked object to trigger the detector check
      Object trigger = new Object();
      ResourceLeakTracker<Object> triggerTracker = detector.track(trigger);
      if (triggerTracker != null) {
        triggerTracker.close(trigger);
      }

      if (leakDetected.get()) {
        break;
      }
    }

    assertTrue(leakDetected.get(), "Netty's leak detection failed to catch the memory leak!");
  }
}
