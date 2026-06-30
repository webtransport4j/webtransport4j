package io.github.webtransport4j.server;

import static org.junit.Assert.assertTrue;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/**
 * A test case designed to intentionally leak an object to verify that Netty's leak detection logic
 * is working correctly and catches unreleased references.
 */
public class MemoryLeakDetectionTest {

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

    // Track a dummy object
    Object dummyLeakedObject = new Object();
    ResourceLeakTracker<Object> tracker = detector.track(dummyLeakedObject);

    // Intentionally lose the reference without calling tracker.close(dummyLeakedObject)
    dummyLeakedObject = null;

    // Force Garbage Collection a few times to trigger the LeakDetector's phantom references
    for (int i = 0; i < 10; i++) {
      System.gc();
      Thread.sleep(100);

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

    assertTrue("Netty's leak detection failed to catch the memory leak!", leakDetected.get());
  }
}
