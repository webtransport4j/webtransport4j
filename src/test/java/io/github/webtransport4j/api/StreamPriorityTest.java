package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/** Tests for {@link StreamPriority}. */
public class StreamPriorityTest {

  @Test
  public void testDefaultConstants() {
    assertEquals(0, StreamPriority.MIN_URGENCY);
    assertEquals(7, StreamPriority.MAX_URGENCY);
    assertEquals(3, StreamPriority.DEFAULT_URGENCY);
    assertFalse(StreamPriority.DEFAULT_INCREMENTAL);

    assertEquals(3, StreamPriority.DEFAULT.urgency());
    assertEquals(3, StreamPriority.DEFAULT.getUrgency());
    assertFalse(StreamPriority.DEFAULT.isIncremental());
    assertFalse(StreamPriority.DEFAULT.incremental());

    assertEquals(0, StreamPriority.HIGHEST.urgency());
    assertFalse(StreamPriority.HIGHEST.isIncremental());

    assertEquals(7, StreamPriority.LOWEST.urgency());
    assertFalse(StreamPriority.LOWEST.isIncremental());
  }

  @Test
  public void testFactoryMethodsReturnCachedConstants() {
    assertSame(StreamPriority.DEFAULT, StreamPriority.of(3, false));
    assertSame(StreamPriority.HIGHEST, StreamPriority.of(0, false));
    assertSame(StreamPriority.LOWEST, StreamPriority.of(7, false));
  }

  @Test
  public void testFactoryMethods() {
    StreamPriority p1 = StreamPriority.urgency(1);
    assertEquals(1, p1.urgency());
    assertFalse(p1.isIncremental());

    StreamPriority p2 = StreamPriority.incremental(true);
    assertEquals(3, p2.urgency());
    assertTrue(p2.isIncremental());

    StreamPriority p3 = StreamPriority.of(5, true);
    assertEquals(5, p3.urgency());
    assertTrue(p3.isIncremental());
  }

  @Test
  public void testWithUrgencyAndWithIncremental() {
    StreamPriority p = StreamPriority.DEFAULT;
    assertSame(p, p.withUrgency(3));
    assertSame(p, p.withIncremental(false));

    StreamPriority modifiedUrgency = p.withUrgency(1);
    assertEquals(1, modifiedUrgency.urgency());
    assertFalse(modifiedUrgency.isIncremental());

    StreamPriority modifiedInc = p.withIncremental(true);
    assertEquals(3, modifiedInc.urgency());
    assertTrue(modifiedInc.isIncremental());
  }

  @Test
  public void testUrgencyRangeValidation() {
    // Valid boundaries
    StreamPriority.of(0, false);
    StreamPriority.of(7, true);

    try {
      StreamPriority.of(-1, false);
      fail("Expected IllegalArgumentException for negative urgency");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("between 0 and 7"));
    }

    try {
      StreamPriority.of(8, false);
      fail("Expected IllegalArgumentException for urgency > 7");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("between 0 and 7"));
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    final StreamPriority p1 = StreamPriority.of(2, true);
    final StreamPriority p2 = StreamPriority.of(2, true);
    final StreamPriority p3 = StreamPriority.of(2, false);
    final StreamPriority p4 = StreamPriority.of(4, true);

    assertEquals(p1, p2);
    assertEquals(p1.hashCode(), p2.hashCode());
    assertNotEquals(p1, p3);
    assertNotEquals(p1, p4);
    assertNotEquals(p1, null);
    assertNotEquals(p1, "not-a-priority");
    assertEquals(p1, p1);
  }

  @Test
  public void testCompareTo() {
    final StreamPriority high = StreamPriority.of(0, false);
    final StreamPriority mid = StreamPriority.of(3, false);
    final StreamPriority midInc = StreamPriority.of(3, true);
    final StreamPriority low = StreamPriority.of(7, false);

    assertTrue(high.compareTo(mid) < 0);
    assertTrue(mid.compareTo(high) > 0);
    assertEquals(0, mid.compareTo(StreamPriority.of(3, false)));
    assertTrue(mid.compareTo(midInc) < 0);
    assertTrue(low.compareTo(mid) > 0);
  }

  @Test
  public void testToString() {
    assertEquals("StreamPriority[urgency=3, incremental=false]", StreamPriority.DEFAULT.toString());
    assertEquals("StreamPriority[urgency=1, incremental=true]", StreamPriority.of(1, true).toString());
  }
}
