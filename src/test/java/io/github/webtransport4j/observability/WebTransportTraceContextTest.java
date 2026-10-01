package io.github.webtransport4j.observability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for {@link WebTransportTraceContext} W3C TraceContext parsing and generation.
 */
public class WebTransportTraceContextTest {

  @Test
  public void testParseValidTraceparent() {
    String header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    WebTransportTraceContext ctx = WebTransportTraceContext.fromHeaders(header, "congo=t61rcWkgMzE");

    assertNotNull(ctx);
    assertEquals("00", ctx.getVersion());
    assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", ctx.getTraceId());
    assertEquals("00f067aa0ba902b7", ctx.getSpanId());
    assertEquals("01", ctx.getTraceFlags());
    assertTrue(ctx.isSampled());
    assertEquals("congo=t61rcWkgMzE", ctx.getTracestate());
    assertEquals(header, ctx.toTraceparent());
  }

  @Test
  public void testParseInvalidTraceparent() {
    assertNull(WebTransportTraceContext.fromHeaders(null, null));
    assertNull(WebTransportTraceContext.fromHeaders("invalid-format", null));
    assertNull(WebTransportTraceContext.fromHeaders("00-too-short-1234-01", null));
  }

  @Test
  public void testCreateNewAndChildSpan() {
    WebTransportTraceContext root = WebTransportTraceContext.createNew(true);
    assertNotNull(root);
    assertEquals("00", root.getVersion());
    assertEquals(32, root.getTraceId().length());
    assertEquals(16, root.getSpanId().length());
    assertTrue(root.isSampled());

    WebTransportTraceContext child = root.createChildSpan();
    assertNotNull(child);
    assertEquals(root.getTraceId(), child.getTraceId()); // same trace ID
    assertFalse(root.getSpanId().equals(child.getSpanId())); // new span ID
    assertEquals(16, child.getSpanId().length());
  }
}
