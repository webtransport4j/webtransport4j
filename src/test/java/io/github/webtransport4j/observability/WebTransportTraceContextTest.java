package io.github.webtransport4j.observability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Unit test for {@link WebTransportTraceContext} W3C TraceContext parsing and generation. */
public class WebTransportTraceContextTest {

  @Test
  public void testParseValidTraceparent() {
    String header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    WebTransportTraceContext ctx =
        WebTransportTraceContext.fromHeaders(header, "congo=t61rcWkgMzE");

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

    // Version "ff" is forbidden by W3C spec
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", null));

    // All-zero trace-id is forbidden
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01", null));

    // All-zero parent-id (span-id) is forbidden
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", null));

    // Non-hex character in trace-id
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01", null));

    // Non-hex character in parent-id
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bz-01", null));

    // Non-hex or invalid length flags
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0", null));
    assertNull(
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g", null));
  }

  @Test
  public void testSampledBitHandling() {
    // Flag "09" (0b00001001) has sampled bit (bit 0) set
    WebTransportTraceContext ctx09 =
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-09", null);
    assertNotNull(ctx09);
    assertTrue(ctx09.isSampled());

    // Flag "08" (0b00001000) has sampled bit (bit 0) cleared
    WebTransportTraceContext ctx08 =
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-08", null);
    assertNotNull(ctx08);
    assertFalse(ctx08.isSampled());

    // Flag "01" has sampled bit set
    WebTransportTraceContext ctx01 =
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", null);
    assertNotNull(ctx01);
    assertTrue(ctx01.isSampled());

    // Flag "00" has sampled bit cleared
    WebTransportTraceContext ctx00 =
        WebTransportTraceContext.fromHeaders(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", null);
    assertNotNull(ctx00);
    assertFalse(ctx00.isSampled());
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
    assertTrue(root.toString().contains(root.getTraceId()));
  }
}
