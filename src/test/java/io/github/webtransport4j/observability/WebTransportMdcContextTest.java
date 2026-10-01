package io.github.webtransport4j.observability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.slf4j.MDC;

/**
 * Unit test for {@link WebTransportMdcContext}.
 */
public class WebTransportMdcContextTest {

  @Test
  public void testMdcScopePopulatesAndRestores() {
    assertNull(MDC.get(WebTransportMdcContext.KEY_SESSION_ID));
    assertNull(MDC.get(WebTransportMdcContext.KEY_PATH));

    try (WebTransportMdcContext.Scope scope =
        WebTransportMdcContext.open(42L, "/test-stream", "127.0.0.1:5000", "quic-conn-123")) {

      assertEquals("42", MDC.get(WebTransportMdcContext.KEY_SESSION_ID));
      assertEquals("/test-stream", MDC.get(WebTransportMdcContext.KEY_PATH));
      assertEquals("127.0.0.1:5000", MDC.get(WebTransportMdcContext.KEY_REMOTE_ADDRESS));
      assertEquals("quic-conn-123", MDC.get(WebTransportMdcContext.KEY_CONNECTION_ID));

      // Nested scope
      try (WebTransportMdcContext.Scope nested =
          WebTransportMdcContext.open(99L, "/nested", "192.168.1.1:6000", "quic-conn-456")) {
        assertEquals("99", MDC.get(WebTransportMdcContext.KEY_SESSION_ID));
        assertEquals("/nested", MDC.get(WebTransportMdcContext.KEY_PATH));
      }

      // Restored to parent
      assertEquals("42", MDC.get(WebTransportMdcContext.KEY_SESSION_ID));
      assertEquals("/test-stream", MDC.get(WebTransportMdcContext.KEY_PATH));
    }

    // Outer restored to null
    assertNull(MDC.get(WebTransportMdcContext.KEY_SESSION_ID));
    assertNull(MDC.get(WebTransportMdcContext.KEY_PATH));
    assertNull(MDC.get(WebTransportMdcContext.KEY_REMOTE_ADDRESS));
    assertNull(MDC.get(WebTransportMdcContext.KEY_CONNECTION_ID));
  }
}
