package io.github.webtransport4j.health;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportServer;
import java.util.Map;
import org.junit.Test;

/**
 * Unit test for {@link WebTransportHealthCheck} Kubernetes liveness and readiness probes.
 */
public class WebTransportHealthCheckTest {

  @Test
  public void testLivenessAndReadinessWhenRunning() {
    WebTransportServer server = mock(WebTransportServer.class);
    when(server.isRunning()).thenReturn(true);
    when(server.isDraining()).thenReturn(false);
    when(server.getPort()).thenReturn(4433);
    when(server.getActiveSessionsCount()).thenReturn(5);

    assertTrue(WebTransportHealthCheck.isAlive(server));
    assertTrue(WebTransportHealthCheck.isReady(server));

    Map<String, Object> details = WebTransportHealthCheck.getHealthDetails(server);
    assertEquals("UP", details.get("status"));
    assertEquals(true, details.get("running"));
    assertEquals(false, details.get("draining"));
    assertEquals(4433, details.get("boundPort"));
    assertEquals(5, details.get("activeSessions"));
  }

  @Test
  public void testReadinessFailsWhenDraining() {
    WebTransportServer server = mock(WebTransportServer.class);
    when(server.isRunning()).thenReturn(true);
    when(server.isDraining()).thenReturn(true);
    when(server.getPort()).thenReturn(4433);
    when(server.getActiveSessionsCount()).thenReturn(2);

    assertTrue(WebTransportHealthCheck.isAlive(server)); // still alive
    assertFalse(WebTransportHealthCheck.isReady(server)); // not ready for new traffic

    Map<String, Object> details = WebTransportHealthCheck.getHealthDetails(server);
    assertEquals("DRAINING", details.get("status"));
    assertEquals(true, details.get("draining"));
  }

  @Test
  public void testDownWhenNotRunning() {
    WebTransportServer server = mock(WebTransportServer.class);
    when(server.isRunning()).thenReturn(false);
    when(server.isDraining()).thenReturn(false);

    assertFalse(WebTransportHealthCheck.isAlive(server));
    assertFalse(WebTransportHealthCheck.isReady(server));

    Map<String, Object> details = WebTransportHealthCheck.getHealthDetails(server);
    assertEquals("DOWN", details.get("status"));
  }
}
