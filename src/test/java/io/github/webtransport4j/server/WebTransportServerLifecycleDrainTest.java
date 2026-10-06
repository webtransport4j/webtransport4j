package io.github.webtransport4j.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.health.WebTransportHealthCheck;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Integration tests for coordinated server draining and graceful shutdown. */
public class WebTransportServerLifecycleDrainTest {
  @Test(timeout = 30000)
  public void testServerDrainTransition() throws Exception {
    try (WebTransportServer server = new WebTransportServerBuilder().port(0).build()) {
      server.start();
      assertTrue(WebTransportHealthCheck.isReady(server));
      server.drain();
      assertTrue(server.isDraining());
      assertTrue(server.isRunning());
      assertFalse(server.isAcceptingSessions());
      assertFalse(WebTransportHealthCheck.isReady(server));
      server.drain(500, TimeUnit.MILLISECONDS);
      assertFalse(server.isRunning());
      assertFalse(server.isDraining());
      server.start();
      assertFalse(server.isDraining());
      assertTrue(WebTransportHealthCheck.isReady(server));
      server.drain(500, TimeUnit.MILLISECONDS);
      assertFalse(server.isRunning());
    }
  }

  @Test(timeout = 30000)
  public void timedDrainClosesSessionsWithinOneBudget() throws Exception {
    try (WebTransportServer server = new WebTransportServerBuilder().port(0).build()) {
      server.start();
      WebTransportSession session = mock(WebTransportSession.class);
      when(session.isOpen()).thenReturn(true);
      when(session.getUniqueSessionId()).thenReturn(1L);
      java.util.concurrent.atomic.AtomicBoolean notified =
          new java.util.concurrent.atomic.AtomicBoolean();
      doAnswer(invocation -> {
        if (!notified.getAndSet(true)) {
          assertTrue(server.isDraining());
        }
        assertFalse(server.isAcceptingSessions());
        assertFalse(WebTransportHealthCheck.isReady(server));
        return null;
      }).when(session).drain();
      server.registerSession(session);
      long started = System.nanoTime();
      server.drain(100, TimeUnit.MILLISECONDS);
      assertTrue(notified.get());
      assertTrue("Drain added a second session wait",
          System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
      org.mockito.Mockito.verify(session).close();
      assertTrue(server.getActiveSessions().isEmpty());
    }
  }
}
