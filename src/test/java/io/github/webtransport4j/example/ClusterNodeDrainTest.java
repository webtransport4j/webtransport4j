package io.github.webtransport4j.example;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.WebTransportServer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

/** Regression coverage for overlapping preStop and JVM shutdown drains. */
public class ClusterNodeDrainTest {
  @Test(timeout = 30000)
  public void bothCallersWaitUntilSessionsFinishDraining() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try (WebTransportServer server = WebTransportServer.builder().port(0).build()) {
      server.start();
      WebTransportSession session = mock(WebTransportSession.class);
      when(session.isOpen()).thenReturn(true);
      when(session.getUniqueSessionId()).thenReturn(123L);
      CountDownLatch notified = new CountDownLatch(1);
      doAnswer(invocation -> {
        notified.countDown();
        return null;
      }).when(session).drain();
      server.registerSession(session);
      Runnable drain = ClusterNodeSample.createDrainTask(server);
      Future<?> preStop = workers.submit(drain);
      Future<?> shutdown = null;
      try {
        assertTrue(notified.await(5, TimeUnit.SECONDS));
        CountDownLatch shutdownStarted = new CountDownLatch(1);
        shutdown = workers.submit(() -> {
          shutdownStarted.countDown();
          drain.run();
        });
        assertTrue(shutdownStarted.await(5, TimeUnit.SECONDS));
        Future<?> concurrentShutdown = shutdown;
        assertThrows(TimeoutException.class, () -> preStop.get(100, TimeUnit.MILLISECONDS));
        assertThrows(TimeoutException.class,
            () -> concurrentShutdown.get(100, TimeUnit.MILLISECONDS));
        assertFalse(server.isAcceptingSessions());
        verify(session, never()).close();
      } finally {
        server.unregisterSession(session);
        preStop.get(5, TimeUnit.SECONDS);
        if (shutdown != null) {
          shutdown.get(5, TimeUnit.SECONDS);
        }
      }
      assertFalse(server.isRunning());
      assertTrue(server.getActiveSessions().isEmpty());
    } finally {
      workers.shutdownNow();
    }
  }
}
