package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.netty.buffer.Unpooled;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Verifies that {@link ReactiveWebTransportHandlerAdapter} keys sessions by uniqueSessionId
 * rather than sessionStreamId, preventing session collision across distinct connections.
 */
public class ReactiveWebTransportHandlerAdapterTest {

  @Test
  public void testDistinctConnectionsWithSameSessionStreamIdDoNotCollide() {
    AtomicInteger session1Datagrams = new AtomicInteger();
    AtomicInteger session2Datagrams = new AtomicInteger();
    AtomicBoolean session2Closed = new AtomicBoolean();

    ReactiveWebTransportHandler delegate =
        new ReactiveWebTransportHandler() {
          @Override
          public Publisher<Void> onSessionReady(@NonNull ReactiveWebTransportSession session) {
            if (session.getUniqueSessionId() == 1001L) {
              Flux.from(session.receiveDatagrams())
                  .subscribe(
                      b -> {
                        session1Datagrams.incrementAndGet();
                        b.close();
                      });
            } else if (session.getUniqueSessionId() == 1002L) {
              Flux.from(session.receiveDatagrams())
                  .subscribe(
                      b -> {
                        session2Datagrams.incrementAndGet();
                        b.close();
                      });
            }
            return Mono.never();
          }

          @Override
          public Publisher<Void> onSessionClosed(
              @NonNull ReactiveWebTransportSession session, int closeCode, String reason) {
            if (session.getUniqueSessionId() == 1002L) {
              session2Closed.set(true);
            }
            return Mono.empty();
          }
        };

    // Both sessions have streamId == 0L (typical for first CONNECT stream on QUIC connections)
    WebTransportSession session1 = mock(WebTransportSession.class);
    when(session1.getSessionStreamId()).thenReturn(0L);
    when(session1.getUniqueSessionId()).thenReturn(1001L);

    WebTransportSession session2 = mock(WebTransportSession.class);
    when(session2.getSessionStreamId()).thenReturn(0L);
    when(session2.getUniqueSessionId()).thenReturn(1002L);

    final ReactiveWebTransportHandlerAdapter adapter =
        new ReactiveWebTransportHandlerAdapter(delegate);

    adapter.onSessionReady(session1);
    adapter.onSessionReady(session2);

    // Dispatch datagram to session 1
    DefaultNettyWebTransportBuffer buf1 =
        new DefaultNettyWebTransportBuffer(Unpooled.wrappedBuffer(new byte[] {1, 2, 3}));
    adapter.onDatagramReceived(session1, buf1);
    buf1.close();

    assertEquals(1, session1Datagrams.get());
    assertEquals(0, session2Datagrams.get());

    // Dispatch datagram to session 2
    DefaultNettyWebTransportBuffer buf2 =
        new DefaultNettyWebTransportBuffer(Unpooled.wrappedBuffer(new byte[] {4, 5, 6}));
    adapter.onDatagramReceived(session2, buf2);
    buf2.close();

    assertEquals(1, session1Datagrams.get());
    assertEquals(1, session2Datagrams.get());

    // Close session 2: session 1 must not be affected
    adapter.onSessionClosed(session2, 0, "normal");
    assertTrue(session2Closed.get());

    // Dispatch another datagram to session 1 after session 2 was closed
    DefaultNettyWebTransportBuffer buf3 =
        new DefaultNettyWebTransportBuffer(Unpooled.wrappedBuffer(new byte[] {7, 8, 9}));
    adapter.onDatagramReceived(session1, buf3);
    buf3.close();

    assertEquals(2, session1Datagrams.get());

    adapter.onSessionClosed(session1, 0, "normal");
  }
}
