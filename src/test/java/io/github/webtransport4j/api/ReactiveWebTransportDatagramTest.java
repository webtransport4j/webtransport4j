package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Unit tests for reactive WebTransport datagram interactions. */
public class ReactiveWebTransportDatagramTest {

  @Test
  public void testSubscriberAndDelegateOwnSeparateReferences() {
    ReactiveWebTransportHandler delegate =
        new ReactiveWebTransportHandler() {
          @Override
          public Publisher<Void> onSessionReady(ReactiveWebTransportSession session) {
            Flux.from(session.receiveDatagrams()).subscribe(WebTransportBuffer::close);
            return Mono.never();
          }

          @Override
          public Publisher<Void> onDatagramReceived(
              ReactiveWebTransportSession session, WebTransportBuffer data) {
            // Acquire a separate reference for asynchronous processing.
            data.retain();
            return Mono.never();
          }
        };

    ReactiveWebTransportHandlerAdapter adapter = new ReactiveWebTransportHandlerAdapter(delegate);
    WebTransportSession session = mock(WebTransportSession.class);

    ByteBuf underlying = Unpooled.buffer(1).writeByte(1);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(underlying);

    try {
      adapter.onSessionReady(session);
      adapter.onDatagramReceived(session, buffer);

      // Transport and delegate own references; the subscriber has closed its reference.
      assertEquals(2, buffer.refCnt());
      assertEquals(1, underlying.refCnt());

      buffer.close(); // Transport callback returns.
      assertEquals(1, buffer.refCnt());
      assertEquals(1, buffer.readableBytes());
      assertEquals(1, underlying.refCnt());

      buffer.close(); // Delegate finishes asynchronous work.
      assertEquals(0, buffer.refCnt());
      assertEquals(0, underlying.refCnt());
    } finally {
      try {
        adapter.onSessionClosed(session);
      } finally {
        releaseRemainingReferences(buffer);
      }
    }
  }

  @Test
  public void testSessionCompletionReleasesDatagramsWithoutSubscriber() {
    assertUnsubscribedDatagramReleased(false);
  }

  @Test
  public void testSessionErrorReleasesDatagramsWithoutSubscriber() {
    assertUnsubscribedDatagramReleased(true);
  }

  private void assertUnsubscribedDatagramReleased(boolean error) {
    ReactiveWebTransportSession session =
        new ReactiveWebTransportSession(mock(WebTransportSession.class));

    ByteBuf underlying = Unpooled.buffer(1);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(underlying);

    try {
      session.emitIncomingDatagram(buffer);

      if (error) {
        session.emitError(new IllegalStateException("session failed"));
      } else {
        session.emitComplete();
      }

      assertEquals(0, buffer.refCnt());
      assertEquals(0, underlying.refCnt());
    } finally {
      releaseRemainingReferences(buffer);
    }
  }

  private static void releaseRemainingReferences(DefaultNettyWebTransportBuffer buffer) {
    while (buffer.refCnt() > 0) {
      buffer.close();
    }
  }
}
