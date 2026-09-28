package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveWebTransportDatagramTest {
  @Test
  public void testSubscriberAndDelegateOwnSeparateReferences() {
    ReactiveWebTransportHandler delegate = new ReactiveWebTransportHandler() {
      @Override
      public Publisher<Void> onSessionReady(ReactiveWebTransportSession session) {
        Flux.from(session.receiveDatagrams()).subscribe(WebTransportBuffer::close);
        return Mono.never();
      }

      @Override
      public Publisher<Void> onDatagramReceived(ReactiveWebTransportSession session, WebTransportBuffer data) {
        data.retain(); // The delegate keeps its own reference for later asynchronous processing.
        return Mono.never();
      }
    };
    ReactiveWebTransportHandlerAdapter adapter = new ReactiveWebTransportHandlerAdapter(delegate);
    WebTransportSession session = mock(WebTransportSession.class);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(Unpooled.buffer(1).writeByte(1));
    try {
      adapter.onSessionReady(session);
      adapter.onDatagramReceived(session, buffer);
      assertEquals(2, buffer.refCnt()); // Transport and delegate; subscriber has already closed its reference.
      buffer.close(); // Transport callback returns.
      assertEquals(1, buffer.refCnt());
      assertEquals(1, buffer.readableBytes());
      buffer.close(); // Delegate finishes asynchronous work.
      assertEquals(0, buffer.delegate().refCnt());
    } finally {
      adapter.onSessionClosed(session);
      while (buffer.refCnt() > 0) {
        buffer.close();
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
    ReactiveWebTransportSession session = new ReactiveWebTransportSession(mock(WebTransportSession.class));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(Unpooled.buffer(1));
    try {
      session.emitIncomingDatagram(buffer);
      if (error) {
        session.emitError(new IllegalStateException("session failed"));
      } else {
        session.emitComplete();
      }
      assertEquals(0, buffer.refCnt());
      assertEquals(0, buffer.delegate().refCnt());
    } finally {
      while (buffer.refCnt() > 0) {
        buffer.close();
      }
    }
  }
}
