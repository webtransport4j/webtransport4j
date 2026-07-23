package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An adapter that wraps a {@link ReactiveWebTransportHandler} to implement
 * the standard {@link WebTransportHandler} interface.
 */
public class ReactiveWebTransportHandlerAdapter implements WebTransportHandler {
  private final ReactiveWebTransportHandler delegate;
  private final Map<Long, ReactiveWebTransportSession> sessions = new ConcurrentHashMap<>();

  public ReactiveWebTransportHandlerAdapter(@NonNull ReactiveWebTransportHandler delegate) {
    this.delegate = delegate;
  }

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    ReactiveWebTransportSession reactiveSession = new ReactiveWebTransportSession(session);
    sessions.put(session.getSessionStreamId(), reactiveSession);
    delegate.onSessionReady(reactiveSession).subscribe(new Subscriber<Void>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Void aVoid) {}

      @Override
      public void onError(Throwable t) {
        reactiveSession.emitError(t);
      }

      @Override
      public void onComplete() {
        reactiveSession.emitComplete();
      }
    });
  }

  @Override
  public void onSessionClosed(@NonNull WebTransportSession session) {
    ReactiveWebTransportSession reactiveSession = sessions.remove(session.getSessionStreamId());
    if (reactiveSession != null) {
      delegate.onSessionClosed(reactiveSession).subscribe(new Subscriber<Void>() {
        @Override
        public void onSubscribe(Subscription s) {
          s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void aVoid) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onComplete() {
          reactiveSession.emitComplete();
        }
      });
    }
  }

  @Override
  public void onIncomingStream(
      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    ReactiveWebTransportSession reactiveSession = sessions.get(session.getSessionStreamId());
    if (reactiveSession != null) {
      ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(stream);
      reactiveSession.emitIncomingStream(reactiveStream);
      subscribeAndIgnore(delegate.onIncomingStream(reactiveSession, reactiveStream));
    }
  }

  @Override
  public void onDatagramReceived(
      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
    ReactiveWebTransportSession reactiveSession = sessions.get(session.getSessionStreamId());
    if (reactiveSession != null) {
      reactiveSession.emitIncomingDatagram(data);
      subscribeAndIgnore(delegate.onDatagramReceived(reactiveSession, data));
    }
  }

  private void subscribeAndIgnore(Publisher<Void> publisher) {
    publisher.subscribe(new Subscriber<Void>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Void aVoid) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    });
  }
}
