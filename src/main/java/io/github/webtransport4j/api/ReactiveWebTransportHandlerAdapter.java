package io.github.webtransport4j.api;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * An adapter that wraps a {@link ReactiveWebTransportHandler} to implement the standard {@link
 * WebTransportHandler} interface.
 */
public class ReactiveWebTransportHandlerAdapter implements WebTransportHandler {
  private final ReactiveWebTransportHandler delegate;
  private final Map<Long, ReactiveWebTransportSession> sessions = new ConcurrentHashMap<>();

  public ReactiveWebTransportHandlerAdapter(@NonNull ReactiveWebTransportHandler delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean onSessionRequest(@NonNull SessionRequestContext requestContext) {
    return delegate.onSessionRequest(requestContext);
  }

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    ReactiveWebTransportSession reactiveSession = new ReactiveWebTransportSession(session);
    sessions.put(session.getSessionStreamId(), reactiveSession);
    delegate
        .onSessionReady(reactiveSession)
        .subscribe(
            new Subscriber<Void>() {
              @Override
              public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
              }

              @Override
              public void onNext(Void unused) {}

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
    onSessionClosed(session, session.getCloseCode(), session.getCloseReason());
  }

  @Override
  public void onSessionClosed(
      @NonNull WebTransportSession session, int closeCode, @Nullable String reason) {
    ReactiveWebTransportSession reactiveSession = sessions.remove(session.getSessionStreamId());
    if (reactiveSession != null) {
      delegate
          .onSessionClosed(reactiveSession, closeCode, reason)
          .subscribe(
              new Subscriber<Void>() {
                @Override
                public void onSubscribe(Subscription s) {
                  s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Void unused) {}

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
  public void onError(@NonNull WebTransportSession session, @NonNull Throwable cause) {
    ReactiveWebTransportSession reactiveSession = sessions.get(session.getSessionStreamId());
    if (reactiveSession != null) {
      reactiveSession.emitError(cause);
      subscribeAndIgnore(delegate.onError(reactiveSession, cause));
    }
  }

  @Override
  public void onConnectionMigration(
      @NonNull WebTransportSession session,
      @NonNull SocketAddress oldAddress,
      @NonNull SocketAddress newAddress) {
    ReactiveWebTransportSession reactiveSession = sessions.get(session.getSessionStreamId());
    if (reactiveSession != null) {
      subscribeAndIgnore(delegate.onConnectionMigration(reactiveSession, oldAddress, newAddress));
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
      // This retained reference belongs to the receiveDatagrams() subscriber, which must close it.
      // The delegate borrows the transport reference and must retain separately for asynchronous
      // work.
      data.retain();
      reactiveSession.emitIncomingDatagram(data);
      subscribeAndIgnore(delegate.onDatagramReceived(reactiveSession, data));
    }
  }

  private void subscribeAndIgnore(Publisher<Void> publisher) {
    publisher.subscribe(
        new Subscriber<Void>() {
          @Override
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(Void unused) {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onComplete() {}
        });
  }
}
