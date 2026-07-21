package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A standard Reactive Streams wrapper for WebTransportSession.
 * Exposes event streams (incoming streams, datagrams) as agnostic Publishers
 * and stream creation commands as asynchronous Publishers.
 */
public class ReactiveWebTransportSession {
  private final WebTransportSession session;
  private final WebTransportFlowPublisher<ReactiveWebTransportStream> incomingStreams = new WebTransportFlowPublisher<>();
  private final WebTransportFlowPublisher<WebTransportBuffer> incomingDatagrams = new WebTransportFlowPublisher<>();

  public ReactiveWebTransportSession(@NonNull WebTransportSession session) {
    this.session = session;
  }

  public long getSessionStreamId() {
    return session.getSessionStreamId();
  }

  public String path() {
    return session.path();
  }

  /**
   * Returns a standard reactive Publisher of incoming streams initiated by the peer.
   */
  public @NonNull Publisher<ReactiveWebTransportStream> receiveStreams() {
    return incomingStreams;
  }

  /**
   * Returns a standard reactive Publisher of incoming datagrams from the peer.
   */
  public @NonNull Publisher<WebTransportBuffer> receiveDatagrams() {
    return incomingDatagrams;
  }

  /**
   * Send a datagram packet to the peer.
   */
  public @NonNull Publisher<Void> sendDatagram(byte[] data) {
    return new Publisher<Void>() {
      @Override
      public void subscribe(Subscriber<? super Void> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            if (n <= 0) {
              subscriber.onError(new IllegalArgumentException("Demand must be positive"));
              return;
            }
            try {
              session.sendDatagram(data);
              subscriber.onComplete();
            } catch (Throwable t) {
              subscriber.onError(t);
            }
          }

          @Override
          public void cancel() {}
        });
      }
    };
  }

  /**
   * Create an outbound bidirectional stream as a standard reactive Publisher.
   */
  public @NonNull Publisher<ReactiveWebTransportStream> createBiStream() {
    return new Publisher<ReactiveWebTransportStream>() {
      @Override
      public void subscribe(Subscriber<? super ReactiveWebTransportStream> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            if (n <= 0) {
              subscriber.onError(new IllegalArgumentException("Demand must be positive"));
              return;
            }
            session.createBiStream().whenComplete((stream, ex) -> {
              if (ex == null) {
                subscriber.onNext(new ReactiveWebTransportStream(stream));
                subscriber.onComplete();
              } else {
                subscriber.onError(ex);
              }
            });
          }

          @Override
          public void cancel() {}
        });
      }
    };
  }

  /**
   * Create an outbound unidirectional stream as a standard reactive Publisher.
   */
  public @NonNull Publisher<ReactiveWebTransportStream> createUniStream() {
    return new Publisher<ReactiveWebTransportStream>() {
      @Override
      public void subscribe(Subscriber<? super ReactiveWebTransportStream> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            if (n <= 0) {
              subscriber.onError(new IllegalArgumentException("Demand must be positive"));
              return;
            }
            session.createUniStream().whenComplete((stream, ex) -> {
              if (ex == null) {
                subscriber.onNext(new ReactiveWebTransportStream(stream));
                subscriber.onComplete();
              } else {
                subscriber.onError(ex);
              }
            });
          }

          @Override
          public void cancel() {}
        });
      }
    };
  }

  // Package-private helpers to route events from WebTransportHandler
  void emitIncomingStream(ReactiveWebTransportStream stream) {
    incomingStreams.emitNext(stream);
  }

  void emitIncomingDatagram(WebTransportBuffer data) {
    incomingDatagrams.emitNext(data);
  }

  void emitComplete() {
    incomingStreams.emitComplete();
    incomingDatagrams.emitComplete();
  }

  void emitError(Throwable t) {
    incomingStreams.emitError(t);
    incomingDatagrams.emitError(t);
  }
}
