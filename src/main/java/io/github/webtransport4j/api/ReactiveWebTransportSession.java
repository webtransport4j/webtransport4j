package io.github.webtransport4j.api;

import java.security.cert.Certificate;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A standard Reactive Streams wrapper for WebTransportSession. Exposes event streams (incoming
 * streams, datagrams) as agnostic Publishers and stream creation commands as asynchronous
 * Publishers.
 */
public class ReactiveWebTransportSession {
  private final WebTransportSession session;
  private final WebTransportFlowPublisher<ReactiveWebTransportStream> incomingStreams =
      new WebTransportFlowPublisher<>();
  private final WebTransportFlowPublisher<WebTransportBuffer> incomingDatagrams =
      new WebTransportFlowPublisher<>();

  public ReactiveWebTransportSession(@NonNull WebTransportSession session) {
    this.session = session;
  }

  /**
   * Returns the underlying {@link WebTransportSession}.
   *
   * @return the raw session
   */
  public @NonNull WebTransportSession rawSession() {
    return session;
  }

  public long getSessionStreamId() {
    return session.getSessionStreamId();
  }

  public String path() {
    return session.path();
  }

  /**
   * Closes the session gracefully.
   */
  public void close() {
    session.close();
  }

  /**
   * Closes the session with an error code and diagnostic reason phrase.
   *
   * @param error the error code
   * @param reason the closure reason, or null
   */
  public void close(long error, @Nullable String reason) {
    session.close(error, reason);
  }

  /**
   * Abruptly closes the WebTransport session with the specified HTTP/3 error code.
   *
   * @param httpErrorCode the HTTP/3 error code
   */
  public void abort(long httpErrorCode) {
    session.abort(httpErrorCode);
  }

  /**
   * Returns the session close code.
   *
   * @return the close code
   */
  public int getCloseCode() {
    return session.getCloseCode();
  }

  /**
   * Returns the unsigned 32-bit session close code as a {@code long}.
   *
   * @return the unsigned 32-bit close code
   */
  public long getCloseCodeAsLong() {
    return session.getCloseCodeAsLong();
  }

  /**
   * Returns the session closure reason.
   *
   * @return the closure reason, or null
   */
  public @Nullable String getCloseReason() {
    return session.getCloseReason();
  }

  /**
   * Returns the peer certificates presented during the TLS handshake, or an empty array if no
   * client certificates were presented or verified.
   *
   * @return array of client certificates, or empty array if none
   */
  public Certificate[] getPeerCertificates() {
    return session.getPeerCertificates();
  }

  /** Returns a standard reactive Publisher of incoming streams initiated by the peer. */
  public @NonNull Publisher<ReactiveWebTransportStream> receiveStreams() {
    return incomingStreams;
  }

  /**
   * Returns a standard reactive Publisher of incoming datagrams from the peer. Each delivered
   * datagram owns a retained reference that the subscriber must close.
   */
  public @NonNull Publisher<WebTransportBuffer> receiveDatagrams() {
    return incomingDatagrams;
  }

  /** Send a datagram packet to the peer. */
  public @NonNull Publisher<Void> sendDatagram(byte[] data) {
    return new Publisher<Void>() {
      @Override
      public void subscribe(Subscriber<? super Void> subscriber) {
        subscriber.onSubscribe(
            new Subscription() {
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

  /** Create an outbound bidirectional stream as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createBiStream() {
    return createBiStreamInternal(null);
  }

  /** Create an outbound bidirectional stream with priority as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createBiStream(
      @NonNull StreamPriority priority) {
    return createBiStreamInternal(priority);
  }

  /** Create an outbound bidirectional stream with priority as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createBiStream(
      int urgency, boolean incremental) {
    return createBiStreamInternal(StreamPriority.of(urgency, incremental));
  }

  private @NonNull Publisher<ReactiveWebTransportStream> createBiStreamInternal(
      StreamPriority priority) {
    return new Publisher<ReactiveWebTransportStream>() {
      @Override
      public void subscribe(Subscriber<? super ReactiveWebTransportStream> subscriber) {
        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long n) {
                if (n <= 0) {
                  subscriber.onError(new IllegalArgumentException("Demand must be positive"));
                  return;
                }
                CompletableFuture<WebTransportStream> future =
                    priority == null ? session.createBiStream() : session.createBiStream(priority);
                future.whenComplete(
                    (stream, ex) -> {
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

  /** Create an outbound unidirectional stream as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createUniStream() {
    return createUniStreamInternal(null);
  }

  /** Create an outbound unidirectional stream with priority as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createUniStream(
      @NonNull StreamPriority priority) {
    return createUniStreamInternal(priority);
  }

  /** Create an outbound unidirectional stream with priority as a standard reactive Publisher. */
  public @NonNull Publisher<ReactiveWebTransportStream> createUniStream(
      int urgency, boolean incremental) {
    return createUniStreamInternal(StreamPriority.of(urgency, incremental));
  }

  private @NonNull Publisher<ReactiveWebTransportStream> createUniStreamInternal(
      StreamPriority priority) {
    return new Publisher<ReactiveWebTransportStream>() {
      @Override
      public void subscribe(Subscriber<? super ReactiveWebTransportStream> subscriber) {
        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long n) {
                if (n <= 0) {
                  subscriber.onError(new IllegalArgumentException("Demand must be positive"));
                  return;
                }
                CompletableFuture<WebTransportStream> future =
                    priority == null
                        ? session.createUniStream()
                        : session.createUniStream(priority);
                future.whenComplete(
                    (stream, ex) -> {
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
