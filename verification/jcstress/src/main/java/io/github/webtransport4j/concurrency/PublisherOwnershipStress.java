package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.api.WebTransportFlowPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;
import org.reactivestreams.*;

/** Actual publisher emit/complete race; an item must be delivered or disposed. */
@JCStressTest
@State
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Exactly one item owner resolved.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Lost or duplicate close/delivery.")
public class PublisherOwnershipStress {
  final AtomicInteger resolved = new AtomicInteger();
  final WebTransportFlowPublisher<Item> publisher = new WebTransportFlowPublisher<>();

  public PublisherOwnershipStress() {
    publisher.subscribe(
        new Subscriber<Item>() {
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          public void onNext(Item item) {
            item.close();
          }

          public void onError(Throwable error) {}

          public void onComplete() {}
        });
  }

  @Actor
  public void emit() {
    publisher.emitNext(new Item());
  }

  @Actor
  public void complete() {
    publisher.emitComplete();
  }

  @Arbiter
  public void check(I_Result result) {
    result.r1 = resolved.get();
  }

  final class Item implements AutoCloseable {
    public void close() {
      resolved.incrementAndGet();
    }
  }
}
