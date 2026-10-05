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

/** Actual terminal compare-and-set ownership with competing complete/error actors. */
@JCStressTest
@State
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "One terminal signal.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Missing or duplicate terminal signal.")
public class PublisherTerminalStress {
  final AtomicInteger terminals = new AtomicInteger();
  final WebTransportFlowPublisher<Integer> publisher = new WebTransportFlowPublisher<>();

  public PublisherTerminalStress() {
    publisher.subscribe(
        new Subscriber<Integer>() {
          public void onSubscribe(Subscription s) {
            s.request(1);
          }

          public void onNext(Integer item) {}

          public void onError(Throwable error) {
            terminals.incrementAndGet();
          }

          public void onComplete() {
            terminals.incrementAndGet();
          }
        });
  }

  @Actor
  public void complete() {
    publisher.emitComplete();
  }

  @Actor
  public void error() {
    publisher.emitError(new Exception("race"));
  }

  @Arbiter
  public void check(I_Result result) {
    result.r1 = terminals.get();
  }
}
