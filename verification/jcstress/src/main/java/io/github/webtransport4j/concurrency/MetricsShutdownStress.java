package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.api.AsyncWebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/** Actual exporter submission/shutdown; ignored calls are distinct from rejected calls. */
@JCStressTest
@State
@Outcome(
    id = {"0, 0", "1, 0", "0, 1"},
    expect = Expect.ACCEPTABLE,
    desc = "Ignored, delivered, or rejected once.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Duplicate callback/drop accounting.")
public class MetricsShutdownStress {
  final AtomicInteger callbacks = new AtomicInteger();
  final AsyncWebTransportMetricsListener listener =
      new AsyncWebTransportMetricsListener(new Delegate(), 1);

  @Actor
  public void submit() {
    listener.onSessionOpened(1, "/test");
  }

  @Actor
  public void close() {
    listener.close();
  }

  @Arbiter
  public void check(II_Result result) {
    listener.close();
    result.r1 = callbacks.get();
    result.r2 = (int) listener.droppedEvents();
  }

  final class Delegate implements WebTransportMetricsListener {
    public void onSessionOpened(long id, String path) {
      callbacks.incrementAndGet();
    }

    public void onSessionClosed(long id, int code) {}

    public void onStreamOpened(long id, long stream, boolean bidi) {}

    public void onStreamClosed(long id, long stream) {}

    public void onDatagramSent(long id, int bytes) {}

    public void onDatagramReceived(long id, int bytes) {}

    public void onDatagramDiscarded(long id, String reason) {}

    public void onConnectionMigration(long id, String a, String b) {}
  }
}
