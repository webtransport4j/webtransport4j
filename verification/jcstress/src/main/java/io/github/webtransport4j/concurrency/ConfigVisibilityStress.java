package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.server.WebTransportConfig;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/** Actual programmatic property publication; each harness state has an independent key. */
@JCStressTest
@State
@Outcome(
    id = {"0", "1"},
    expect = Expect.ACCEPTABLE,
    desc = "Reader saw before or after publication.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Corrupt property value.")
public class ConfigVisibilityStress {
  static final AtomicLong IDS = new AtomicLong();
  final String key = "webtransport4j.server.ratelimit.jcstress." + IDS.incrementAndGet();

  @Actor
  public void publish() {
    WebTransportConfig.setProperty(key, "1");
  }

  @Actor
  public void read(I_Result result) {
    result.r1 = Integer.parseInt(WebTransportConfig.get(key, "0"));
  }

  @Arbiter
  public void clean() {
    WebTransportConfig.removeProperty(key);
  }
}
