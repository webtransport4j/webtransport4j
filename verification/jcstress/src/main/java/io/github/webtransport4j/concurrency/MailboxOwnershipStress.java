package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.server.DatagramMailbox;
import io.github.webtransport4j.server.WebTransportDatagramFrame;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/** Actual datagram mailbox enqueue/close race with real reference-counted buffers. */
@JCStressTest
@State
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Only caller reference remains.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Mailbox leaked or released caller ownership.")
public class MailboxOwnershipStress {
  final EmbeddedChannel channel = new EmbeddedChannel();
  final WebTransportDatagramFrame frame = new WebTransportDatagramFrame(0, Unpooled.buffer(1));
  final DatagramMailbox mailbox =
      new DatagramMailbox(channel, new DirectExecutor(), (c, id, f) -> {});

  @Actor
  public void enqueue() {
    mailbox.enqueue(frame);
  }

  @Actor
  public void close() {
    mailbox.drainAndRelease();
  }

  @Arbiter
  public void check(I_Result result) {
    mailbox.drainAndRelease();
    result.r1 = frame.refCnt();
    while (frame.refCnt() > 0) {
      frame.release();
    }
    channel.finishAndReleaseAll();
  }

  static final class DirectExecutor extends AbstractExecutorService {
    public void execute(Runnable command) {
      command.run();
    }

    public void shutdown() {}

    public List<Runnable> shutdownNow() {
      return Collections.emptyList();
    }

    public boolean isShutdown() {
      return false;
    }

    public boolean isTerminated() {
      return false;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }
}
