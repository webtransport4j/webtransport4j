package io.github.webtransport4j.server;

import static io.github.webtransport4j.concurrency.ConcurrencySupport.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Deterministic close/publication and close/in-flight buffer ownership races. */
public class MailboxConcurrencyTest {
  @Test(timeout = 15000)
  public void datagramPublicationAfterCloseReleasesMailboxReference() throws Exception {
    CountDownLatch retained = new CountDownLatch(1);
    CountDownLatch publish = new CountDownLatch(1);
    WebTransportDatagramFrame frame =
        new WebTransportDatagramFrame(0, Unpooled.buffer(1)) {
          public WebTransportDatagramFrame retain() {
            super.retain();
            retained.countDown();
            await(publish);
            return this;
          }
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    DatagramMailbox mailbox =
        new DatagramMailbox(mock(QuicChannel.class), executor, (c, id, f) -> {});
    ExecutorService producer = Executors.newSingleThreadExecutor();
    try {
      final Future<?> enqueue = producer.submit(() -> mailbox.enqueue(frame));
      await(retained);
      mailbox.drainAndRelease();
      publish.countDown();
      enqueue.get(5, TimeUnit.SECONDS);
      assertEquals(1, frame.refCnt());
      mailbox.drainAndRelease();
      assertEquals(1, frame.refCnt());
    } finally {
      publish.countDown();
      producer.shutdownNow();
      executor.shutdownNow();
      mailbox.drainAndRelease();
      frame.release();
    }
  }

  @Test(timeout = 15000)
  public void streamClosePreservesInFlightReferenceUntilCallbackReturns() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    StreamMailbox mailbox =
        new StreamMailbox(
            mock(QuicStreamChannel.class),
            executor,
            (c, id, frame) -> {
              entered.countDown();
              await(release);
            },
            0);
    WebTransportStreamFrame first = new WebTransportStreamFrame(0, 4, true, Unpooled.buffer(1));
    WebTransportStreamFrame second = new WebTransportStreamFrame(0, 8, true, Unpooled.buffer(1));
    try {
      mailbox.enqueue(first);
      await(entered);
      mailbox.enqueue(second);
      mailbox.drainAndRelease();
      assertEquals(2, first.refCnt());
      assertEquals(1, second.refCnt());
      release.countDown();
      executor.shutdown();
      org.junit.Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(1, first.refCnt());
      mailbox.drainAndRelease();
      assertEquals(1, second.refCnt());
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      mailbox.drainAndRelease();
      first.release();
      second.release();
    }
  }
}
