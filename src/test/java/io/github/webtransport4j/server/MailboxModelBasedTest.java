package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.junit.Test;

/** Seeded queue/drop/close histories compared with a bounded FIFO reference model. */
public class MailboxModelBasedTest {
  @Test
  public void compareBoundedDatagramHistoriesAndReferenceOwnership() {
    compareHistories(false);
  }

  @Test
  public void comparePriorityEvictionHistoriesAndReferenceOwnership() {
    compareHistories(true);
  }

  private void compareHistories(boolean priorityEnabled) {
    String key = "webtransport4j.datagram.mailbox.capacity";
    String original = System.getProperty(key);
    System.setProperty(key, "2");
    try {
      Random random = new Random(948173);
      for (int history = 0; history < 100; history++) {
        Queue<Runnable> workers = new ArrayDeque<>();
        ExecutorService executor = mock(ExecutorService.class);
        doAnswer(
                call -> {
                  workers.add(call.getArgument(0));
                  return null;
                })
            .when(executor)
            .execute(any(Runnable.class));
        List<Long> actual = new ArrayList<>();
        List<Long> expected = new ArrayList<>();
        Queue<Long> pending = new ArrayDeque<>();
        Queue<Long> priority = new ArrayDeque<>();
        List<WebTransportDatagramFrame> frames = new ArrayList<>();
        DatagramMailbox mailbox =
            new DatagramMailbox(
                mock(QuicChannel.class), executor, (channel, id, frame) -> actual.add(id));
        boolean closed = false;
        try {
          for (int step = 0; step < 20; step++) {
            int command = random.nextInt(4);
            if (command < 2) {
              WebTransportDatagramFrame frame =
                  new WebTransportDatagramFrame(step, Unpooled.buffer(1));
              frames.add(frame);
              boolean highPriority = priorityEnabled && command == 1;
              mailbox.enqueue(frame, highPriority);
              if (!closed) {
                if (pending.size() + priority.size() == 2 && highPriority && !pending.isEmpty()) {
                  pending.remove();
                }
                if (pending.size() + priority.size() < 2) {
                  (highPriority ? priority : pending).add((long) step);
                }
              }
            } else if (command == 2) {
              while (!workers.isEmpty()) {
                workers.remove().run();
              }
              expected.addAll(priority);
              priority.clear();
              expected.addAll(pending);
              pending.clear();
            } else {
              mailbox.drainAndRelease();
              priority.clear();
              pending.clear();
              closed = true;
            }
            assertEquals("history=" + history + ", step=" + step, expected, actual);
            int retained = 0;
            for (WebTransportDatagramFrame frame : frames) {
              retained += frame.refCnt() - 1;
            }
            assertEquals(pending.size() + priority.size(), retained);
            assertEquals(retained, mailbox.size());
            assertEquals(priority.size(), mailbox.getHighPrioritySize());
          }
        } finally {
          mailbox.drainAndRelease();
          while (!workers.isEmpty()) {
            workers.remove().run();
          }
          for (WebTransportDatagramFrame frame : frames) {
            assertEquals(1, frame.refCnt());
            frame.release();
          }
        }
      }
    } finally {
      if (original == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, original);
      }
    }
  }
}
