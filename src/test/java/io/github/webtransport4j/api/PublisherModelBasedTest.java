package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Exhaustive bounded command histories checked against an independent subscription model. */
public class PublisherModelBasedTest {
  @Test
  public void exhaustivelyCompareFiveCommandHistories() {
    for (int history = 0; history < 1024; history++) {
      WebTransportFlowPublisher<Integer> actual = new WebTransportFlowPublisher<>();
      Model expected = new Model();
      List<Integer> received = new ArrayList<>();
      int[] terminals = {0};
      Subscription[] subscription = {null};
      actual.subscribe(
          new Subscriber<Integer>() {
            public void onSubscribe(Subscription s) {
              subscription[0] = s;
            }

            public void onNext(Integer item) {
              received.add(item);
            }

            public void onError(Throwable error) {
              throw new AssertionError(error);
            }

            public void onComplete() {
              terminals[0]++;
            }
          });
      int commands = history;
      for (int step = 0; step < 5; step++) {
        int command = commands % 4;
        commands /= 4;
        if (command == 0) {
          actual.emitNext(step);
          expected.emit(step);
        }
        if (command == 1) {
          subscription[0].request(1);
          expected.request();
        }
        if (command == 2) {
          actual.emitComplete();
          expected.complete();
        }
        if (command == 3) {
          subscription[0].cancel();
          expected.cancel();
        }
        String trace = "history=" + history + ", step=" + step;
        assertEquals(trace, expected.delivered, received);
        assertEquals(trace, expected.terminal ? 1 : 0, terminals[0]);
      }
      subscription[0].cancel();
    }
  }

  private static final class Model {
    final Queue<Integer> queued = new ArrayDeque<>();
    final List<Integer> delivered = new ArrayList<>();
    int credit;
    boolean complete;
    boolean cancelled;
    boolean terminal;

    void emit(int item) {
      if (!complete && !cancelled) {
        queued.add(item);
        drain();
      }
    }

    void request() {
      credit++;
      drain();
    }

    void complete() {
      complete = true;
      drain();
    }

    void cancel() {
      cancelled = true;
      queued.clear();
    }

    void drain() {
      if (cancelled || terminal) {
        return;
      }
      while (credit > 0 && !queued.isEmpty()) {
        delivered.add(queued.remove());
        credit--;
      }
      if (complete && queued.isEmpty()) {
        terminal = true;
      }
    }
  }
}
