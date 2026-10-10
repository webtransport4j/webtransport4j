package io.github.webtransport4j.example.lichess.model;

import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundVersioned;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Replicates the circular version history buffer from Lichess {@code History.scala}.
 *
 * <p>Retains the latest {@code historySize} versioned events for a game room. When a client
 * reconnects with {@code ?v=X}, it replays all missed events if {@code X} is within the
 * retention window; otherwise it returns null (triggering a full state {@code resync}).
 */
public final class LilaHistory {

  private final int historySize;
  private final List<RoundVersioned> events;
  private final Object lock = new Object();

  /**
   * Creates a new history buffer with default capacity (30 events).
   */
  public LilaHistory() {
    this(30);
  }

  /**
   * Creates a history buffer with the specified capacity.
   *
   * @param historySize max number of events to retain
   */
  public LilaHistory(int historySize) {
    this.historySize = historySize;
    this.events = new ArrayList<>(historySize);
  }

  /**
   * Records a new versioned event into history.
   *
   * @param event the versioned event
   */
  public void add(@NonNull RoundVersioned event) {
    synchronized (lock) {
      events.add(event);
      if (events.size() > historySize) {
        events.remove(0);
      }
    }
  }

  /**
   * Retrieves missed events since {@code sinceVersion}.
   *
   * @param sinceVersion the last version acknowledged by the client
   * @return list of missed events in chronological order, or null if the gap exceeds retention
   */
  public @Nullable List<RoundVersioned> getFrom(long sinceVersion) {
    synchronized (lock) {
      if (events.isEmpty()) {
        return Collections.emptyList();
      }

      long latest = events.get(events.size() - 1).version();
      if (sinceVersion >= latest) {
        return Collections.emptyList();
      }

      long oldest = events.get(0).version();
      // If the client's version is older than what we have buffered, history gap is too large!
      if (sinceVersion < oldest - 1) {
        return null;
      }

      List<RoundVersioned> result = new ArrayList<>();
      for (RoundVersioned ev : events) {
        if (ev.version() > sinceVersion) {
          result.add(ev);
        }
      }
      return result;
    }
  }

  /**
   * Returns the current number of retained events.
   *
   * @return retained event count
   */
  public int size() {
    synchronized (lock) {
      return events.size();
    }
  }

  /**
   * Clears the history buffer.
   */
  public void clear() {
    synchronized (lock) {
      events.clear();
    }
  }
}
