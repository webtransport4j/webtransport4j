package io.github.webtransport4j.api;

import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Represents stream priority according to RFC 9218 (Extensible Prioritization Scheme for HTTP).
 *
 * <p>Urgency is an integer between 0 (highest urgency) and 7 (lowest urgency), with a default of 3.
 * Incremental is a boolean flag indicating whether the stream data can be processed incrementally
 * (interleaved with other streams of the same urgency), defaulting to false.
 */
public final class StreamPriority implements Comparable<StreamPriority> {

  /** Minimum urgency level (highest priority). */
  public static final int MIN_URGENCY = 0;

  /** Maximum urgency level (lowest priority). */
  public static final int MAX_URGENCY = 7;

  /** Default urgency level per RFC 9218. */
  public static final int DEFAULT_URGENCY = 3;

  /** Default incremental scheduling behavior per RFC 9218. */
  public static final boolean DEFAULT_INCREMENTAL = false;

  /** Default priority (urgency = 3, incremental = false). */
  public static final StreamPriority DEFAULT =
      new StreamPriority(DEFAULT_URGENCY, DEFAULT_INCREMENTAL);

  /** Highest priority (urgency = 0, incremental = false). */
  public static final StreamPriority HIGHEST = new StreamPriority(MIN_URGENCY, false);

  /** Lowest priority (urgency = 7, incremental = false). */
  public static final StreamPriority LOWEST = new StreamPriority(MAX_URGENCY, false);

  private final int urgency;
  private final boolean incremental;

  /**
   * Constructs a new {@link StreamPriority}.
   *
   * @param urgency urgency level between 0 (highest) and 7 (lowest)
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @throws IllegalArgumentException if urgency is not between 0 and 7
   */
  public StreamPriority(int urgency, boolean incremental) {
    if (urgency < MIN_URGENCY || urgency > MAX_URGENCY) {
      throw new IllegalArgumentException(
          "Urgency must be between "
              + MIN_URGENCY
              + " and "
              + MAX_URGENCY
              + ", but was: "
              + urgency);
    }
    this.urgency = urgency;
    this.incremental = incremental;
  }

  /**
   * Creates a {@link StreamPriority} with the given urgency and incremental flag.
   *
   * @param urgency urgency level between 0 and 7
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a {@link StreamPriority} instance
   */
  public static @NonNull StreamPriority of(int urgency, boolean incremental) {
    if (urgency == DEFAULT_URGENCY && incremental == DEFAULT_INCREMENTAL) {
      return DEFAULT;
    }
    if (urgency == MIN_URGENCY && !incremental) {
      return HIGHEST;
    }
    if (urgency == MAX_URGENCY && !incremental) {
      return LOWEST;
    }
    return new StreamPriority(urgency, incremental);
  }

  /**
   * Creates a non-incremental {@link StreamPriority} with the specified urgency.
   *
   * @param urgency urgency level between 0 and 7
   * @return a {@link StreamPriority} instance
   */
  public static @NonNull StreamPriority urgency(int urgency) {
    return of(urgency, DEFAULT_INCREMENTAL);
  }

  /**
   * Returns the urgency level (0 to 7).
   *
   * @return urgency level
   */
  public int urgency() {
    return urgency;
  }

  /**
   * Returns the urgency level (0 to 7).
   *
   * @return urgency level
   */
  public int getUrgency() {
    return urgency;
  }

  /**
   * Creates a {@link StreamPriority} with the default urgency and the given incremental flag.
   *
   * @param incremental true if incremental/interleaved scheduling is enabled
   * @return a {@link StreamPriority} instance
   */
  public static @NonNull StreamPriority incremental(boolean incremental) {
    return of(DEFAULT_URGENCY, incremental);
  }

  /**
   * Returns whether incremental scheduling is enabled.
   *
   * @return true if incremental scheduling is enabled
   */
  public boolean incremental() {
    return incremental;
  }

  /**
   * Returns whether incremental scheduling is enabled.
   *
   * @return true if incremental scheduling is enabled
   */
  public boolean isIncremental() {
    return incremental;
  }

  /**
   * Returns a copy of this {@link StreamPriority} with the given urgency.
   *
   * @param newUrgency new urgency level between 0 and 7
   * @return a new {@link StreamPriority} instance
   */
  public @NonNull StreamPriority withUrgency(int newUrgency) {
    if (this.urgency == newUrgency) {
      return this;
    }
    return of(newUrgency, this.incremental);
  }

  /**
   * Returns a copy of this {@link StreamPriority} with the given incremental flag.
   *
   * @param newIncremental new incremental flag
   * @return a new {@link StreamPriority} instance
   */
  public @NonNull StreamPriority withIncremental(boolean newIncremental) {
    if (this.incremental == newIncremental) {
      return this;
    }
    return of(this.urgency, newIncremental);
  }

  @Override
  public int compareTo(@NonNull StreamPriority other) {
    Objects.requireNonNull(other, "other cannot be null");
    int cmp = Integer.compare(this.urgency, other.urgency);
    if (cmp != 0) {
      return cmp;
    }
    return Boolean.compare(this.incremental, other.incremental);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof StreamPriority)) {
      return false;
    }
    StreamPriority other = (StreamPriority) obj;
    return this.urgency == other.urgency && this.incremental == other.incremental;
  }

  @Override
  public int hashCode() {
    return 31 * urgency + (incremental ? 1 : 0);
  }

  @Override
  public @NonNull String toString() {
    return "StreamPriority[urgency=" + urgency + ", incremental=" + incremental + "]";
  }
}
