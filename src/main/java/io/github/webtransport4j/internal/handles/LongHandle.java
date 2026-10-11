package io.github.webtransport4j.internal.handles;

/**
 * Handle abstraction for atomic long field updates across Java versions.
 *
 * @param <T> the type of the object holding the field
 */
public abstract class LongHandle<T> {

  /**
   * Reads the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @return current value
   */
  public abstract long get(T obj);

  /**
   * Sets the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @param val new value
   */
  public abstract void set(T obj, long val);

  /**
   * Atomically sets the field to the update value if the current value equals the expected value.
   *
   * @param obj the target instance
   * @param expect expected value
   * @param update new value
   * @return true if successful
   */
  public abstract boolean compareAndSet(T obj, long expect, long update);

  /**
   * Atomically increments the field by 1 and returns the updated value.
   *
   * @param obj the target instance
   * @return updated value
   */
  public abstract long incrementAndGet(T obj);

  /**
   * Atomically decrements the field by 1 and returns the updated value.
   *
   * @param obj the target instance
   * @return updated value
   */
  public abstract long decrementAndGet(T obj);

  /**
   * Atomically adds delta to the field and returns the updated value.
   *
   * @param obj the target instance
   * @param delta amount to add
   * @return updated value
   */
  public abstract long addAndGet(T obj, long delta);

  /**
   * Atomically increments the field by 1 and returns the previous value.
   *
   * @param obj the target instance
   * @return previous value
   */
  public abstract long getAndIncrement(T obj);

  /**
   * Atomically decrements the field by 1 and returns the previous value.
   *
   * @param obj the target instance
   * @return previous value
   */
  public abstract long getAndDecrement(T obj);

  /**
   * Atomically adds delta to the field and returns the previous value.
   *
   * @param obj the target instance
   * @param delta amount to add
   * @return previous value
   */
  public abstract long getAndAdd(T obj, long delta);

  /**
   * Atomically sets the field to val and returns the previous value.
   *
   * @param obj the target instance
   * @param val new value
   * @return previous value
   */
  public abstract long getAndSet(T obj, long val);

  /**
   * Atomically updates the field using the given function and returns the updated value.
   *
   * @param obj the target instance
   * @param updateFunction function to compute new value
   * @return updated value
   */
  public long updateAndGet(T obj, java.util.function.LongUnaryOperator updateFunction) {
    long prev = get(obj);
    for (;;) {
      long next = updateFunction.applyAsLong(prev);
      if (compareAndSet(obj, prev, next)) {
        return next;
      }
      prev = get(obj);
    }
  }

  /**
   * Atomically updates the field using the given function and returns the previous value.
   *
   * @param obj the target instance
   * @param updateFunction function to compute new value
   * @return previous value
   */
  public long getAndUpdate(T obj, java.util.function.LongUnaryOperator updateFunction) {
    long prev = get(obj);
    for (;;) {
      long next = updateFunction.applyAsLong(prev);
      if (compareAndSet(obj, prev, next)) {
        return prev;
      }
      prev = get(obj);
    }
  }
}
