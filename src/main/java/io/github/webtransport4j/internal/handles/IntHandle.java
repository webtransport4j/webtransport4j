package io.github.webtransport4j.internal.handles;

/**
 * Handle abstraction for atomic int field updates across Java versions.
 *
 * @param <T> the type of the object holding the field
 */
public abstract class IntHandle<T> {

  /**
   * Reads the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @return current value
   */
  public abstract int get(T obj);

  /**
   * Sets the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @param val new value
   */
  public abstract void set(T obj, int val);

  /**
   * Atomically sets the field to the update value if the current value equals the expected value.
   *
   * @param obj the target instance
   * @param expect expected value
   * @param update new value
   * @return true if successful
   */
  public abstract boolean compareAndSet(T obj, int expect, int update);

  /**
   * Atomically increments the field by 1 and returns the updated value.
   *
   * @param obj the target instance
   * @return updated value
   */
  public abstract int incrementAndGet(T obj);

  /**
   * Atomically decrements the field by 1 and returns the updated value.
   *
   * @param obj the target instance
   * @return updated value
   */
  public abstract int decrementAndGet(T obj);

  /**
   * Atomically adds delta to the field and returns the updated value.
   *
   * @param obj the target instance
   * @param delta amount to add
   * @return updated value
   */
  public abstract int addAndGet(T obj, int delta);

  /**
   * Atomically increments the field by 1 and returns the previous value.
   *
   * @param obj the target instance
   * @return previous value
   */
  public abstract int getAndIncrement(T obj);

  /**
   * Atomically decrements the field by 1 and returns the previous value.
   *
   * @param obj the target instance
   * @return previous value
   */
  public abstract int getAndDecrement(T obj);

  /**
   * Atomically adds delta to the field and returns the previous value.
   *
   * @param obj the target instance
   * @param delta amount to add
   * @return previous value
   */
  public abstract int getAndAdd(T obj, int delta);

  /**
   * Atomically sets the field to val and returns the previous value.
   *
   * @param obj the target instance
   * @param val new value
   * @return previous value
   */
  public abstract int getAndSet(T obj, int val);

  /**
   * Atomically updates the field using the given function and returns the updated value.
   *
   * @param obj the target instance
   * @param updateFunction function to compute new value
   * @return updated value
   */
  public int updateAndGet(T obj, java.util.function.IntUnaryOperator updateFunction) {
    int prev = get(obj);
    for (;;) {
      int next = updateFunction.applyAsInt(prev);
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
  public int getAndUpdate(T obj, java.util.function.IntUnaryOperator updateFunction) {
    int prev = get(obj);
    for (;;) {
      int next = updateFunction.applyAsInt(prev);
      if (compareAndSet(obj, prev, next)) {
        return prev;
      }
      prev = get(obj);
    }
  }
}
