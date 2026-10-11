package io.github.webtransport4j.internal.handles;

/**
 * Handle abstraction for atomic reference field updates across Java versions.
 *
 * @param <T> the type of the object holding the field
 * @param <V> the type of the reference field
 */
public abstract class RefHandle<T, V> {

  /**
   * Reads the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @return current value
   */
  public abstract V get(T obj);

  /**
   * Sets the value of the target field with volatile memory semantics.
   *
   * @param obj the target instance
   * @param val new value
   */
  public abstract void set(T obj, V val);

  /**
   * Atomically sets the field to the update value if the current value equals the expected value.
   *
   * @param obj the target instance
   * @param expect expected value
   * @param update new value
   * @return true if successful
   */
  public abstract boolean compareAndSet(T obj, V expect, V update);

  /**
   * Atomically sets the field to val and returns the previous value.
   *
   * @param obj the target instance
   * @param val new value
   * @return previous value
   */
  public abstract V getAndSet(T obj, V val);
}
