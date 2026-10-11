package io.github.webtransport4j.internal.handles;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

/**
 * Factory for creating handle abstractions.
 *
 * <p>Java 8 baseline implementation backed by {@link AtomicIntegerFieldUpdater},
 * {@link AtomicLongFieldUpdater}, and {@link AtomicReferenceFieldUpdater}.
 */
public final class Handles {

  private Handles() {}

  /**
   * Creates an {@link IntHandle} for the given field.
   *
   * @param <T> owner type
   * @param owner owner class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param updaterSupplier supplier providing the field updater
   * @return handle instance
   */
  public static <T> IntHandle<T> newIntHandle(
      Class<T> owner,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicIntegerFieldUpdater<T>> updaterSupplier) {
    return new UpdaterIntHandle<>(updaterSupplier.get());
  }

  /**
   * Creates a {@link LongHandle} for the given field.
   *
   * @param <T> owner type
   * @param owner owner class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param updaterSupplier supplier providing the field updater
   * @return handle instance
   */
  public static <T> LongHandle<T> newLongHandle(
      Class<T> owner,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicLongFieldUpdater<T>> updaterSupplier) {
    return new UpdaterLongHandle<>(updaterSupplier.get());
  }

  /**
   * Creates a {@link RefHandle} for the given field.
   *
   * @param <T> owner type
   * @param <V> field value type
   * @param owner owner class
   * @param fieldType field value type class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param updaterSupplier supplier providing the field updater
   * @return handle instance
   */
  public static <T, V> RefHandle<T, V> newRefHandle(
      Class<T> owner,
      Class<V> fieldType,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicReferenceFieldUpdater<T, V>> updaterSupplier) {
    return new UpdaterRefHandle<>(updaterSupplier.get());
  }

  private static final class UpdaterIntHandle<T> extends IntHandle<T> {
    private final AtomicIntegerFieldUpdater<T> updater;

    UpdaterIntHandle(AtomicIntegerFieldUpdater<T> updater) {
      this.updater = updater;
    }

    @Override
    public int get(T obj) {
      return updater.get(obj);
    }

    @Override
    public void set(T obj, int val) {
      updater.set(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, int expect, int update) {
      return updater.compareAndSet(obj, expect, update);
    }

    @Override
    public int incrementAndGet(T obj) {
      return updater.incrementAndGet(obj);
    }

    @Override
    public int decrementAndGet(T obj) {
      return updater.decrementAndGet(obj);
    }

    @Override
    public int addAndGet(T obj, int delta) {
      return updater.addAndGet(obj, delta);
    }

    @Override
    public int getAndIncrement(T obj) {
      return updater.getAndIncrement(obj);
    }

    @Override
    public int getAndDecrement(T obj) {
      return updater.getAndDecrement(obj);
    }

    @Override
    public int getAndAdd(T obj, int delta) {
      return updater.getAndAdd(obj, delta);
    }

    @Override
    public int getAndSet(T obj, int val) {
      return updater.getAndSet(obj, val);
    }
  }

  private static final class UpdaterLongHandle<T> extends LongHandle<T> {
    private final AtomicLongFieldUpdater<T> updater;

    UpdaterLongHandle(AtomicLongFieldUpdater<T> updater) {
      this.updater = updater;
    }

    @Override
    public long get(T obj) {
      return updater.get(obj);
    }

    @Override
    public void set(T obj, long val) {
      updater.set(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, long expect, long update) {
      return updater.compareAndSet(obj, expect, update);
    }

    @Override
    public long incrementAndGet(T obj) {
      return updater.incrementAndGet(obj);
    }

    @Override
    public long decrementAndGet(T obj) {
      return updater.decrementAndGet(obj);
    }

    @Override
    public long addAndGet(T obj, long delta) {
      return updater.addAndGet(obj, delta);
    }

    @Override
    public long getAndIncrement(T obj) {
      return updater.getAndIncrement(obj);
    }

    @Override
    public long getAndDecrement(T obj) {
      return updater.getAndDecrement(obj);
    }

    @Override
    public long getAndAdd(T obj, long delta) {
      return updater.getAndAdd(obj, delta);
    }

    @Override
    public long getAndSet(T obj, long val) {
      return updater.getAndSet(obj, val);
    }
  }

  private static final class UpdaterRefHandle<T, V> extends RefHandle<T, V> {
    private final AtomicReferenceFieldUpdater<T, V> updater;

    UpdaterRefHandle(AtomicReferenceFieldUpdater<T, V> updater) {
      this.updater = updater;
    }

    @Override
    public V get(T obj) {
      return updater.get(obj);
    }

    @Override
    public void set(T obj, V val) {
      updater.set(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, V expect, V update) {
      return updater.compareAndSet(obj, expect, update);
    }

    @Override
    public V getAndSet(T obj, V val) {
      return updater.getAndSet(obj, val);
    }
  }
}
