package io.github.webtransport4j.internal.handles;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

/**
 * Factory for creating handle abstractions.
 *
 * <p>Java 11+ multi-release JAR implementation backed directly by {@link VarHandle}.
 */
public final class Handles {

  private Handles() {}

  /**
   * Creates an {@link IntHandle} for the given field using {@link VarHandle}.
   *
   * @param <T> owner type
   * @param owner owner class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param unused unused in Java 11+ (retained for identical API signature)
   * @return handle instance
   */
  public static <T> IntHandle<T> newIntHandle(
      Class<T> owner,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicIntegerFieldUpdater<T>> unused) {
    try {
      VarHandle vh = lookup.findVarHandle(owner, fieldName, int.class);
      return new VarHandleIntHandle<>(vh);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Creates a {@link LongHandle} for the given field using {@link VarHandle}.
   *
   * @param <T> owner type
   * @param owner owner class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param unused unused in Java 11+ (retained for identical API signature)
   * @return handle instance
   */
  public static <T> LongHandle<T> newLongHandle(
      Class<T> owner,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicLongFieldUpdater<T>> unused) {
    try {
      VarHandle vh = lookup.findVarHandle(owner, fieldName, long.class);
      return new VarHandleLongHandle<>(vh);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Creates a {@link RefHandle} for the given field using {@link VarHandle}.
   *
   * @param <T> owner type
   * @param <V> field value type
   * @param owner owner class
   * @param fieldType field value type class
   * @param fieldName target field name
   * @param lookup caller's method handles lookup
   * @param unused unused in Java 11+ (retained for identical API signature)
   * @return handle instance
   */
  public static <T, V> RefHandle<T, V> newRefHandle(
      Class<T> owner,
      Class<V> fieldType,
      String fieldName,
      MethodHandles.Lookup lookup,
      Supplier<AtomicReferenceFieldUpdater<T, V>> unused) {
    try {
      VarHandle vh = lookup.findVarHandle(owner, fieldName, fieldType);
      return new VarHandleRefHandle<>(vh);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final class VarHandleIntHandle<T> extends IntHandle<T> {
    private final VarHandle handle;

    VarHandleIntHandle(VarHandle handle) {
      this.handle = handle;
    }

    @Override
    public int get(T obj) {
      return (int) handle.getVolatile(obj);
    }

    @Override
    public void set(T obj, int val) {
      handle.setVolatile(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, int expect, int update) {
      return handle.compareAndSet(obj, expect, update);
    }

    @Override
    public int incrementAndGet(T obj) {
      return (int) handle.getAndAdd(obj, 1) + 1;
    }

    @Override
    public int decrementAndGet(T obj) {
      return (int) handle.getAndAdd(obj, -1) - 1;
    }

    @Override
    public int addAndGet(T obj, int delta) {
      return (int) handle.getAndAdd(obj, delta) + delta;
    }

    @Override
    public int getAndIncrement(T obj) {
      return (int) handle.getAndAdd(obj, 1);
    }

    @Override
    public int getAndDecrement(T obj) {
      return (int) handle.getAndAdd(obj, -1);
    }

    @Override
    public int getAndAdd(T obj, int delta) {
      return (int) handle.getAndAdd(obj, delta);
    }

    @Override
    public int getAndSet(T obj, int val) {
      return (int) handle.getAndSet(obj, val);
    }
  }

  private static final class VarHandleLongHandle<T> extends LongHandle<T> {
    private final VarHandle handle;

    VarHandleLongHandle(VarHandle handle) {
      this.handle = handle;
    }

    @Override
    public long get(T obj) {
      return (long) handle.getVolatile(obj);
    }

    @Override
    public void set(T obj, long val) {
      handle.setVolatile(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, long expect, long update) {
      return handle.compareAndSet(obj, expect, update);
    }

    @Override
    public long incrementAndGet(T obj) {
      return (long) handle.getAndAdd(obj, 1L) + 1L;
    }

    @Override
    public long decrementAndGet(T obj) {
      return (long) handle.getAndAdd(obj, -1L) - 1L;
    }

    @Override
    public long addAndGet(T obj, long delta) {
      return (long) handle.getAndAdd(obj, delta) + delta;
    }

    @Override
    public long getAndIncrement(T obj) {
      return (long) handle.getAndAdd(obj, 1L);
    }

    @Override
    public long getAndDecrement(T obj) {
      return (long) handle.getAndAdd(obj, -1L);
    }

    @Override
    public long getAndAdd(T obj, long delta) {
      return (long) handle.getAndAdd(obj, delta);
    }

    @Override
    public long getAndSet(T obj, long val) {
      return (long) handle.getAndSet(obj, val);
    }
  }

  private static final class VarHandleRefHandle<T, V> extends RefHandle<T, V> {
    private final VarHandle handle;

    VarHandleRefHandle(VarHandle handle) {
      this.handle = handle;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(T obj) {
      return (V) handle.getVolatile(obj);
    }

    @Override
    public void set(T obj, V val) {
      handle.setVolatile(obj, val);
    }

    @Override
    public boolean compareAndSet(T obj, V expect, V update) {
      return handle.compareAndSet(obj, expect, update);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getAndSet(T obj, V val) {
      return (V) handle.getAndSet(obj, val);
    }
  }
}
