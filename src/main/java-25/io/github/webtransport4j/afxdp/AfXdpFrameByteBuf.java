package io.github.webtransport4j.afxdp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.WrappedByteBuf;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Zero-copy Direct ByteBuf wrapping a slice of an AF_XDP UMEM frame.
 *
 * <p>When the buffer's reference count drops to 0 (all handlers are finished with the packet),
 * it automatically recycles the frame address back to {@link AfXdpUmem}.
 */
public final class AfXdpFrameByteBuf extends WrappedByteBuf {

  private final AfXdpUmem umem;
  private final long frameAddress;
  private final AtomicBoolean recycled;

  public AfXdpFrameByteBuf(ByteBuf delegate, AfXdpUmem umem, long frameAddress) {
    this(delegate, umem, frameAddress, new AtomicBoolean(false));
  }

  private AfXdpFrameByteBuf(
      ByteBuf delegate, AfXdpUmem umem, long frameAddress, AtomicBoolean recycled) {
    super(Objects.requireNonNull(delegate, "delegate"));
    this.umem = Objects.requireNonNull(umem, "umem");
    this.frameAddress = frameAddress;
    this.recycled = Objects.requireNonNull(recycled, "recycled");
  }

  private void tryRecycle() {
    if (recycled.compareAndSet(false, true)) {
      umem.recycleFrame(frameAddress);
    }
  }

  @Override
  public boolean release() {
    boolean deallocated = super.release();
    if (deallocated) {
      tryRecycle();
    }
    return deallocated;
  }

  @Override
  public boolean release(int decrement) {
    boolean deallocated = super.release(decrement);
    if (deallocated) {
      tryRecycle();
    }
    return deallocated;
  }

  @Override
  public ByteBuf retainedDuplicate() {
    return new AfXdpFrameByteBuf(super.retainedDuplicate(), umem, frameAddress, recycled);
  }

  @Override
  public ByteBuf retainedSlice() {
    return new AfXdpFrameByteBuf(super.retainedSlice(), umem, frameAddress, recycled);
  }

  @Override
  public ByteBuf retainedSlice(int index, int length) {
    return new AfXdpFrameByteBuf(super.retainedSlice(index, length), umem, frameAddress, recycled);
  }

  @Override
  public ByteBuf touch() {
    super.touch();
    return this;
  }

  @Override
  public ByteBuf touch(Object hint) {
    super.touch(hint);
    return this;
  }

  public long getFrameAddress() {
    return frameAddress;
  }

  public boolean isRecycled() {
    return recycled.get();
  }
}
