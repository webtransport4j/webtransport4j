package io.github.webtransport4j.internal;

import io.netty.channel.EventLoop;
import io.netty.util.internal.ThreadExecutorMap;

/** Internal boundary for operations requiring a control-plane thread. */
public final class EventLoopSafety {
  private EventLoopSafety() {}

  /** Detects the executor, rather than guessing from thread names or rejecting business workers. */
  public static boolean inEventLoop() {
    io.netty.util.concurrent.EventExecutor executor = ThreadExecutorMap.currentExecutor();
    return executor instanceof EventLoop
        || (executor == null
            && Thread.currentThread() instanceof io.netty.util.concurrent.FastThreadLocalThread);
  }

  /** Rejects before acquiring a monitor or performing a blocking control-plane operation. */
  public static void requireBlockingAllowed() {
    if (inEventLoop()) {
      throw new IllegalStateException(
          "Blocking control-plane operation must run off the Netty event loop");
    }
  }
}
