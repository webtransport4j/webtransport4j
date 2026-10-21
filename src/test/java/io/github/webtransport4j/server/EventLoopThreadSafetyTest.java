package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.internal.EventLoopSafety;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.DefaultEventExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies the event-loop boundary rejects blocking control-plane operations. */
public class EventLoopThreadSafetyTest {
  @Timeout(value = 10000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  @Test
  public void rejectsControlPlaneLocksOnEventLoopButAllowsWorkerThread() throws Exception {
    DefaultEventLoop loop = new DefaultEventLoop();
    DefaultEventExecutor worker = new DefaultEventExecutor();
    WebTransportServer server = WebTransportServer.builder().build();
    try {
      loop.submit(
              () -> {
                assertTrue(EventLoopSafety.inEventLoop());
                assertThrows(IllegalStateException.class, EventLoopSafety::requireBlockingAllowed);
                assertThrows(IllegalStateException.class, server::start);
                assertThrows(IllegalStateException.class, server::close);
                assertThrows(
                    IllegalStateException.class,
                    () -> WebTransportConfig.setProperty("locking.test", "must-not-publish"));
                assertThrows(
                    IllegalStateException.class,
                    () -> WebTransportConfig.removeProperty("locking.test"));
              })
          .get(3, TimeUnit.SECONDS);
      worker
          .submit(
              () -> {
                assertFalse(EventLoopSafety.inEventLoop());
                EventLoopSafety.requireBlockingAllowed();
              })
          .get(3, TimeUnit.SECONDS);
    } finally {
      server.close();
      loop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
      worker.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
    }
  }
}
