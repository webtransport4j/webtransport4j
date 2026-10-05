package io.github.webtransport4j.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.internal.EventLoopSafety;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.DefaultEventExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Verifies the event-loop boundary rejects blocking control-plane operations. */
public class EventLoopThreadSafetyTest {
  @Test(timeout = 10000)
  public void rejectsControlPlaneLocksOnEventLoopButAllowsBusinessExecutor() throws Exception {
    DefaultEventLoop loop = new DefaultEventLoop();
    DefaultEventExecutor business = new DefaultEventExecutor();
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
      business
          .submit(
              () -> {
                assertFalse(EventLoopSafety.inEventLoop());
                EventLoopSafety.requireBlockingAllowed();
              })
          .get(3, TimeUnit.SECONDS);
    } finally {
      server.close();
      loop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
      business.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
    }
  }
}
