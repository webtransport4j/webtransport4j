package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.server.WebTransportServer;
import io.netty.handler.codec.quic.QuicSslContext;
import java.lang.reflect.Method;
import org.mockito.Mockito;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/**
 * Actual delayed reload callback against close, with the callback boundary reached reflectively.
 */
@JCStressTest
@State
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "No active TLS context after close.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Reload republished into a closed server.")
public class TlsShutdownStress {
  final WebTransportServer server = WebTransportServer.builder().port(0).build();
  final QuicSslContext context = Mockito.mock(QuicSslContext.class);

  @Actor
  public void reload() {
    try {
      Method install =
          WebTransportServer.class.getDeclaredMethod(
              "installReloadedSslContext", QuicSslContext.class);
      install.setAccessible(true);
      install.invoke(server, context);
    } catch (Exception error) {
      throw new AssertionError(error);
    }
  }

  @Actor
  public void close() {
    server.close();
  }

  @Arbiter
  public void check(I_Result result) {
    result.r1 = server.getActiveSslContext() == null ? 0 : 1;
    server.close();
  }
}
